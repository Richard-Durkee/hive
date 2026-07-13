/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that discovers partition directories via BFS, yielding them in
 * batches without accumulating all paths in memory simultaneously.
 *
 * <p>This replaces the unbounded {@code Set<Path> allDirs} pattern in
 * {@link HiveMetaStoreChecker#checkPartitionDirs} with a bounded-memory
 * iterator that yields discovered partition paths in configurable batch sizes.
 *
 * <p>Memory usage is O(batchSize + BFS_frontier) instead of O(total_partitions).
 *
 * @see <a href="https://issues.apache.org/jira/browse/HIVE-12859">HIVE-12859</a>
 */
public class PartitionPathIterator implements Iterator<List<Path>>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionPathIterator.class);

  /** Sentinel value placed into discoveredPaths to signal BFS completion. */
  private static final Path POISON = new Path("__BFS_COMPLETE__");

  private final Path basePath;
  private final FileSystem fs;
  private final List<String> partColNames;
  private final int batchSize;
  private final Configuration conf;
  private final ExecutorService executor;
  private final boolean throwOnValidationError;

  // Buffer of discovered leaf partition paths not yet yielded.
  // Bounded to batchSize*2 to limit memory; BFS blocks on put when full.
  private final LinkedBlockingQueue<Path> discoveredPaths;
  // Any exception encountered during BFS
  private volatile MetastoreException bfsException = null;
  // BFS driver thread reference for interrupt on close
  private final Thread bfsThread;
  // Total partitions discovered (for progress reporting)
  private volatile long totalDiscovered = 0;
  // Whether we've seen the poison pill (BFS done, queue drained)
  private boolean finished = false;
  // Lookahead path from hasNext() consumed by next()
  private Path lookahead = null;

  /**
   * @param basePath       Table location path
   * @param fs             FileSystem for the table location
   * @param partColNames   Partition column names (determines BFS depth)
   * @param batchSize      Number of partition paths to yield per batch
   * @param conf           Hive/Metastore configuration
   */
  public PartitionPathIterator(Path basePath, FileSystem fs,
      List<String> partColNames, int batchSize, Configuration conf) {
    this.basePath = basePath;
    this.fs = fs;
    this.partColNames = partColNames;
    this.batchSize = batchSize;
    this.conf = conf;
    this.throwOnValidationError = "throw".equals(
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.MSCK_PATH_VALIDATION));
    this.discoveredPaths = new LinkedBlockingQueue<>(batchSize * 2);

    int poolSize = MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT);
    if (poolSize <= 1) {
      poolSize = 1;
    }
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("MSCK-GetPaths-%d")
        .build();
    // Bounded work queue + CallerRunsPolicy prevents unbounded task accumulation
    // when a BFS level has many directories (e.g., 1M leaf partitions).
    this.executor = new ThreadPoolExecutor(
        poolSize, poolSize,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(poolSize * 2),
        threadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());

    // Start BFS in background thread with initial seed path
    ConcurrentLinkedQueue<PathDepthInfo> initialPaths = new ConcurrentLinkedQueue<>();
    initialPaths.add(new PathDepthInfo(basePath, 0));
    this.bfsThread = new Thread(() -> runBfs(initialPaths), "MSCK-BFS-Driver");
    this.bfsThread.setDaemon(true);
    this.bfsThread.start();
  }

  /**
   * Background BFS traversal. Puts discovered leaf paths into
   * {@code discoveredPaths} queue with backpressure (blocks when queue full).
   * Sends a POISON sentinel when complete so consumers don't need to poll.
   */
  private void runBfs(ConcurrentLinkedQueue<PathDepthInfo> pendingPaths) {
    try {
      while (!pendingPaths.isEmpty()) {
        ConcurrentLinkedQueue<PathDepthInfo> nextLevel = new ConcurrentLinkedQueue<>();
        List<Future<Path>> futures = new ArrayList<>();

        while (!pendingPaths.isEmpty()) {
          PathDepthInfo pd = pendingPaths.poll();
          if (pd != null) {
            futures.add(executor.submit(() -> processPath(pd, nextLevel)));
          }
          // Drain in chunks to keep futures list bounded
          if (futures.size() >= batchSize) {
            drainFutures(futures);
          }
        }
        // Drain remaining futures from last partial chunk
        drainFutures(futures);
        pendingPaths = nextLevel;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      bfsException = new MetastoreException("BFS interrupted", e);
      discoveredPaths.clear(); // Bug 4: discard pre-failure buffered items
    } catch (ExecutionException e) {
      bfsException = new MetastoreException("BFS execution error", e.getCause());
      discoveredPaths.clear(); // Bug 4: discard pre-failure buffered items
    } finally {
      // Signal completion via poison pill -- guaranteed to be seen by consumer
      // even if bfsComplete flag has visibility delay
      try {
        discoveredPaths.put(POISON);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void drainFutures(List<Future<Path>> futures)
      throws InterruptedException, ExecutionException {
    for (Future<Path> future : futures) {
      Path p = future.get();
      if (p != null) {
        discoveredPaths.put(p);
        totalDiscovered++;
      }
    }
    futures.clear();
  }

  /**
   * Process a single path at a given depth. Returns the path if at leaf depth,
   * otherwise enqueues children for next BFS level.
   */
  private Path processPath(PathDepthInfo pd,
      ConcurrentLinkedQueue<PathDepthInfo> nextLevel)
      throws IOException, MetastoreException {

    final Path currentPath = pd.p;
    final int currentDepth = pd.depth;

    if (currentDepth == partColNames.size()) {
      return currentPath;
    }

    RemoteIterator<FileStatus> fileIterator = fs.listStatusIterator(currentPath);
    List<FileStatus> statuses = new ArrayList<>();
    while (fileIterator.hasNext()) {
      FileStatus status = fileIterator.next();
      if (HiveMetaStoreChecker.HIDDEN_FILES_PATH_FILTER.accept(status.getPath())) {
        statuses.add(status);
      }
    }

    if (statuses.isEmpty() && currentDepth > 0) {
      logOrThrow("MSCK is missing partition columns under " + currentPath);
    } else {
      for (FileStatus fileStatus : statuses) {
        if (!fileStatus.isDirectory()) {
          logOrThrow(
              "MSCK finds a file rather than a directory: " + fileStatus.getPath());
        } else {
          Path nextPath = fileStatus.getPath();
          String[] parts = nextPath.getName().split("=");
          if (parts.length != 2) {
            logOrThrow("Invalid partition name " + nextPath);
          } else if (!parts[0].equalsIgnoreCase(partColNames.get(currentDepth))) {
            logOrThrow(
                "Unexpected partition key " + parts[0] + " found at " + nextPath);
          } else {
            nextLevel.add(new PathDepthInfo(nextPath, currentDepth + 1));
          }
        }
      }
    }
    return null;
  }

  private void logOrThrow(String msg) throws MetastoreException {
    if (throwOnValidationError) {
      throw new MetastoreException(msg);
    } else {
      LOG.warn(msg);
    }
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    // Bug 3: fail-fast if BFS encountered an error
    if (bfsException != null) {
      throw new RuntimeException(
          "BFS traversal failed: " + bfsException.getMessage(), bfsException);
    }
    if (lookahead != null) {
      return true;
    }
    // Block until a path is available or BFS signals completion.
    try {
      Path p = discoveredPaths.poll(5, TimeUnit.SECONDS);
      if (p == null) {
        // Timeout — check if BFS had an error, otherwise keep waiting
        if (bfsException != null) {
          throw new RuntimeException(
              "BFS traversal failed: " + bfsException.getMessage(), bfsException);
        }
        return hasNext();
      }
      if (p == POISON) {
        finished = true;
        // Bug 3: exception may have been set just before POISON was put
        if (bfsException != null) {
          throw new RuntimeException(
              "BFS traversal failed: " + bfsException.getMessage(), bfsException);
        }
        return false;
      }
      lookahead = p;
      return true;
    } catch (InterruptedException e) {
      // Bug 2: propagate interrupt as exception, don't silently end iteration
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during partition discovery", e);
    }
  }

  /**
   * Returns the next batch of discovered partition paths.
   * Batch size is at most {@code batchSize}, may be smaller if fewer
   * paths are currently available.
   */
  @Override
  public List<Path> next() {
    if (!hasNext()) {
      if (bfsException != null) {
        throw new RuntimeException(
            "BFS traversal failed: " + bfsException.getMessage(), bfsException);
      }
      throw new NoSuchElementException();
    }

    List<Path> batch = new ArrayList<>(batchSize);
    // Consume lookahead from hasNext()
    if (lookahead != null) {
      batch.add(lookahead);
      lookahead = null;
    }
    // Drain up to batchSize - 1 more without blocking
    while (batch.size() < batchSize) {
      Path p = discoveredPaths.poll();
      if (p == null) {
        break;
      }
      if (p == POISON) {
        finished = true;
        break;
      }
      batch.add(p);
    }
    return batch;
  }

  /** Returns any exception that occurred during BFS traversal. */
  public MetastoreException getException() {
    return bfsException;
  }

  /** Total number of partition paths discovered so far. */
  public long getDiscoveredCount() {
    return totalDiscovered;
  }

  @Override
  public void close() {
    // Bug 5: clear the queue BEFORE interrupt so that if BFS thread is
    // blocked in put() (queue full), it sees room and unblocks. The
    // interrupt then catches it on the next blocking call.
    discoveredPaths.clear();
    bfsThread.interrupt();
    executor.shutdownNow();
  }

  private static class PathDepthInfo {
    final Path p;
    final int depth;

    PathDepthInfo(Path p, int depth) {
      this.p = p;
      this.depth = depth;
    }
  }
}
