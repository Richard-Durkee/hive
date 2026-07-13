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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.experimental.categories.Category;

/**
 * Concurrency correctness tests for {@link PartitionPathIterator}.
 * Tests target specific race conditions identified in analysis.
 */
@Category(MetastoreUnitTest.class)
public class TestPartitionPathIterator {

  @Rule
  public Timeout globalTimeout = Timeout.seconds(15);

  private Configuration conf;
  private Path tablePath;
  private static final List<String> ONE_PART_COL = Collections.singletonList("dt");
  private static final List<String> TWO_PART_COLS = Arrays.asList("dt", "city");

  @Before
  public void setUp() {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT, 1);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.MSCK_PATH_VALIDATION, "warn");
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.MSCK_REPAIR_BATCH_SIZE, 100);
    tablePath = new Path("hdfs://ns1/warehouse/db/tbl");
  }

  // =========================================================================
  // Helper: build a mock FileSystem from a path -> children map.
  // =========================================================================

  private FileSystem mockFs(Map<Path, List<Path>> tree) throws IOException {
    FileSystem fs = mock(FileSystem.class);
    when(fs.listStatusIterator(any(Path.class))).thenAnswer(invocation -> {
      Path p = invocation.getArgument(0);
      List<Path> children = tree.getOrDefault(p, Collections.emptyList());
      List<FileStatus> statuses = new ArrayList<>();
      for (Path child : children) {
        FileStatus st = mock(FileStatus.class);
        when(st.getPath()).thenReturn(child);
        when(st.isDirectory()).thenReturn(true);
        statuses.add(st);
      }
      return remoteIteratorOf(statuses);
    });
    return fs;
  }

  private static <T> RemoteIterator<T> remoteIteratorOf(List<T> items) {
    return new RemoteIterator<T>() {
      int idx = 0;
      @Override public boolean hasNext() { return idx < items.size(); }
      @Override public T next() { return items.get(idx++); }
    };
  }

  // =========================================================================
  // Normal-path tests
  // =========================================================================

  @Test
  public void testNormalIterationCollectsAllPartitions() throws IOException {
    Path p1 = new Path(tablePath, "dt=2024-01-01");
    Path p2 = new Path(tablePath, "dt=2024-01-02");
    Path p3 = new Path(tablePath, "dt=2024-01-03");

    Map<Path, List<Path>> tree = new HashMap<>();
    tree.put(tablePath, Arrays.asList(p1, p2, p3));

    FileSystem fs = mockFs(tree);
    Set<Path> discovered = new HashSet<>();
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, ONE_PART_COL, 10, conf)) {
      while (iter.hasNext()) {
        discovered.addAll(iter.next());
      }
      assertNull("No BFS exception expected", iter.getException());
    }
    assertEquals(Set.of(p1, p2, p3), discovered);
  }

  @Test
  public void testTwoLevelPartitionTree() throws IOException {
    Path dt1 = new Path(tablePath, "dt=2024-01-01");
    Path dt2 = new Path(tablePath, "dt=2024-01-02");
    Path leaf1 = new Path(dt1, "city=london");
    Path leaf2 = new Path(dt1, "city=paris");
    Path leaf3 = new Path(dt2, "city=berlin");

    Map<Path, List<Path>> tree = new HashMap<>();
    tree.put(tablePath, Arrays.asList(dt1, dt2));
    tree.put(dt1, Arrays.asList(leaf1, leaf2));
    tree.put(dt2, Collections.singletonList(leaf3));

    FileSystem fs = mockFs(tree);
    Set<Path> discovered = new HashSet<>();
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, TWO_PART_COLS, 10, conf)) {
      while (iter.hasNext()) {
        discovered.addAll(iter.next());
      }
      assertNull(iter.getException());
    }
    assertEquals(Set.of(leaf1, leaf2, leaf3), discovered);
  }

  @Test
  public void testEmptyTable() throws IOException {
    Map<Path, List<Path>> tree = new HashMap<>();
    tree.put(tablePath, Collections.emptyList());

    FileSystem fs = mockFs(tree);
    List<Path> all = new ArrayList<>();
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, ONE_PART_COL, 10, conf)) {
      while (iter.hasNext()) {
        all.addAll(iter.next());
      }
      assertNull(iter.getException());
    }
    assertTrue("Expected no paths for empty table", all.isEmpty());
  }

  // =========================================================================
  // Bug 1: next() never returns empty batch when hasNext() was true
  // =========================================================================

  @Test
  public void testNextNeverReturnsEmptyBatch() throws IOException {
    int partitionCount = 50;
    int batchSize = 5;

    List<Path> children = new ArrayList<>();
    Map<Path, List<Path>> tree = new HashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      children.add(new Path(tablePath, "dt=2024-01-" + String.format("%02d", i + 1)));
    }
    tree.put(tablePath, children);

    FileSystem fs = mockFs(tree);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.MSCK_REPAIR_BATCH_SIZE, batchSize);

    int emptyBatches = 0;
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, ONE_PART_COL, batchSize, conf)) {
      while (iter.hasNext()) {
        List<Path> batch = iter.next();
        if (batch.isEmpty()) {
          emptyBatches++;
        }
      }
      assertNull(iter.getException());
    }
    assertEquals("next() should never return empty batch after hasNext()=true", 0, emptyBatches);
  }

  // =========================================================================
  // Bug 2: hasNext() does not swallow interrupt as false
  // =========================================================================

  @Test
  public void testHasNextPropagatesInterrupt() throws Exception {
    int partitionCount = 200;
    List<Path> children = new ArrayList<>();
    Map<Path, List<Path>> tree = new HashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      children.add(new Path(tablePath, "dt=2024-" + String.format("%04d", i)));
    }
    tree.put(tablePath, children);

    // Slow FS so consumer ends up blocking in hasNext
    FileSystem fs = mock(FileSystem.class);
    CountDownLatch bfsStarted = new CountDownLatch(1);
    when(fs.listStatusIterator(any(Path.class))).thenAnswer(inv -> {
      bfsStarted.countDown();
      Thread.sleep(200);
      Path p = inv.getArgument(0);
      List<Path> ch = tree.getOrDefault(p, Collections.emptyList());
      List<FileStatus> statuses = new ArrayList<>();
      for (Path child : ch) {
        FileStatus st = mock(FileStatus.class);
        when(st.getPath()).thenReturn(child);
        when(st.isDirectory()).thenReturn(true);
        statuses.add(st);
      }
      return remoteIteratorOf(statuses);
    });

    AtomicBoolean exceptionThrown = new AtomicBoolean(false);
    CountDownLatch done = new CountDownLatch(1);

    Thread consumer = new Thread(() -> {
      try (PartitionPathIterator iter = new PartitionPathIterator(
          tablePath, fs, ONE_PART_COL, 10, conf)) {
        while (iter.hasNext()) {
          iter.next();
        }
      } catch (RuntimeException e) {
        if (e.getMessage() != null && e.getMessage().contains("Interrupted")) {
          exceptionThrown.set(true);
        }
      } finally {
        done.countDown();
      }
    });
    consumer.start();

    bfsStarted.await(5, TimeUnit.SECONDS);
    Thread.sleep(50);
    consumer.interrupt();

    done.await(10, TimeUnit.SECONDS);
    assertTrue("Expected interrupt to propagate as RuntimeException, not return false",
        exceptionThrown.get());
  }

  // =========================================================================
  // Bug 3: BFS exception surfaces immediately (fail-fast)
  // =========================================================================

  @Test
  public void testBfsExceptionSurfacesImmediately() throws IOException {
    Path dt1 = new Path(tablePath, "dt=2024-01-01");
    Path dt2 = new Path(tablePath, "dt=2024-01-02");
    Path leaf1 = new Path(dt1, "city=london");

    Map<Path, List<Path>> tree = new HashMap<>();
    tree.put(tablePath, Arrays.asList(dt1, dt2));
    tree.put(dt1, Collections.singletonList(leaf1));

    FileSystem fs = mockFs(tree);
    // dt2 listing throws
    when(fs.listStatusIterator(dt2)).thenThrow(new IOException("Simulated failure"));

    boolean exceptionDuringIteration = false;
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, TWO_PART_COLS, 10, conf)) {
      try {
        while (iter.hasNext()) {
          iter.next();
        }
      } catch (RuntimeException e) {
        exceptionDuringIteration = true;
      }
    }
    assertTrue("BFS exception should surface during iteration, not be deferred",
        exceptionDuringIteration);
  }

  // =========================================================================
  // Bug 4: Pre-failure paths not silently consumed
  // =========================================================================

  @Test
  public void testPreFailurePathsNotSilentlyConsumed() throws IOException {
    // 5 dt= partitions; listing for 3rd throws
    List<Path> dtChildren = new ArrayList<>();
    Map<Path, List<Path>> tree = new HashMap<>();
    for (int i = 1; i <= 5; i++) {
      dtChildren.add(new Path(tablePath, "dt=2024-01-0" + i));
    }
    tree.put(tablePath, dtChildren);
    for (int i = 1; i <= 5; i++) {
      Path dt = new Path(tablePath, "dt=2024-01-0" + i);
      if (i != 3) {
        tree.put(dt, Collections.singletonList(new Path(dt, "city=london")));
      }
    }

    FileSystem fs = mockFs(tree);
    Path failingPath = new Path(tablePath, "dt=2024-01-03");
    when(fs.listStatusIterator(failingPath))
        .thenThrow(new IOException("Injected failure"));

    List<Path> consumed = new ArrayList<>();
    boolean exceptionDuringIteration = false;
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, TWO_PART_COLS, 10, conf)) {
      try {
        while (iter.hasNext()) {
          consumed.addAll(iter.next());
        }
      } catch (RuntimeException e) {
        exceptionDuringIteration = true;
      }
      // If exception thrown during iteration: correct (fail-fast)
      // If no exception but getException() non-null: Bug 4 (deferred error)
      if (!exceptionDuringIteration && iter.getException() != null) {
        fail("Bug 4: " + consumed.size() + " paths consumed before error surfaced. " +
            "Exception: " + iter.getException().getMessage());
      }
    }
  }

  // =========================================================================
  // Bug 5: close() doesn't deadlock when queue is full
  // =========================================================================

  @Test(timeout = 10000)
  public void testCloseDoesNotDeadlock() throws Exception {
    int batchSize = 2; // queue capacity = 4
    int partitionCount = 100;

    List<Path> children = new ArrayList<>();
    Map<Path, List<Path>> tree = new HashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      children.add(new Path(tablePath, "dt=part-" + i));
    }
    tree.put(tablePath, children);

    FileSystem fs = mockFs(tree);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.MSCK_REPAIR_BATCH_SIZE, batchSize);

    PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, ONE_PART_COL, batchSize, conf);

    // Consume just one batch so queue fills up
    assertTrue(iter.hasNext());
    List<Path> firstBatch = iter.next();
    assertFalse("First batch should not be empty", firstBatch.isEmpty());

    // Let BFS fill the queue
    Thread.sleep(200);

    // close() must return quickly -- deadlock caught by test timeout
    long start = System.currentTimeMillis();
    iter.close();
    long elapsed = System.currentTimeMillis() - start;
    assertTrue("close() took " + elapsed + "ms -- possible deadlock", elapsed < 5000);
  }

  // =========================================================================
  // Additional: batch size respected, discoveredCount monotonic
  // =========================================================================

  @Test
  public void testBatchSizeRespected() throws IOException {
    int partitionCount = 20;
    int batchSize = 5;

    List<Path> children = new ArrayList<>();
    Map<Path, List<Path>> tree = new HashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      children.add(new Path(tablePath, "dt=p" + i));
    }
    tree.put(tablePath, children);

    FileSystem fs = mockFs(tree);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.MSCK_REPAIR_BATCH_SIZE, batchSize);

    int total = 0;
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, ONE_PART_COL, batchSize, conf)) {
      while (iter.hasNext()) {
        List<Path> batch = iter.next();
        assertTrue("Batch exceeds batchSize", batch.size() <= batchSize);
        total += batch.size();
      }
      assertNull(iter.getException());
    }
    assertEquals(partitionCount, total);
  }

  @Test
  public void testDiscoveredCountMonotonicallyIncreases() throws IOException {
    List<Path> children = new ArrayList<>();
    Map<Path, List<Path>> tree = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      children.add(new Path(tablePath, "dt=2024-" + i));
    }
    tree.put(tablePath, children);

    FileSystem fs = mockFs(tree);
    long prev = 0;
    try (PartitionPathIterator iter = new PartitionPathIterator(
        tablePath, fs, ONE_PART_COL, 5, conf)) {
      while (iter.hasNext()) {
        iter.next();
        long curr = iter.getDiscoveredCount();
        assertTrue("getDiscoveredCount() went backwards: " + prev + " -> " + curr,
            curr >= prev);
        prev = curr;
      }
    }
    assertEquals(20, prev);
  }
}
