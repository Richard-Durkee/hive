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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the callback-based incremental partition directory discovery
 * (HIVE-12859 fix).
 */
@Category(MetastoreUnitTest.class)
public class TestCheckPartitionDirsIncremental {

  /**
   * Test basic 2-level partition tree (country/city).
   * Verifies callback is invoked for each leaf partition path.
   */
  @Test
  public void testBasicTwoLevelTree() throws IOException, MetastoreException, MetaException {
    LocalFileSystem mockFs = Mockito.mock(LocalFileSystem.class);
    Path tableLocation = new Path("mock:///tmp/testTable");

    // Level 1: country=US, country=IND
    Path countryUS = new Path(tableLocation, "country=US");
    Path countryIND = new Path(tableLocation, "country=IND");

    // Level 2: cities
    Path cityPA = new Path(countryUS, "city=PA");
    Path citySF = new Path(countryUS, "city=SF");
    Path cityBOM = new Path(countryIND, "city=BOM");
    Path cityDEL = new Path(countryIND, "city=DEL");

    // Data files at leaf level
    Path paData = new Path(cityPA, "datafile");
    Path sfData = new Path(citySF, "datafile");
    Path bomData = new Path(cityBOM, "datafile");
    Path delData = new Path(cityDEL, "datafile");

    // Level 1 listing
    FileStatus[] allCountries = getMockFileStatus(countryUS, countryIND);
    mockListStatusIterator(mockFs, tableLocation, allCountries);

    // Level 2 listing
    FileStatus[] filesInUS = getMockFileStatus(cityPA, citySF);
    mockListStatusIterator(mockFs, countryUS, filesInUS);

    FileStatus[] filesInInd = getMockFileStatus(cityBOM, cityDEL);
    mockListStatusIterator(mockFs, countryIND, filesInInd);

    // Level 3 listing (data files - not directories)
    FileStatus[] paFiles = getMockFileStatus(paData);
    mockListStatusIterator(mockFs, cityPA, paFiles);

    FileStatus[] sfFiles = getMockFileStatus(sfData);
    mockListStatusIterator(mockFs, citySF, sfFiles);

    FileStatus[] bomFiles = getMockFileStatus(bomData);
    mockListStatusIterator(mockFs, cityBOM, bomFiles);

    FileStatus[] delFiles = getMockFileStatus(delData);
    mockListStatusIterator(mockFs, cityDEL, delFiles);

    HiveMetaStoreChecker checker = new HiveMetaStoreChecker(
        Mockito.mock(IMetaStoreClient.class), MetastoreConf.newMetastoreConf());
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    List<Path> discoveredPaths = new ArrayList<>();
    AtomicInteger callbackCount = new AtomicInteger(0);

    try {
      checker.checkPartitionDirsIncremental(
          executorService, tableLocation, mockFs,
          Arrays.asList("country", "city"), 100,
          path -> {
            callbackCount.incrementAndGet();
            discoveredPaths.add(path);
          });
    } finally {
      executorService.shutdown();
    }

    // Verify callback was invoked 4 times (once per leaf partition)
    Assert.assertEquals("Callback should be invoked for each partition", 4, callbackCount.get());
    Assert.assertEquals("Should discover 4 partitions", 4, discoveredPaths.size());

    // Verify all leaf paths were discovered
    Assert.assertTrue("Should find city=PA", discoveredPaths.contains(cityPA));
    Assert.assertTrue("Should find city=SF", discoveredPaths.contains(citySF));
    Assert.assertTrue("Should find city=BOM", discoveredPaths.contains(cityBOM));
    Assert.assertTrue("Should find city=DEL", discoveredPaths.contains(cityDEL));

    // Verify listStatus was called correct number of times (1 table + 2 countries = 3)
    verify(mockFs, times(3)).listStatusIterator(any(Path.class));
  }

  /**
   * Test empty table (no partitions on filesystem).
   * Verifies callback is never invoked.
   */
  @Test
  public void testEmptyTable() throws IOException, MetastoreException, MetaException {
    LocalFileSystem mockFs = Mockito.mock(LocalFileSystem.class);
    Path tableLocation = new Path("mock:///tmp/emptyTable");

    // Empty directory listing
    FileStatus[] empty = new FileStatus[0];
    mockListStatusIterator(mockFs, tableLocation, empty);

    HiveMetaStoreChecker checker = new HiveMetaStoreChecker(
        Mockito.mock(IMetaStoreClient.class), MetastoreConf.newMetastoreConf());
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    AtomicInteger callbackCount = new AtomicInteger(0);

    try {
      checker.checkPartitionDirsIncremental(
          executorService, tableLocation, mockFs,
          Arrays.asList("year", "month"), 100,
          path -> callbackCount.incrementAndGet());
    } finally {
      executorService.shutdown();
    }

    Assert.assertEquals("Callback should not be invoked for empty table", 0, callbackCount.get());
    verify(mockFs, times(1)).listStatusIterator(any(Path.class));
  }

  /**
   * Test IOException propagation from callback.
   * Verifies exceptions thrown by callback are properly propagated.
   */
  @Test
  public void testIOExceptionPropagation() throws MetastoreException, MetaException {
    LocalFileSystem mockFs = Mockito.mock(LocalFileSystem.class);
    Path tableLocation = new Path("mock:///tmp/testTable");

    // Single partition
    Path partition = new Path(tableLocation, "year=2024");

    try {
      FileStatus[] partitions = getMockFileStatus(partition);
      mockListStatusIterator(mockFs, tableLocation, partitions);

      // Empty listing at leaf level
      mockListStatusIterator(mockFs, partition, new FileStatus[0]);

      HiveMetaStoreChecker checker = new HiveMetaStoreChecker(
          Mockito.mock(IMetaStoreClient.class), MetastoreConf.newMetastoreConf());
      ExecutorService executorService = Executors.newFixedThreadPool(2);

      try {
        checker.checkPartitionDirsIncremental(
            executorService, tableLocation, mockFs,
            Arrays.asList("year"), 100,
            path -> {
              throw new IOException("Simulated IO failure");
            });
        Assert.fail("Expected IOException to be propagated");
      } catch (IOException e) {
        Assert.assertEquals("Simulated IO failure", e.getMessage());
      } finally {
        executorService.shutdown();
      }
    } catch (IOException e) {
      Assert.fail("Unexpected IOException during setup: " + e.getMessage());
    }
  }

  /**
   * Test MetastoreException propagation from callback.
   */
  @Test
  public void testMetastoreExceptionPropagation() throws IOException, MetaException {
    LocalFileSystem mockFs = Mockito.mock(LocalFileSystem.class);
    Path tableLocation = new Path("mock:///tmp/testTable");

    // Single partition
    Path partition = new Path(tableLocation, "year=2024");
    FileStatus[] partitions = getMockFileStatus(partition);
    mockListStatusIterator(mockFs, tableLocation, partitions);

    // Empty listing at leaf level
    mockListStatusIterator(mockFs, partition, new FileStatus[0]);

    HiveMetaStoreChecker checker = new HiveMetaStoreChecker(
        Mockito.mock(IMetaStoreClient.class), MetastoreConf.newMetastoreConf());
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    try {
      checker.checkPartitionDirsIncremental(
          executorService, tableLocation, mockFs,
          Arrays.asList("year"), 100,
          path -> {
            throw new MetastoreException("Simulated metastore failure");
          });
      Assert.fail("Expected MetastoreException to be propagated");
    } catch (MetastoreException e) {
      Assert.assertEquals("Simulated metastore failure", e.getMessage());
    } finally {
      executorService.shutdown();
    }
  }

  /**
   * Test batching behavior with small batch size.
   * Verifies futures are drained at batch boundaries.
   */
  @Test
  public void testBatchingBehavior() throws IOException, MetastoreException, MetaException {
    LocalFileSystem mockFs = Mockito.mock(LocalFileSystem.class);
    Path tableLocation = new Path("mock:///tmp/testTable");

    // Create 10 partitions at level 1
    List<Path> partitionPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partitionPaths.add(new Path(tableLocation, "part=" + i));
    }

    FileStatus[] partitions = getMockFileStatus(partitionPaths.toArray(new Path[0]));
    mockListStatusIterator(mockFs, tableLocation, partitions);

    // Empty listings at leaf level for each partition
    for (Path p : partitionPaths) {
      mockListStatusIterator(mockFs, p, new FileStatus[0]);
    }

    HiveMetaStoreChecker checker = new HiveMetaStoreChecker(
        Mockito.mock(IMetaStoreClient.class), MetastoreConf.newMetastoreConf());
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    List<Path> discoveredPaths = new ArrayList<>();

    try {
      // Use batch size of 3 to force multiple drain cycles
      checker.checkPartitionDirsIncremental(
          executorService, tableLocation, mockFs,
          Arrays.asList("part"), 3,
          path -> discoveredPaths.add(path));
    } finally {
      executorService.shutdown();
    }

    // All 10 partitions should be discovered
    Assert.assertEquals("Should discover all 10 partitions", 10, discoveredPaths.size());
  }

  // Helper methods (same pattern as TestMsckCheckPartitions)

  private void mockListStatusIterator(LocalFileSystem mockFs, Path location,
      FileStatus[] fileStatuses) throws IOException {
    when(mockFs.listStatusIterator(location)).thenReturn(
        new RemoteIterator<FileStatus>() {
          private int i = 0;

          @Override
          public boolean hasNext() throws IOException {
            return this.i < fileStatuses.length;
          }

          @Override
          public FileStatus next() throws IOException {
            return fileStatuses[this.i++];
          }
        });
  }

  private FileStatus[] getMockFileStatus(Path... paths) throws IOException {
    FileStatus[] result = new FileStatus[paths.length];
    int i = 0;
    for (Path p : paths) {
      result[i++] = createMockFileStatus(p);
    }
    return result;
  }

  private FileStatus createMockFileStatus(Path p) {
    FileStatus mock = Mockito.mock(FileStatus.class);
    when(mock.getPath()).thenReturn(p);
    // Treat as file if name contains "datafile", otherwise directory
    if (p.toString().contains("datafile")) {
      when(mock.isDirectory()).thenReturn(false);
    } else {
      when(mock.isDirectory()).thenReturn(true);
    }
    return mock;
  }
}
