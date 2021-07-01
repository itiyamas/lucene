/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.mockfile.LeakFS;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.StringHelper;

/** Tests NIOFSDirectory */
public class TestNIOFSDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return new NIOFSDirectory(path);
  }

  public void testHandleExceptionInConstructor() throws Exception {
    Path path = createTempDir().toRealPath();
    final LeakFS leakFS =
        new LeakFS(path.getFileSystem()) {
          @Override
          public FileChannel newFileChannel(
              Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
              throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
              @Override
              public long size() throws IOException {
                throw new IOException("simulated");
              }
            };
          }
        };
    FileSystem fs = leakFS.getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(path, fs);
    try (Directory dir = new NIOFSDirectory(wrapped)) {
      try (IndexOutput out = dir.createOutput("test.bin", IOContext.DEFAULT)) {
        out.writeString("hello");
      }
      final IOException error =
          expectThrows(IOException.class, () -> dir.openInput("test.bin", IOContext.DEFAULT));
      assertEquals("simulated", error.getMessage());
    }
  }

  public void testNIOFSWriteFsyncRead() throws Exception {


    Path path = Paths.get(System.getProperty("tests.niofstest.dirname"));
    PrintWriter logger =
            new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(path.toString() + "/logs")), Charset.defaultCharset()), true);
    Set<String> files = new ConcurrentSkipListSet<>();
    AtomicLong counter = new AtomicLong();

    int threads = Integer.valueOf(System.getProperty("tests.niofs.parallelThreads"));

    ExecutorService pool = Executors.newFixedThreadPool(threads);

    CountDownLatch latch = new CountDownLatch(2);
    new Thread(()-> writeFiles(counter, files, pool, path, latch, logger, threads)).start();
    new Thread(()-> testFiles(files, pool, path, latch, logger)).start();
    latch.await();

    boolean cleanUp = Boolean.valueOf(System.getProperty("tests.niofs.cleanUp"));
    if(cleanUp) {
      cleanUp(path, files);
    }

    logger.close();
    pool.shutdownNow();

  }

  private void cleanUp(Path path, Set<String> files) throws IOException {
    try (Directory dir = new NIOFSDirectory(path)) {
      files.stream().forEach(x->{
        try {
          dir.deleteFile(x);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
  }

  private void writeFiles(AtomicLong counter, Set<String> files, ExecutorService pool, Path path, CountDownLatch latch, PrintWriter logger, int threadSize) {

    try (Directory dir = new NIOFSDirectory(path)) {

      Random r = new Random();
      long currentTime = System.currentTimeMillis();
      long newTime = System.currentTimeMillis();
      long time = Long.valueOf(System.getProperty("tests.niofstest.time.millis"));
      while(newTime < currentTime + time) {
        String fileName = "test.bin" + counter.incrementAndGet();
        pool.execute(()->createAndWriteFile(dir, fileName, files, logger));
        logger.println("Successfully created " + fileName);

        if(RandomNumbers.randomIntBetween(r, 1, threadSize/2) == 3) {
          try {
            Thread.sleep(1000);
            newTime = System.currentTimeMillis();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

    } catch (Exception e) {
      logger.println(e.getStackTrace());
      e.printStackTrace();
    } finally {
      latch.countDown();
    }

  }

  private void testFiles(Set<String> files, ExecutorService pool, Path path, CountDownLatch latch, PrintWriter logger) {
    try (Directory dir = new MMapDirectory(path)) {
      long currentTime = System.currentTimeMillis();
      long newTime = System.currentTimeMillis();
      long time = Long.valueOf(System.getProperty("tests.niofstest.time.millis"));
      while(newTime < currentTime + time) {
        for(String file: files) {
          pool.execute(()->testFile(dir, file, files, logger));
          logger.println("Successfully tested " + file);
        }
        try {
          Thread.sleep(1000);
          newTime = System.currentTimeMillis();
        } catch (InterruptedException e) {
          logger.println(e.getStackTrace());

        }
      }
    } catch (Exception e) {
      logger.println(e.getStackTrace());
    } finally {
      latch.countDown();
    }
  }

  private void createAndWriteFile(Directory dir, String fileName, Set<String> files, PrintWriter logger) {
    try (IndexOutput out = dir.createOutput( fileName, IOContext.DEFAULT)) {
      CodecUtil.writeIndexHeader(out, "Lucene90CompoundData", 0, StringHelper.randomId(), "");
      out.writeString(StringHelper.randomId().toString());
      CodecUtil.writeFooter(out);
    } catch(Exception ex) {
      logger.println(ex.getStackTrace());
      throw new RuntimeException(ex);
    }

    try {
      dir.sync(Collections.singletonList(fileName));
      files.add(fileName);
    } catch (IOException e) {
      logger.println(e.getStackTrace());
      throw new RuntimeException(e);
    }
  }

  private void testFile(Directory dir, String fileName, Set<String> files, PrintWriter logger) {
      try (ChecksumIndexInput in = dir.openChecksumInput( fileName, IOContext.DEFAULT)) {
        CodecUtil.retrieveChecksum(in);
      } catch(IOException ex) {

        logger.println("Exception in retrieving checksum" + ex.getStackTrace());
        throw new RuntimeException(ex);
     }

    boolean cleanUp = Boolean.valueOf(System.getProperty("tests.niofs.cleanUp"));

    try {
      if(cleanUp) {
        dir.deleteFile(fileName);
      }
      files.remove(fileName);
    } catch (IOException e) {
      logger.println(e.getStackTrace());
      throw new RuntimeException(e);
    }
  }
}
