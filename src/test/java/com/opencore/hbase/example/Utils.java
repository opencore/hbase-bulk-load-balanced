package com.opencore.hbase.example;

import com.github.sakserv.minicluster.impl.MRLocalCluster;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/** Utilities used in the example. */
class Utils {

  // creates some files of random data into the provided directory, stored as sequence files.
  static void createRandomDataFiles(Configuration conf, Path directory) throws IOException {
    for (int file = 0; file < 10; file++) {
      String fileName = "in-" + file + ".seq";

      try (SequenceFile.Writer writer =
          SequenceFile.createWriter(
              conf,
              SequenceFile.Writer.file(new org.apache.hadoop.fs.Path(directory, fileName)),
              SequenceFile.Writer.keyClass(BytesWritable.class),
              SequenceFile.Writer.valueClass(Text.class)); ) {

        for (int record = 0; record < 25000; record++) {
          String key = RandomStringUtils.randomAlphabetic(12).toLowerCase();
          String value = RandomStringUtils.randomAlphabetic(512);
          writer.append(new BytesWritable(Bytes.toBytes(key)), new Text(value));
        }
      }
    }
  }

  // creates a local cluster that is started
  static MRLocalCluster createStartedCluster() throws Exception {
    MRLocalCluster mrLocalCluster =
        new MRLocalCluster.Builder()
            .setNumNodeManagers(1)
            .setJobHistoryAddress("localhost:37005")
            .setResourceManagerAddress("localhost:37001")
            .setResourceManagerHostname("localhost")
            .setResourceManagerSchedulerAddress("localhost:37002")
            .setResourceManagerResourceTrackerAddress("localhost:37003")
            .setResourceManagerWebappAddress("localhost:37004")
            .setUseInJvmContainerExecutor(false)
            .setConfig(new Configuration())
            .build();

    mrLocalCluster.start();
    return mrLocalCluster;
  }
}
