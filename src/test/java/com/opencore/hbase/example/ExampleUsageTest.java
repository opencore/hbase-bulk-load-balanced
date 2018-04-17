package com.opencore.hbase.example;

import com.github.sakserv.minicluster.impl.MRLocalCluster;
import com.opencore.hbase.KeyBoundaries;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static com.opencore.hbase.example.Utils.createRandomDataFiles;
import static com.opencore.hbase.example.Utils.createStartedCluster;

/**
 * Demonstrates example usage of the process.
 *
 * <ol>
 *   <li>Random data files are created: sequence file with a {@link BytesWritable} key
 *   <li>A mini cluster is spun up
 *   <li>The files are rewritten into sorted key order, stored as new sequence files. A {@link
 *       TotalOrderPartitioner} achieves this by:
 *       <ol>
 *         <li>Sampling of the input files to determine some initial partitions
 *         <li>Shuffling the input into those partitions and sorting the records
 *       </ol>
 *   <li>The files are inspected to determine key ranges
 * </ol>
 */
public class ExampleUsageTest {
  private static final Configuration CONFIG = new Configuration();
  private static final Logger LOG = LoggerFactory.getLogger(ExampleUsageTest.class);
  private static Path BASE_DIR;
  private static MRLocalCluster CLUSTER;

  /** Generates some random data files and starts the local CLUSTER. */
  @BeforeClass
  public static void setup() throws Exception {
    String testDir = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
    BASE_DIR = new Path("target", testDir);
    createRandomDataFiles(CONFIG, new Path(BASE_DIR, "input"));
    CLUSTER = createStartedCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.stop();
    }
  }

  @Test
  public void runExample() throws Exception {
    // Stage 1: Generate sorted sequence files from input data (users will need to adapt this stage
    // to their own data)
    Job job = prepareSortToSequenceFiles("input", "top.seq", "sorted");
    job.waitForCompletion(true);

    // Stage 2: Analyze the sorted sequence files to determine splits by size
    // These keys should be used to create the pre-split HBase table
    KeyBoundaries partitioner = new KeyBoundaries(CONFIG);
    List<byte[]> boundaries =
        partitioner.partitionsOfSize(new Path(BASE_DIR, "sorted"), 12 * 1024 * 1024);
    LOG.info("Boundaries (approx 12MB)");
    boundaries.forEach(s -> LOG.info(" - {}", Bytes.toString(s)));

    // Or alternatively, split into N partitions
    boundaries = partitioner.partitionsOfNumber(new Path(BASE_DIR, "sorted"), 35);
    LOG.info("Boundaries (approx 35 partitions)");
    boundaries.forEach(s -> LOG.info(" - {}", Bytes.toString(s)));
  }

  /**
   * Prepares a Job that will use a {@link TotalOrderPartitioner} to sort input data into sequence
   * files. This starts by sampling the input data to determine the partitions into which sorted
   * data is written. The output is sequence files which will be globally sorted, but not in equal
   * sized partitions.
   *
   * @param inputDir The input data directory name
   * @param partitionFile The intermediary file for the {@link TotalOrderPartitioner} to use
   * @param outputDir The output directory into which sequence files are written
   */
  private Job prepareSortToSequenceFiles(String inputDir, String partitionFile, String outputDir)
      throws Exception {

    // Setup
    CONFIG.set("mapreduce.output.fileoutputformat.compress", "true");
    Job job = Job.getInstance(CONFIG, "Generate sorted sequence files");
    job.setJarByClass(ExampleUsageTest.class);
    job.setNumReduceTasks(7);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileInputFormat.setInputPaths(job, new Path(BASE_DIR, inputDir));
    SequenceFileOutputFormat.setOutputPath(job, new Path(BASE_DIR, outputDir));

    // Important that Mapper input and output keys are Byte
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);

    job.setPartitionerClass(TotalOrderPartitioner.class);
    TotalOrderPartitioner.setPartitionFile(
        job.getConfiguration(), new Path(BASE_DIR, partitionFile));

    // Sample input and generate a lookup file used for the partitioning
    InputSampler.Sampler<BytesWritable, Text> sampler =
        new InputSampler.RandomSampler(0.3, 100, 100);
    InputSampler.writePartitionFile(job, sampler);

    return job;
  }
}
