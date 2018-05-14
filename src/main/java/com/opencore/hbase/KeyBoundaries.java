package com.opencore.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A utility to help determine key boundaries which can be used for creating a pre-split HBase
 * table.
 *
 * <p>This requires a collection of sequence files having a key of type {@link BytesWritable} that
 * have been globally sorted. Methods are provided to analyse the files and determine the split keys
 * to partition by predefined size, or into N partitions based on total size. Because this operates
 * on sequence files and not HFiles the partitioning is not exact (due to e.g. compression) but
 * considered a close approximation. In most cases splitting into N partitions will be the expected
 * approach.
 *
 * <p>For cases when row sizes are expected to be equal, a good approach is to:
 *
 * <ol>
 *   <li>Create sorted sequence files of type <code><BytesWritable, NullWritable></code> (i.e. keys
 *       only)
 *   <li>Estimate or measure the total data size (totalBytes) that will be loaded into HBase
 *   <li>Split by N partitions where N = totalBytes / targetSize
 * </ol>
 *
 * HBase defaults to 10GB maximum file sizes (<code>hbase.hregion.max.filesize</code>). A sensible
 * file size to aim for would be 6-8GB to allow some room for growth before HBase will split the
 * table. Due to the nature of this being a close approximation, you should always allow some buffer
 * below the configured maximum file size in HBase.
 */
public class KeyBoundaries {
  private static final Logger LOG = LoggerFactory.getLogger(KeyBoundaries.class);
  private final FileSystem fs;
  private final Configuration conf;

  public KeyBoundaries(Configuration conf) throws IOException {
    this.conf = conf;
    this.fs = FileSystem.get(conf);
  }

  /**
   * Provides the row keys that fall on the boundaries when the input data is split into N equally
   * sized partitions. This is determined by dividing the total size of the sequence files by the
   * number of partitions sought to determine the size of each partition. This is then passed to
   * {@link #partitionsOfSize(Path, long)}.
   *
   * @param input The directory of sequence files
   * @param numberOfPartitions The number of partitions sought
   * @return The row keys to be used for the region splits
   * @throws IOException On error reading the sequence files
   */
  public List<byte[]> partitionsOfNumber(Path input, int numberOfPartitions) throws IOException {
    long totalBytes = fs.getContentSummary(input).getSpaceConsumed();
    Preconditions.checkArgument(totalBytes > 0, "Input must contain data");
    long targetSplitSize = totalBytes / numberOfPartitions;
    Preconditions.checkArgument(
        targetSplitSize > 0, "Unable to split input into so many partitions");
    return partitionsOfSize(input, targetSplitSize);
  }

  /**
   * Provides the row keys that fall on the boundaries when the input data is split into chunks of
   * the stated size. Note that this will not directly translate into HBase region sizes but is an
   * approximation if the sequence files hold all the data.
   *
   * @param input The directory of sequence files
   * @param partitionSizeBytes The desired size of the partitions
   * @return The row keys to be used for the region splits
   * @throws IOException On error reading the region splits
   */
  public List<byte[]> partitionsOfSize(Path input, long partitionSizeBytes) throws IOException {
    Preconditions.checkArgument(
        partitionSizeBytes > 0, "Unable to split input into so many partitions");
    FileStatus[] statuses = fs.listStatus(input);

    // Sort the files by the first key contained. There are no guarantees that the files are in any
    // meaningful sorted order
    List<FileStatus> files = Arrays.asList(statuses);
    LOG.info("Sorting {} input files by key contained", files.size());
    Collections.sort(files, new FirstByteKeyComparator());
    Iterator<FileStatus> fileIter = files.iterator();
    LOG.info("Finished sorting input files");

    List<byte[]> boundaries = new ArrayList<>(100);

    long offset = 0;
    BytesWritable key = new BytesWritable();
    while (fileIter.hasNext()) {
      FileStatus status = fileIter.next();

      // short circuit
      if (status.getLen() < partitionSizeBytes - offset) {
        offset += status.getLen();
        continue;
      }

      try (SequenceFile.Reader reader =
          new SequenceFile.Reader(conf, SequenceFile.Reader.file(status.getPath()))) {
        Preconditions.checkArgument(
            reader.getKeyClass() == BytesWritable.class,
            "Only keys of type BytesWritable are supported");
        while (reader.next(key)) {
          if (reader.getPosition() + offset >= partitionSizeBytes) {
            boundaries.add(key.copyBytes());
            offset = 0;
          }
          long advance = partitionSizeBytes - offset;

          if (reader.getPosition() + advance >= status.getLen()) {
            offset += status.getLen() - reader.getPosition(); // remaining bytes
            break; // EOF
          } else {
            reader.sync(reader.getPosition() + advance);
            offset += advance;
          }
        }
      }
    }

    return boundaries;
  }

  /**
   * A comparator of the first key for in the sequence file.
   * Employs caching to avoid multiple sequence file reads which are expensive.
   */
  private class FirstByteKeyComparator implements Comparator<FileStatus> {
    private final BytesWritable.Comparator comp = new BytesWritable.Comparator();
    private final Map<Path, BytesWritable> cache = new HashMap<>();

    @Override
    public int compare(FileStatus left, FileStatus right) {
      return comp.compare(getFirstKey(left.getPath()), getFirstKey(right.getPath()));
    }

    private synchronized BytesWritable getFirstKey(Path p) {
      if (cache.containsKey(p)) {
        return cache.get(p);
      } else {
        try (SequenceFile.Reader reader =
            new SequenceFile.Reader(conf, SequenceFile.Reader.file(p))) {
          Preconditions.checkArgument(
              reader.getKeyClass() == BytesWritable.class,
              "Only keys of type BytesWritable are supported");
          BytesWritable key = new BytesWritable();
          reader.next(key);
          cache.put(p, key);
          return key;

        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }
}
