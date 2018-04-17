# hbase-bulk-load-balanced
Preparing data to ensure well balanced regions

This project explores the notion of writing data to sequence files ordered by key,
and then analysing those files to determine key ranges that will result in balanced
HBase regions.

A complete working example is in the test demonstrating the intended use. It is expected
that users of this project will be able to write the sequence file from their data following
the test as a blueprint, but can then use this project as a dependency to determine the
splits - i.e. use the `KeyBoundaries`.

