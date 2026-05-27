A [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is a probabilistic data structure for testing set membership. It may produce [false positives](https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives) (the element is not in the set, but the filter reports that it might be), but there are no false negatives: if any checked bit is zero, the element is not in the set.

When reading a table, the filter lets the storage layer skip data fragments that cannot contain the requested value, reducing the amount of data actually read.
