Bloom skip index parameters are specified in the `WITH (...)` clause:

* `bloom_filter`
  * `false_positive_probability`: Target false-positive rate of the filter (range `(0, 1)`). If omitted, the default is `0.1` for column-oriented tables and `0.0001` for row-oriented tables.
* `bloom_ngram_filter` (for string columns)
  * `ngram_size`: N-gram length, an integer from `3` to `8` (default `3`).
  * `false_positive_probability`: Target false-positive rate (range `(0, 1)`; default `0.1`).
  * `case_sensitive`: Whether n-grams respect character case: `true` or `false` (default `true`).
