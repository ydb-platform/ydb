* `COVER (...)` and extra index columns are not supported.
* Column-oriented tables allow only one indexed column. Row-oriented tables allow multiple indexed columns.
* `bloom_ngram_filter` is not supported on row-oriented tables.
<<<<<<< HEAD
* On row-oriented tables, the indexed columns of a `bloom_filter` must be a left prefix of the [primary key](../../../../concepts/glossary.md#primary-key). Non-prefix column sets are rejected.
* On row-oriented tables, two `bloom_filter` indexes cannot share the same prefix length (the same set of leading primary-key columns).
=======
>>>>>>> 40b22cd2351 (added docs for bloom indexes (#38535))
