* The index is always local (`LOCAL`); there is no global variant.
* `COVER (...)` and data columns are not supported.
* For column-oriented tables, exactly one indexed column is allowed. For row-oriented tables, multiple indexed columns are allowed.
* The `bloom_ngram_filter` index type is not supported for row-oriented tables.
* Queries do not use the `VIEW <index>` syntax (unlike, for example, [fulltext indexes](../../../../dev/fulltext-indexes.md)).
