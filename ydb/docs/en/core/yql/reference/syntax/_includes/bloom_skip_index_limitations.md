* The index is always local (`LOCAL`); there is no global variant.
* `COVER (...)` and data columns are not supported.
* For column-oriented tables, exactly one indexed column is allowed. For row-oriented tables, multiple indexed columns are allowed.
* The `bloom_ngram_filter` index type is not supported for row-oriented tables.
* The filter is applied on reads only for data where an index block was already persisted during a write or rebuild; for other fragments, skipping with this index is not used until they are rebuilt.
* Queries do not use the `VIEW <index>` syntax (unlike, for example, [fulltext indexes](../../../../dev/fulltext-indexes.md)).
