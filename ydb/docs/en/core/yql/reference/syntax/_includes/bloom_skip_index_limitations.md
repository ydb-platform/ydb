* The index is always [local](../../../../concepts/glossary.md#local-index) (`LOCAL`); there is no [global](../../../../concepts/glossary.md#secondary-index) variant.
* `COVER (...)` and extra index columns are not supported.
* Column-oriented tables allow only one indexed column. Row-oriented tables allow multiple indexed columns.
* `bloom_ngram_filter` is not supported on row-oriented tables (only on column-oriented tables).
* The filter is used on read only for data where an index block was already stored on write or rebuild. Other fragments are not skipped by this index until rebuild.
* Queries do not use `VIEW <index>` syntax (unlike, for example, [fulltext indexes](../../../../dev/fulltext-indexes.md)).
