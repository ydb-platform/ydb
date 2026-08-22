* `GLOBAL/LOCAL` — global or local index, depending on the index type (`<index_type>`), only one of them may be available:

  * `GLOBAL` — an index implemented as a separate table or a set of tables. Synchronous update of such an index requires distributed transactions.
  * `LOCAL` — a local index within a shard of a columnar or row-based table, does not require distributed transactions during update, but does not provide pruning during search.
* `<index_name>` — unique index name by which data can be accessed.
* `SYNC/ASYNC` — indicator of index synchrony.

  * `SYNC` - [synchronous](../../../../concepts/query_execution/secondary_indexes.md#sync) index. Default value.
  * `ASYNC` - [asynchronous](../../../../concepts/query_execution/secondary_indexes.md#async) index.
* `UNIQUE` — indicator of a [unique secondary index](../../../../concepts/query_execution/secondary_indexes.md#unique). A unique index must be global synchronous (`GLOBAL UNIQUE SYNC`) and must not contain the `USING <index_type>` construct.
* `<index_type>` — index type, currently supported:

  * `secondary` — secondary index. Only `GLOBAL` mode is available for secondary indexes. This is the default index type.
  * `vector_kmeans_tree` — vector index. Described in detail in [{#T}](../create_table/vector_index.md).
  * `fulltext_plain` — basic fulltext index. Described in detail in [{#T}](../create_table/fulltext_index.md).
  * `fulltext_relevance` — fulltext index with [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) statistics for relevance scoring. Described in detail in [{#T}](../create_table/fulltext_index.md).
  * `json` — JSON index to speed up `JSON_EXISTS` and `JSON_VALUE` predicates on a column of type `Json` or `JsonDocument`. Described in more detail in [{#T}](../create_table/json_index.md).
  * `bloom_filter` — local Bloom index. Available only for `LOCAL`. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
  * `bloom_ngram_filter` — local N-gram Bloom index. Available only for `LOCAL`. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
  * `min_max` — local min/max index. Available only for `LOCAL`. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-min-max).
* `<index_columns>` — comma-separated list of column names for the table being created. This list defines the composition and order of columns included in the index key. Must be specified. The index key will include both the columns listed and the columns from the table's primary key.
* `<cover_columns>` — comma-separated list of column names from the created table that will be saved in the index in addition to index key columns, providing the ability to get additional data without accessing the table. Empty by default.
* `<parameter_name>` and `<parameter_value>` — index parameters specific to a particular `<index_type>`. Some index parameters cannot be specified during index creation. See [Altering an index](../alter_table/indexes.md#alter-index).
