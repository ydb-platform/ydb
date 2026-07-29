* `GLOBAL/LOCAL` — global or local index, depending on the index type (`<index_type>`), only one of them may be available:

  * `GLOBAL` — an index implemented as a separate table or a set of tables. Synchronous update of such an index requires distributed transactions.
  * `LOCAL` — a local index within a shard of a columnar or row-based table, does not require distributed transactions during update, but does not provide pruning during search.
* `<index_name>` — unique index name by which data can be accessed.
* `SYNC/ASYNC` — indicator of index synchrony.

  * `SYNC` - [synchronous](../../../../concepts/query_execution/secondary_indexes.md#sync) index. Default value.
  * `ASYNC` - [asynchronous](../../../../concepts/query_execution/secondary_indexes.md#async) index.
* `UNIQUE` — indicator of a [unique secondary index](../../../../concepts/query_execution/secondary_indexes.md#unique). A unique index must be global synchronous (`GLOBAL UNIQUE SYNC`) and must not contain the `USING <index_type>` construct.
* `<index_type>` - index type, currently supported:

  * `secondary` — secondary index. Only the `GLOBAL` mode is available for secondary indexes. This is the default index type.
  * `vector_kmeans_tree` — vector index. More details are described in the [{#T}](../create_table/vector_index.md) section.
  * `fulltext_plain` — basic full-text index. More details are described in [{#T}](../create_table/fulltext_index.md).
  * `fulltext_relevance` — full-text index with [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) statistics for relevance calculation. More details are described in [{#T}](../create_table/fulltext_index.md).
  * `json` — JSON index to speed up predicates `JSON_EXISTS` and `JSON_VALUE` on a column of type `Json` or `JsonDocument`. More details are described in [{#T}](../create_table/json_index.md).
  * `bloom_filter` — local Bloom index. Only `LOCAL` is available. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
  * `bloom_ngram_filter` — local N-gram Bloom index. Only `LOCAL` is available. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
* `<index_columns>` — comma-separated list of column names of the table being created, which determines the composition and order of columns included in the index key. Must be specified. The index key will consist of these columns with the addition of the table's primary key columns.
* `<cover_columns>` — comma-separated list of column names of the table being created that will be stored in the index in addition to the index key columns, allowing you to get additional data without accessing the table. Empty by default.
* `<parameter_name>` and `<parameter_value>` are index parameters specific to a particular `<index_type>`.
