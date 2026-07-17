* `GLOBAL/LOCAL` — a global or local index; depending on the index type (`<index_type>`), only one of them may be available:

  * `GLOBAL` — an index implemented as a separate table or a set of tables. Synchronous update of such an index requires distributed transactions.
  * `LOCAL` — a local index within a shard of a columnar or row-based table; it does not require distributed transactions for updates, but does not provide pruning during search.
* `<index_name>` — a unique index name that can be used to access data.
* `SYNC/ASYNC` — the index synchronicity flag.

  * `SYNC` — [synchronous](../../../../concepts/query_execution/secondary_indexes.md#sync) index. Default value.
  * `ASYNC` — [asynchronous](../../../../concepts/query_execution/secondary_indexes.md#async) index.
* `UNIQUE` — the [unique secondary index](../../../../concepts/query_execution/secondary_indexes.md#unique) flag. A unique index must be global synchronous (`GLOBAL UNIQUE SYNC`) and must not contain the `USING <index_type>` construct.
* `<index_type>` — index type; currently supported:

  * `secondary` — secondary index. Only the `GLOBAL` mode is available for secondary indexes. This is the default index type.
  * `vector_kmeans_tree` — vector index. Described in more detail in the [{#T}](../create_table/vector_index.md) section.
  * `fulltext_plain` — basic full-text index. Described in more detail in [{#T}](../create_table/fulltext_index.md).
  * `fulltext_relevance` — full-text index with [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) statistics for relevance calculation. Described in more detail in [{#T}](../create_table/fulltext_index.md).
  * `json` — JSON index for accelerating `JSON_EXISTS` and `JSON_VALUE` predicates on a column of type `Json` or `JsonDocument`. Described in more detail in [{#T}](../create_table/json_index.md).
  * `bloom_filter` — local Bloom index. Only `LOCAL` is available. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
  * `bloom_ngram_filter` — local N-gram Bloom index. Only `LOCAL` is available. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
* `<index_columns>` — a comma-separated list of column names of the table being created, which determines the composition and order of columns included in the index key. Must be specified. The index key will consist of these columns with the addition of the table's primary key columns.
* `<cover_columns>` — a comma-separated list of column names of the table being created that will be stored in the index in addition to the index key columns, allowing additional data to be retrieved without accessing the table. Empty by default.
* `<parameter_name>` and `<parameter_value>` — index parameters specific to a particular `<index_type>`.
