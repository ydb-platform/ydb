* `GLOBAL/LOCAL` — a global or local index; depending on the index type (`<index_type>`), only one of them may be available:

  * `GLOBAL` — an index implemented as a separate table or set of tables. Synchronous updating of such an index requires distributed transactions.
  * `LOCAL` — a local index within a shard of a columnar or row table, does not require distributed transactions when updating, but does not provide pruning during search.
* `<index_name>` — the unique index name by which data can be accessed.
* `SYNC/ASYNC` — index synchrony flag.

  * `SYNC` - [synchronous](../../../../concepts/query_execution/secondary_indexes.md#sync) index. Default value.
  * `ASYNC` - [asynchronous](../../../../concepts/query_execution/secondary_indexes.md#async) index.
* `UNIQUE` — a flag for a [unique secondary index](../../../../concepts/query_execution/secondary_indexes.md#unique). A unique index must be global and synchronous (`GLOBAL UNIQUE SYNC`) and must not contain the `USING <index_type>` construct.
* `<index_type>` - index type, currently supported:

  * `secondary` — secondary index. For secondary indexes, only mode `GLOBAL` is available. This is the default index type.
  * `vector_kmeans_tree` — vector index. See the section [{#T}](../create_table/vector_index.md) for more details.
  * `fulltext_plain` — basic full-text index. See [{#T}](../create_table/fulltext_index.md) for more details.
  * `fulltext_relevance` — full-text index with BM25 statistics ( [BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) for relevance scoring. See [{#T}](../create_table/fulltext_index.md) for more details.
  * `json` — JSON index to accelerate predicates `JSON_EXISTS` and `JSON_VALUE` on a column of type `Json` or `JsonDocument`. See [{#T}](../create_table/json_index.md) for more details.
  * `bloom_filter` — local Bloom index. Available only in `LOCAL`. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
  * `bloom_ngram_filter` — local N-gram Bloom index. Available only in `LOCAL`. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
* `<index_columns>` — a comma-separated list of column names of the table being created, which determines the composition and order of columns in the index key. It must be specified. The index key will consist of these columns plus the primary key columns of the table.
* `<cover_columns>` — a comma-separated list of column names of the table being created that will be stored in the index in addition to the index key columns, allowing retrieval of extra data without fetching it from the table. Defaults to empty.
* `<parameter_name>` and `<parameter_value>` — index parameters specific to a particular `<index_type>`.
