* `GLOBAL` — an index implemented as a separate table or set of tables. Synchronous updates to such an index require distributed transactions.  
* `<index_name>` — a unique name of the index that will be used to access data.
* `UNIQUE` — required to create a [unique index](../../../../concepts/query_execution/secondary_indexes.md#unique). A unique index must always be created as global and synchronous (`GLOBAL UNIQUE SYNC`) and must not specify a `USING <index_type>` clause.
* `SYNC/ASYNC` — the index synchronization mode.

    * `SYNC` — a [synchronous](../../../../concepts/query_execution/secondary_indexes.md#sync) index. This is the default value.
    * `ASYNC` — an [asynchronous](../../../../concepts/query_execution/secondary_indexes.md#async) index.

* `<index_type>` — index type, currently supported:

    * `secondary` — secondary index. Only `GLOBAL` mode is available for secondary indexes. This is the default index type.
    * `vector_kmeans_tree` — vector index. Described in detail in [{#T}](../create_table/vector_index.md).
    * `fulltext_plain` — basic fulltext index. Described in detail in [{#T}](../create_table/fulltext_index.md).
    * `fulltext_relevance` — fulltext index with [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) statistics for relevance scoring. Described in detail in [{#T}](../create_table/fulltext_index.md).
    * `bloom_filter` — local Bloom skip index. Only `LOCAL` is available. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).
    * `bloom_ngram_filter` — local N-gram Bloom skip index. Only `LOCAL` is available. See [ALTER TABLE ADD INDEX](../alter_table/indexes.md#local-bloom).

* `<index_columns>` — comma-separated list of column names for the table being created. This list defines the composition and order of columns included in the index key. Must be specified. The index key will include both the columns listed and the columns from the table's primary key.
* `<cover_columns>` — comma-separated list of column names from the created table that will be saved in the index in addition to index key columns, providing the ability to get additional data without accessing the table. Empty by default.
* `<parameter_name>` and `<parameter_value>` — index parameters specific to a particular `<index_type>`.
