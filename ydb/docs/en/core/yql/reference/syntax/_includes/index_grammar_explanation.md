* `GLOBAL/LOCAL` — global or local index; depending on the index type (`<index_type>`), only one of them may be available:
  
    * `GLOBAL` — an index implemented as a separate table or set of tables. Synchronous updates to such an index require distributed transactions.  
    * `LOCAL` — a local index within a shard of a row-oriented or column-oriented table. Does not require distributed transactions for updates, but does not provide pruning during search.

* `<index_name>` — unique index name that will be used to access data.
* `SYNC/ASYNC` — synchronous or asynchronous writes to the index, synchronous by default.
* `<index_type>` - index type, currently supported:

    * `secondary` — secondary index. Only `GLOBAL` is available. This is the default value.
    * `vector_kmeans_tree` — vector index. Described in detail in [{#T}](../create_table/vector_index.md).

* `UNIQUE` — creates an index with uniqueness guarantee for inserted values.
* `<index_columns>` — comma-separated list of column names from the created table that can be used for index searches. Must be specified.
* `<cover_columns>` — comma-separated list of column names from the created table that will be saved in the index in addition to search columns, providing the ability to get additional data without accessing the table. Empty by default.
* `<parameter_name>` and `<parameter_value>` — index parameters specific to a particular `<index_type>`.