Indexed vector search completeness or performance may decrease after updating a large amount of data in a table with a vector index. For more details, see [Updating Vector Indexes](../dev/vector-indexes.md#update).

If automatic index updates are disabled ([`autoupdate=false`](../dev/vector-indexes.md#autoupdate)), table changes do not reach the index. Rebuild the index to include them in searches.
