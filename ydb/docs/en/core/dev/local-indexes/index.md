# Local indexes

[Local indexes](../../concepts/query_execution/local_indexes.md) are auxiliary structures stored together with table data and applied while reading from storage. Unlike global indexes, they do not materialize a separate index table and are not selected in queries with `VIEW`.

This section contains practical materials on local index types:

* [{#T}](../bloom-skip-indexes.md) — Bloom indexes for accelerating selective queries by skipping data fragments.
* [{#T}](../min_max-skip-index.md) — min_max index that skips data fragments based on saved value ranges.
