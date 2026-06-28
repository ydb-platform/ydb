# Local indexes

[Local indexes](../../concepts/query_execution/local_indexes.md) are auxiliary structures stored together with table data and applied while reading from storage. Unlike global indexes, they do not materialize a separate index table and are not selected in queries with `VIEW`.

This section contains practical guides for kinds of local indexes:

* [{#T}](../bloom-skip-indexes.md) — Bloom skip indexes that speed up selective queries by skipping data fragments.
