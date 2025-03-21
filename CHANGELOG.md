## Unreleased

### Functionality
* 15186:Increased the query text limit size in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))

### Bug fixes
* 15160:Fixed the issue of the transaction hanging if a user performs a control plane operation with a topic (for example, adding partitions or a consumer) and the PQ tablet moves to another node. The transaction is now completed successfully. [#15070](https://github.com/ydb-platform/ydb/issues/15070) [#15160](https://github.com/ydb-platform/ydb/pull/15160) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
