## Unreleased

### Functionality
* 15186:Increased the query text limit size in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))

### Bug fixes
* 15160:Fixed an issue where, if a user performs a control plane operation with a topic (for example, adding partitions or a consumer) and, at an unfortunate moment, the PQ tablet moves to another node, the transaction may hang. To the user, this would appear as "the operation is frozen". [#15070](https://github.com/ydb-platform/ydb/issues/15070) [#15160](https://github.com/ydb-platform/ydb/pull/15160) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

