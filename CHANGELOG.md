## Unreleased

### Functionality
* 15186:Increased the query text limit size in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15693:changed naming healthcheck config [#15693](https://github.com/ydb-platform/ydb/pull/15693) ([Andrei Rykov](https://github.com/StekPerepolnen))

### Bug fixes
* 15070:60:Fixed the issue of the transaction hanging if a user performs a control plane operation with a topic (for example, adding partitions or a consumer) and the PQ tablet moves to another node. The transaction is now completed successfully. [#15070](https://github.com/ydb-platform/ydb/issues/15070) [#15160](https://github.com/ydb-platform/ydb/pull/15160) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 15721:Fix UUID type conversion in ReadTable [#15721](https://github.com/ydb-platform/ydb/pull/15721) ([Ivan Nikolaev](https://github.com/lex007in))

