## Unreleased

### Functionality
* 15186:Increased the query text limit size in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))

### Bug fixes
* 15070:60:Fixed the issue of the transaction hanging if a user performs a control plane operation with a topic (for example, adding partitions or a consumer) and the PQ tablet moves to another node. The transaction is now completed successfully. [#15070](https://github.com/ydb-platform/ydb/issues/15070) [#15160](https://github.com/ydb-platform/ydb/pull/15160) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 16061:Init spec columns if they are needed for program execution in PLAIN reader [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16060:fixes https://github.com/ydb-platform/ydb/issues/15551 [#16060](https://github.com/ydb-platform/ydb/pull/16060) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16021:Fix replication bug #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed possible memory travel when balancing a topic reading session
https://github.com/ydb-platform/ydb/issues/16017
... [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))

