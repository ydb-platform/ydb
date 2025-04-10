## Unreleased

### Functionality
* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* None:15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
* 16994:Create 30 groups of columns instead of 100 under asan to eliminate timeout

... [#16994](https://github.com/ydb-platform/ydb/pull/16994) ([Alexander Avdonkin](https://github.com/aavdonkin))

### Bug fixes
* 15721:Fixed a bug in YDB UUID column handling in ReadTable SDK method. [#15721](https://github.com/ydb-platform/ydb/pull/15721) ([Ivan Nikolaev](https://github.com/lex007in))
* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16060:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the **RETURNING** clause  work incorrectly with INSERT/UPSERT operations. [#16060](https://github.com/ydb-platform/ydb/pull/16060) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 10650:21:Fixed a rare error that led to a VERIFY error when replicating data. #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16958:Fixed unauthorized error in `ydb admin database restore` when multiple database admins are in dump. https://github.com/ydb-platform/ydb/issues/16833 [#16958](https://github.com/ydb-platform/ydb/pull/16958) ([Ilia Shakhov](https://github.com/pixcc))
* 16943:Fixed scheme error in `ydb admin cluster dump` when specifying a domain database. https://github.com/ydb-platform/ydb/issues/16262 [#16943](https://github.com/ydb-platform/ydb/pull/16943) ([Ilia Shakhov](https://github.com/pixcc))

