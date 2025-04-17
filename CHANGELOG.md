## Unreleased

### Functionality
* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* None:15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
* 17282:https://github.com/ydb-platform/ydb/issues/16468 [#17282](https://github.com/ydb-platform/ydb/pull/17282) ([Олег](https://github.com/iddqdex))
* 17230:* YDB FQ: enhance error occurring during Generic::TPartition parsing [#17230](https://github.com/ydb-platform/ydb/pull/17230) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 17222:https://github.com/ydb-platform/ydb/issues/16944 [#17222](https://github.com/ydb-platform/ydb/pull/17222) ([Олег](https://github.com/iddqdex))

### Bug fixes
* 15721:Fixed a bug in YDB UUID column handling in ReadTable SDK method. [#15721](https://github.com/ydb-platform/ydb/pull/15721) ([Ivan Nikolaev](https://github.com/lex007in))
* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16060:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the **RETURNING** clause  work incorrectly with INSERT/UPSERT operations. [#16060](https://github.com/ydb-platform/ydb/pull/16060) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 10650:21:Fixed a rare error that led to a VERIFY error when replicating data. #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16768:Fixed an issue with topic auto-partitioning when the `max_active_partition` configuration parameter was set via the `ALTER TOPIC` statement. [#16768](https://github.com/ydb-platform/ydb/pull/16768) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16763:4:Fixed redirects from cluster endpoints (storage nodes) to database nodes, resolving inconsistent behavior where some system tables were not visible. #16763 [#16764](https://github.com/ydb-platform/ydb/pull/16764) ([Alexey Efimov](https://github.com/adameat))
* 17285:https://github.com/ydb-platform/ydb/issues/16984
Нашел неоднозначность в q21 clickbench [#17285](https://github.com/ydb-platform/ydb/pull/17285) ([Олег](https://github.com/iddqdex))

