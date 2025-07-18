## Unreleased

### Functionality

* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
and timeout (by default, the maximum response time from healthcheck). Documentation is under construction. [#15693](https://github.com/ydb-platform/ydb/pull/15693) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17394:Added counters to the spilling IO queue to track the number of waiting operations. Documentation is under construction #17599. [#17394](https://github.com/ydb-platform/ydb/pull/17394) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17362:Add support for [google breakpad](https://chromium.googlesource.com/breakpad/breakpad) inside YDB. Now you can set a minidumps path using an environment variable.
[#17362](https://github.com/ydb-platform/ydb/pull/17362) ([Олег](https://github.com/iddqdex))
* 17148:Extended federated query capabilities to support a new external data source [Prometheus](https://en.wikipedia.org/wiki/Prometheus_(software)). Documentation is under construction YQ-4261 [#17148](https://github.com/ydb-platform/ydb/pull/17148) ([Stepan](https://github.com/pstpn))
* 17007:Extended federated query capabilities to support a new external data source [Apache Iceberg](https://iceberg.apache.org). Documentation is under construction YQ-4266 [#17007](https://github.com/ydb-platform/ydb/pull/17007) ([Slusarenko Igor](https://github.com/buhtr))
* 19315:Introduce distconf cache for group info propagation. [#19315](https://github.com/ydb-platform/ydb/pull/19315) ([Alexander Rutkovsky](https://github.com/alexvru))
* 19227:YDB FQ: enable `REGEXP` pushdown for `Utf8` columns. [#19227](https://github.com/ydb-platform/ydb/pull/19227) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 19216:Added basic checks for group names when creating or renaming them: they may consists of Latin letters in both cases, digits, underscore and hyphen. [#19216](https://github.com/ydb-platform/ydb/pull/19216) ([yurikiselev](https://github.com/yurikiselev))
* 19111:Added support for specify the size of sliced portions on the zero layer. [#19111](https://github.com/ydb-platform/ydb/pull/19111) ([Oleg Doronin](https://github.com/dorooleg))
* 18988:2DC:added support pile identifiers for dynamic nodes. [#18988](https://github.com/ydb-platform/ydb/pull/18988) ([Alexander Rutkovsky](https://github.com/alexvru))
* 18859:Reconfiguration of StateStorage: added support Board reconfiguration via DistConf. [#18859](https://github.com/ydb-platform/ydb/pull/18859) ([Evgenik2](https://github.com/Evgenik2))

### Bug fixes

* 15721:Fixed a bug in YDB UUID column handling in ReadTable SDK method. [#15721](https://github.com/ydb-platform/ydb/pull/15721) ([Ivan Nikolaev](https://github.com/lex007in))
* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16060:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the **RETURNING** clause  work incorrectly with INSERT/UPSERT operations. [#16060](https://github.com/ydb-platform/ydb/pull/16060) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16021:Fixed a rare error that led to a VERIFY error when replicating data. #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16768:Fixed an issue with topic auto-partitioning when the `max_active_partition` configuration parameter was set via the `ALTER TOPIC` statement. [#16768](https://github.com/ydb-platform/ydb/pull/16768) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16764:Fixed redirects from cluster endpoints (storage nodes) to database nodes, resolving inconsistent behavior where some system tables were not visible. #16763 [#16764](https://github.com/ydb-platform/ydb/pull/16764) ([Alexey Efimov](https://github.com/adameat))
* 17198:Fixed an issue with UUID data type handling in YDB CLI backup/restore operations. [#17198](https://github.com/ydb-platform/ydb/pull/17198) ([Semyon Danilov](https://github.com/SammyVimes))
* 17157:Viewer API: Fixed the retrieval of tablet list for tables implementing secondary indexes. #17103 [#17157](https://github.com/ydb-platform/ydb/pull/17157) ([Alexey Efimov](https://github.com/adameat))
