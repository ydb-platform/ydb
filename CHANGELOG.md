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
* 16076:Added automatic cleanup of temporary tables and directories created during S3 export operations. Previously, users had to manually remove temporary directories and tables that were created as part of the export pipeline. [#16076](https://github.com/ydb-platform/ydb/pull/16076) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 19893:Implemented new robust algorithm for HDRF scheduler that don't allow to execute more tasks than demand allows. [#19893](https://github.com/ydb-platform/ydb/pull/19893) ([Ivan](https://github.com/abyss7))
* 19817:Added CS compaction ResourceBroker queues configuration to MemoryController. [#19817](https://github.com/ydb-platform/ydb/pull/19817) ([Vladilen](https://github.com/Vladilen))
* 19735:Added Accept header to oidc white list. [#19735](https://github.com/ydb-platform/ydb/pull/19735) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 19653:OIDC needs pass tracing headers [#19653](https://github.com/ydb-platform/ydb/pull/19653) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 19642:Added ColumnShard update/delete rows and bytes statistics. [#19642](https://github.com/ydb-platform/ydb/pull/19642) ([Vladilen](https://github.com/Vladilen))
* 19618:Re-implement the KQP scheduler for CPU using HDRF model. [#19618](https://github.com/ydb-platform/ydb/pull/19618) ([Ivan](https://github.com/abyss7))
* 19600:Healthcheck API can now report if cluster is in not bootstrapped state under configuration v2. [#19600](https://github.com/ydb-platform/ydb/pull/19600) ([vporyadke](https://github.com/vporyadke))
* 19580:Added support fo changing [schema object limits](./concepts/limits-ydb#schema-object) (MAX_SHARDS, MAX_PATHS, ...) with YQL. ([Daniil Demin](https://github.com/jepett0))
* 19567:Added new operation increment, support Integer types only. [#19567](https://github.com/ydb-platform/ydb/pull/19567) ([r314-git](https://github.com/r314-git))
* 18333:Added audit logs for YMQ events in common ydb format. [#18333](https://github.com/ydb-platform/ydb/pull/18333) ([flown4qqqq](https://github.com/flown4qqqq))

### Bug fixes

* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16764:Fixed redirects from cluster endpoints (storage nodes) to database nodes, resolving inconsistent behavior where some system tables were not visible. #16763 [#16764](https://github.com/ydb-platform/ydb/pull/16764) ([Alexey Efimov](https://github.com/adameat))
* 17198:Fixed an issue with UUID data type handling in YDB CLI backup/restore operations. [#17198](https://github.com/ydb-platform/ydb/pull/17198) ([Semyon Danilov](https://github.com/SammyVimes))
* 17157:Viewer API: Fixed the retrieval of tablet list for tables implementing secondary indexes. #17103 [#17157](https://github.com/ydb-platform/ydb/pull/17157) ([Alexey Efimov](https://github.com/adameat))
* 19781:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19799) - VERIFY in CMS when trying to lock pdisk without vdisks twice. [#19781](https://github.com/ydb-platform/ydb/pull/19781) ([Semyon Danilov](https://github.com/SammyVimes))
* 19762:Fixed a [bug](https://github.com/ydb-platform/ydb/issues/19619) with absolute paths in workloads. [#19762](https://github.com/ydb-platform/ydb/pull/19762) ([Олег](https://github.com/iddqdex))
