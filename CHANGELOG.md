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
* 19893:Don't allow to execute more tasks than demand allows [#19893](https://github.com/ydb-platform/ydb/pull/19893) ([Ivan](https://github.com/abyss7))
* 19817:Added CS compaction ResourceBroker queues configuration to MemoryController [#19817](https://github.com/ydb-platform/ydb/pull/19817) ([Vladilen](https://github.com/Vladilen))
* 19735:added Accept header to oidc white list [#19735](https://github.com/ydb-platform/ydb/pull/19735) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 19679:* YDB FQ: add default protocol for OpenSearch [#19679](https://github.com/ydb-platform/ydb/pull/19679) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 19663:Yandex.cloud and YDB-platform are closely related. In particular, for YMQ it is necessary to supply audit logs in the cloud events format. However, YDB has its own standardized format. For YMQ, it is implemented in this format.: https://github.com/ydb-platform/ydb/pull/18333. To transfer audit logs to the Cloud Events format, a separate mapper will be implemented. This pull request adds a new format type, CLOUD_EVENTS. [#19663](https://github.com/ydb-platform/ydb/pull/19663) ([flown4qqqq](https://github.com/flown4qqqq))
* 19653:OIDC needs pass tracing headers [#19653](https://github.com/ydb-platform/ydb/pull/19653) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 19642:Added ColumnShard update/delete rows and bytes statistics, fixed and unmuted related unit test [#19642](https://github.com/ydb-platform/ydb/pull/19642) ([Vladilen](https://github.com/Vladilen))
* 19618:Re-implement the KQP scheduler for CPU using HDRF model [#19618](https://github.com/ydb-platform/ydb/pull/19618) ([Ivan](https://github.com/abyss7))
* 19600:healthcheck api can now report if cluster is in not bootstrapped state under configuration v2 [#19600](https://github.com/ydb-platform/ydb/pull/19600) ([vporyadke](https://github.com/vporyadke))
* 19580:Enable changing scheme limits (MAX_SHARDS, MAX_PATHS, ...) with YQL.

```sql
ALTER DATABASE `/my_db` SET (k = v);
``` [#19580](https://github.com/ydb-platform/ydb/pull/19580) ([Daniil Demin](https://github.com/jepett0))
* 19567:New operation increment. Support Integer types only. [#19567](https://github.com/ydb-platform/ydb/pull/19567) ([r314-git](https://github.com/r314-git))
* 18333:YMQ already has audit logs at the Control Plane level, which are intended for sending to Audit Trails (Yandex Cloud). However, their format is considered archaic.

In this pull request, the YMQ audit logs are added in a common format, which will be used for translation in CloudEvents format.

The logs themselves are sent based on three events:
1. Creating a queue
2. Changing queue metadata
3. Deleting a queue

This pull request implements only the very fact of the appearance of audit logs. Some fields in this PR will be filled with default values. The correct filling of the fields will be implemented in a separate PR

Moreover, in this PR masking token is not correct. It will be fixed in future PR too. [#18333](https://github.com/ydb-platform/ydb/pull/18333) ([flown4qqqq](https://github.com/flown4qqqq))

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
* 19901:If writing is done to the topic using a transaction and the retention of messages in the topic is less than the duration of the transaction, then inconsistent data could be written to the partition. [#19901](https://github.com/ydb-platform/ydb/pull/19901) ([Nikolay Shestakov](https://github.com/nshestakov))
* 19860:This Pull Request addresses a compilation error that was identified specifically in Darwin-build environments when working with cloud_events format in Yandex Message Queue. The issue emerged following the recent changes introduced in PR #18333 (https://github.com/ydb-platform/ydb/pull/18333).

The fix has been thoroughly tested on both Darwin and Linux environments to ensure that the cloud_events format functionality in Yandex Message Queue now works correctly across all supported platforms without compilation errors. [#19860](https://github.com/ydb-platform/ydb/pull/19860) ([flown4qqqq](https://github.com/flown4qqqq))
* 19781:Issue: https://github.com/ydb-platform/ydb/issues/19799 [#19781](https://github.com/ydb-platform/ydb/pull/19781) ([Semyon Danilov](https://github.com/SammyVimes))
* 19762:Fix bug with absolute paths in workloads: https://github.com/ydb-platform/ydb/issues/19619 [#19762](https://github.com/ydb-platform/ydb/pull/19762) ([Олег](https://github.com/iddqdex))
* 19677:make nodes less critical (to make cluster less critical), closes #19676 [#19677](https://github.com/ydb-platform/ydb/pull/19677) ([Alexey Efimov](https://github.com/adameat))

### Performance

* 19807:Changed the retry policy settings. Users will receive faster confirmation that the server has written the message.

Added logging of requests to KQP. [#19807](https://github.com/ydb-platform/ydb/pull/19807) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

