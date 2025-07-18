## Unreleased

### Functionality

* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
and timeout (by default, the maximum response time from healthcheck). Documentation is under construction. [#15693](https://github.com/ydb-platform/ydb/pull/15693) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17394:Added counters to the spilling IO queue to track the number of waiting operations. Documentation is under construction #17599. [#17394](https://github.com/ydb-platform/ydb/pull/17394) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17362:Add support for [Google Breakpad](https://chromium.googlesource.com/breakpad/breakpad) to YDB. Now you can set a minidumps path using an environment variable.
[#17362](https://github.com/ydb-platform/ydb/pull/17362) ([Олег](https://github.com/iddqdex))
* 17148:Extended federated query capabilities to support a new external data source [Prometheus](https://en.wikipedia.org/wiki/Prometheus_(software)). Documentation is under construction YQ-4261 [#17148](https://github.com/ydb-platform/ydb/pull/17148) ([Stepan](https://github.com/pstpn))
* 17007:Extended federated query capabilities to support a new external data source [Apache Iceberg](https://iceberg.apache.org). Documentation is under construction YQ-4266 [#17007](https://github.com/ydb-platform/ydb/pull/17007) ([Slusarenko Igor](https://github.com/buhtr))
* 16076:Added automatic cleanup of temporary tables and directories created during S3 export operations. Previously, users had to manually remove temporary directories and tables that were created as part of the export pipeline. [#16076](https://github.com/ydb-platform/ydb/pull/16076) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 20176:Added automatic creation and deletion of a service consumer for topic compaction. [#20176](https://github.com/ydb-platform/ydb/pull/20176) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 19994:Added CS Group Memory Limiter and Blob cache configuration to MemoryController. (#19708) [#19994](https://github.com/ydb-platform/ydb/pull/19994) ([Vladilen](https://github.com/Vladilen))
* 19678:Added support for [idempotent producers](https://cwiki.apache.org/confluence/display/KAFKA/Idempotent+Producer) to the YDB implementation of the Apache Kafka protocol. [#19678](https://github.com/ydb-platform/ydb/pull/19678) ([qyryq](https://github.com/qyryq))
* 19444:Introduced a new UnorderedData protocol for full synchronization, which allows taking a full index snapshot only once per full sync session. The new protocol will be used to send DoNotKeep flags from Phantom Flag Storage, which will be sent without relying on the linear order. For now the new protocol is disabled by default. [#19444](https://github.com/ydb-platform/ydb/pull/19444) ([Sergey Belyakov](https://github.com/serbel324))
* 19337:* Introduced two new Distributed Storage parameters — `GroupSizeInUnits` property of storage groups and `SlotSizeInUnits` of PDisks. Docs: #19364 [#19337](https://github.com/ydb-platform/ydb/pull/19337) ([Yaroslav Dynnikov](https://github.com/rosik))
* 18955:Added a new external data source to YDB topics. [#18955](https://github.com/ydb-platform/ydb/pull/18955) ([Dmitry Kardymon](https://github.com/kardymonds))
* 18544:Added shared metadata cache. Now all column shards on the same node share the same metadata cache by default. To disable the shared metadata cache, use the `EnableSharedMetadataAccessorCache` flag. The default cache size is 1 GB, this value can be changed in the `SharedMetadataAccessorCacheSize` parameter. [#18544](https://github.com/ydb-platform/ydb/pull/18544) ([Iurii Kravchenko](https://github.com/XJIE6))
* 19680:Implemented the select load type for the vector workload test. [#19680](https://github.com/ydb-platform/ydb/pull/19680) ([azevaykin](https://github.com/azevaykin))
* 20040:Enabled verbose memory limit by default in recipe [#20040](https://github.com/ydb-platform/ydb/pull/20040) ([Ivan](https://github.com/abyss7))

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
* 20238:CS fixed race in CPU limiter [#20238](https://github.com/ydb-platform/ydb/pull/20238) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 20223:Do not fold SafeCast because it could produce unexpected result
Fix for KIKIMR-23489 [#20223](https://github.com/ydb-platform/ydb/pull/20223) ([Denis Khalikov](https://github.com/denis0x0D))
* 20217:Fixed issue [#18604](https://github.com/ydb-platform/ydb/issues/18604). Now the force availability mode ignores the limits of offline nodes. ([Ilia Shakhov](https://github.com/pixcc))
* 20175:fixes https://github.com/ydb-platform/ydb/issues/17462
fixes https://github.com/ydb-platform/ydb/issues/20294 [#20175](https://github.com/ydb-platform/ydb/pull/20175) ([pilik](https://github.com/pashandor789))
* 20157:Fixed use after free in CPU scheduler, fixed verify fail in CS CPU limiter: https://github.com/ydb-platform/ydb/issues/20116 [#20157](https://github.com/ydb-platform/ydb/pull/20157) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 20138:Issue: https://github.com/ydb-platform/ydb/issues/20139 [#20138](https://github.com/ydb-platform/ydb/pull/20138) ([Semyon Danilov](https://github.com/SammyVimes))
* 20084:Fixed an issue where committing message offsets when reading a topic might result in the following error: "Unable to navigate:path: 'Root/logbroker-federation/--cut--/stable/guidance' status: PathErrorUnknown"
Сломали начиная с 25-1-2 [#20084](https://github.com/ydb-platform/ydb/pull/20084) ([Nikolay Shestakov](https://github.com/nshestakov))

### Performance

* 20195:- Use pool's usage and fair-share to limit schedulable tasks - instead of query's ones.
- Fix burst throttle metric.
- Add histogram of delays metric.
- Use FIFO distribution for queries fair-share (not used, but improves performance). [#20195](https://github.com/ydb-platform/ydb/pull/20195) ([Ivan](https://github.com/abyss7))
* 20128:Added a cache of SchemeNavigate responses. Users will receive faster confirmation that the server has written the message. [#20128](https://github.com/ydb-platform/ydb/pull/20128) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 20048:Improved performance of upsert operations for tables with defaults. Upsert operations now ignore the last DefaulFilledColumnsCnt columns if a row exist. This logic allows QP to avoid reading rows to check if it exists (the whole operation with default columns is processed at a shard). [#20048](https://github.com/ydb-platform/ydb/pull/20048) ([r314-git](https://github.com/r314-git))
* 19724:Added the `buffer_page_alloc_size` configuration parameter with the default value of 4Kb [#19724](https://github.com/ydb-platform/ydb/pull/19724) ([Ivan](https://github.com/abyss7))
* 19687:Password verification is extracted to a separate actor from `TSchemeShard` local transaction. [#19687](https://github.com/ydb-platform/ydb/pull/19687) ([Yury Kiselev](https://github.com/yurikiselev))
