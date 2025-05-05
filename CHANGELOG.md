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
* 17686:Now test wait for all the events before finishing
... [#17686](https://github.com/ydb-platform/ydb/pull/17686) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17682:Last offset and custom message size limit support [#17682](https://github.com/ydb-platform/ydb/pull/17682) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 17672:Added additional CLI parameters for `ydb debug latency`:
* --min-inflight
* allow to specify multiple percentiles using multiple "-p" params [#17672](https://github.com/ydb-platform/ydb/pull/17672) ([Evgeniy Ivanov](https://github.com/eivanov89))
* 17650:`Add` operation allows building aggregation queries. For instance, constucting Roaring bitmap from intermediate `AGG_LIST` may be replaced with custom `UDAF` using `Add`, which may be more space efficient

`IsEmpty` just nice operation to have, allows skip costly check for cardinality equals to zero [#17650](https://github.com/ydb-platform/ydb/pull/17650) ([jsjant](https://github.com/jsjant))
* 17618:Don't allow SelfHeal actor to create more than 1 active ReassignerActor. This change will prevent SelfHeal from overloading BSC with ReassignItem requests thus causing DoS. [#17618](https://github.com/ydb-platform/ydb/pull/17618) ([Sergey Belyakov](https://github.com/serbel324))
* 17561:Added support for traceparent and updated gRPC client and request settings in the C++ SDK. [#17561](https://github.com/ydb-platform/ydb/pull/17561) ([Zarina Tlupova](https://github.com/zarinatlupova))
* 17537:The language version may affect the computation layer, so it should be passed via dq task [#17537](https://github.com/ydb-platform/ydb/pull/17537) ([Vitaly Stoyan](https://github.com/vitstn))
* 17360:Add a setting that specifies the initial stream offset by timestamp or duration [#17360](https://github.com/ydb-platform/ydb/pull/17360) ([Alexey Pozdniakov](https://github.com/APozdniakov))
* 17324:Support for reading data from Yandex Monitoring [#17324](https://github.com/ydb-platform/ydb/pull/17324) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))
* 17314:* Added progress stats period into execute script request API (periodic statistics updates)
* Added dynamic updates for query service config [#17314](https://github.com/ydb-platform/ydb/pull/17314) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 16932:Add `MaxFaultyPDisksPerNode` to CMS config, so that Sentinel could not exceed that number. This may be useful in case if a node with a large count of disks goes down but it is known that this node will be brought back in the near future. This way, we will not be moving many disks at once (which can be rather long in case of HDDs). If `MaxFaultyPDisksPerNode` is 0, then there is no limit. [#16932](https://github.com/ydb-platform/ydb/pull/16932) ([Semyon Danilov](https://github.com/SammyVimes))

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
* 17606:healthcheck: fix check for unknown StatusV2 in VDisks (https://github.com/ydb-platform/ydb/issues/17619) А forward compatibility issue fix [#17606](https://github.com/ydb-platform/ydb/pull/17606) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17556:Fix S3 export multipart upload: EntityTooSmall error
https://github.com/ydb-platform/ydb/issues/16873 [#17556](https://github.com/ydb-platform/ydb/pull/17556) ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 17497:The SDK user writes messages to the topic in a transaction. It does not wait for confirmation that the messages have been recorded and calls Commit. The processing of this and the following transactions in the PQ tablet stops.

When processing the `UserActionAndTransactionEvents` queue, it was not taken into account that the `TEvGetWriteInfoError` message was received. As a result, the transaction remained at the head of the queue and blocked the processing of other operations.

Issue #17499 [#17497](https://github.com/ydb-platform/ydb/pull/17497) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 17468:fix output of optional structs, closes #17226 [#17468](https://github.com/ydb-platform/ydb/pull/17468) ([Alexey Efimov](https://github.com/adameat))
* 17429:https://github.com/ydb-platform/ydb/issues/16176 [#17429](https://github.com/ydb-platform/ydb/pull/17429) ([Nikolay Shestakov](https://github.com/nshestakov))
* 17418:Previously replication didn't handle StatusFlags from PDisk properly and didn't stop when available space was running low. Because of this, space was consumed until VDisk and corresponding storage group became read-only. With this change, replication will postpone on YELLOW_STOP color flag and resume after a certain delay if enough space is available.

https://github.com/ydb-platform/ydb/issues/13608 [#17418](https://github.com/ydb-platform/ydb/pull/17418) ([Sergey Belyakov](https://github.com/serbel324))
* 16208:YQ-4205 fixed query service config passing into SS [#16208](https://github.com/ydb-platform/ydb/pull/16208) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))

### Performance

* 17725:Limit internal inflight config updates. [#17725](https://github.com/ydb-platform/ydb/pull/17725) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 17611:Introduce Intersect boolean operation to avoid actual intersection and checking cardinality after [#17611](https://github.com/ydb-platform/ydb/pull/17611) ([jsjant](https://github.com/jsjant))
* 17533:Constant folding now also handles deterministic UDFs.
Non-deterministic UDFs are detected via a black list [#17533](https://github.com/ydb-platform/ydb/pull/17533) ([Pavel Velikhov](https://github.com/pavelvelikhov))

