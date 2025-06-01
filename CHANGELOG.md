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
* 19024:* YDB FQ: support REGEXP pushdown for Generic provider [#19024](https://github.com/ydb-platform/ydb/pull/19024) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 18968:Added new config parameter _EnbaleRuntimeListing, that enables/disables runtime metrics listing. [#18968](https://github.com/ydb-platform/ydb/pull/18968) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))
* 18964:Исправление теста TestReadUpdateWriteLoad.test[read_update_write_load] [#18964](https://github.com/ydb-platform/ydb/pull/18964) ([Oleg Doronin](https://github.com/dorooleg))
* 18886:Solomon responses with http code 503 are now retryable [#18886](https://github.com/ydb-platform/ydb/pull/18886) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))
* 18683:Enable creation of compacted topics for kafka API [#18683](https://github.com/ydb-platform/ydb/pull/18683) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 18663:pass folder_id parameter to ticket parser [#18663](https://github.com/ydb-platform/ydb/pull/18663) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 18360:Switch on new query workload [#18360](https://github.com/ydb-platform/ydb/pull/18360) ([Олег](https://github.com/iddqdex))
* 18263:- Topic SDK read session connects directly to partition nodes reducing inter-node network communication [#18263](https://github.com/ydb-platform/ydb/pull/18263) ([qyryq](https://github.com/qyryq))
* 17888:Add new check configuration version command for ydb cli [#17888](https://github.com/ydb-platform/ydb/pull/17888) ([mregrock](https://github.com/mregrock))
* 17663:New optimisations for reading from solomon [#17663](https://github.com/ydb-platform/ydb/pull/17663) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))
* 17525:Implemented ReconfigStateStorage API in distconf
Multiple StateStorage ring groups are allowed [#17525](https://github.com/ydb-platform/ydb/pull/17525) ([Evgenik2](https://github.com/Evgenik2))
* 17421:* Added number commited result rows into metadata, also added flag `finished` for each result set
* Supported reading for incompleted results
* Added test on reading incompleted results [#17421](https://github.com/ydb-platform/ydb/pull/17421) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))

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
* 19094:fixes issue in stream lookup join https://github.com/ydb-platform/ydb/issues/19083 [#19094](https://github.com/ydb-platform/ydb/pull/19094) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 19048:fixes crash on double pass away, closes #19044 [#19048](https://github.com/ydb-platform/ydb/pull/19048) ([Alexey Efimov](https://github.com/adameat))
* 18924:Fixes race condition between YardInit and Slay, removing "phantom vdisks" from pdisks [#18924](https://github.com/ydb-platform/ydb/pull/18924) ([Semyon Danilov](https://github.com/SammyVimes))
* 18764:1. Исправлена обработка OlapApply внутри CS для Timestamp
2. Добавлена валидация на негативные timestamp при заливке данных через BulkUpsert

https://github.com/ydb-platform/ydb/issues/18747 [#18764](https://github.com/ydb-platform/ydb/pull/18764) ([Oleg Doronin](https://github.com/dorooleg))

### Performance

* 18765:Pass TStorageConfig through pointer to prevent copying when doing subscription fan-out [#18765](https://github.com/ydb-platform/ydb/pull/18765) ([Alexander Rutkovsky](https://github.com/alexvru))

