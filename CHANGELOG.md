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
* 21007:Added support for enable default allocator for Arrow in config. (https://github.com/ydb-platform/ydb/pull/21007) ([Ivan](https://github.com/abyss7))
* 20753:https://github.com/ydb-platform/ydb/issues/20767 
Return CORS headers on 403, mon. Pages show Network Error without CORS; we need CORS so the client can interpret the received response [#20753](https://github.com/ydb-platform/ydb/pull/20753) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 20738:Added stats for grouped limiter and caches. [#20738](https://github.com/ydb-platform/ydb/pull/20738) ([Vladilen](https://github.com/Vladilen))
* 20525:YMQ: Do not send x-amz-crc32 HTTP header (AWS does not do it) [#20525](https://github.com/ydb-platform/ydb/pull/20525) ([qyryq](https://github.com/qyryq))

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
* 20929:fixes data doubling on memory reallocation when processing incoming chunk
closes #20890 [#20929](https://github.com/ydb-platform/ydb/pull/20929) ([Alexey Efimov](https://github.com/adameat))
* 20916:[Ticket](https://github.com/ydb-platform/ydb/issues/20833)
... [#20916](https://github.com/ydb-platform/ydb/pull/20916) ([Evgenik2](https://github.com/Evgenik2))
* 20863:fixes crash when request was made with URL longer than 2048 bytes and DEBUG level for logging HTTP is active
closes #20859 [#20863](https://github.com/ydb-platform/ydb/pull/20863) ([Alexey Efimov](https://github.com/adameat))
* 20785:Fix data race which occured during reconfiguration of TSamplingThrottlingControl, https://github.com/ydb-platform/ydb/issues/20741 [#20785](https://github.com/ydb-platform/ydb/pull/20785) ([Sergey Belyakov](https://github.com/serbel324))
* 20748:Fixed the [KesusQuoterService freeze](https://github.com/ydb-platform/ydb/issues/20747) in case of several unsuccessful attempts to connect to the Kesus tablet. [#20748](https://github.com/ydb-platform/ydb/pull/20748) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 20720:return CORS to bad 403 response https://github.com/ydb-platform/ydb/issues/20682 [#20720](https://github.com/ydb-platform/ydb/pull/20720) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 20681:fix incorrect error reporting https://github.com/ydb-platform/ydb/issues/20682 [#20681](https://github.com/ydb-platform/ydb/pull/20681) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 20670:Fixed external sources ddl errors, also fix for: https://github.com/ydb-platform/ydb/issues/20669 [#20670](https://github.com/ydb-platform/ydb/pull/20670) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))

