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
* 18731:Add monitoring counters with constant value =1 for nodes, VDisks and PDisks. [#18731](https://github.com/ydb-platform/ydb/pull/18731) ([Sergey Belyakov](https://github.com/serbel324))
* 18561:Add miss kafka port support

... [#18561](https://github.com/ydb-platform/ydb/pull/18561) ([Sergey J](https://github.com/sourcecd))
* 18376:add a heuristic that should prevent a tablet that can overload a node on its own from endlessly moving between nodes [#18376](https://github.com/ydb-platform/ydb/pull/18376) ([vporyadke](https://github.com/vporyadke))
* 18297:Introduce Bridge tech [#18297](https://github.com/ydb-platform/ydb/pull/18297) ([Alexander Rutkovsky](https://github.com/alexvru))
* 18258:Increase timeout on BS_QUEUE reestablishing session when many actors try to reconnect simultaneously [#18258](https://github.com/ydb-platform/ydb/pull/18258) ([Sergey Belyakov](https://github.com/serbel324))
* 18137:Implement index-only searches with covering vector indexes (#17770) [#18137](https://github.com/ydb-platform/ydb/pull/18137) ([Vitaliy Filippov](https://github.com/vitalif))
* 17061:Add of date range parameters (--date-to, --date-from to support uniform PK distribution) for ydb workload log run operations including bulk_upsert, insert, and upsert [#17061](https://github.com/ydb-platform/ydb/pull/17061) ([Emgariko](https://github.com/Emgariko))

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
* 18752:fix for https://st.yandex-team.ru/YQL-19988 [#18752](https://github.com/ydb-platform/ydb/pull/18752) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))
* 18701:Fixed topic SDK flaky tests: https://github.com/ydb-platform/ydb/issues/17867, https://github.com/ydb-platform/ydb/issues/17985, https://github.com/ydb-platform/ydb/issues/17867 [#18701](https://github.com/ydb-platform/ydb/pull/18701) ([Bulat](https://github.com/Gazizonoki))
* 18698:The issue was missed checks for enabled encryption during zero copy routine. It causes attempt to send data unencrypted via XDC socket.
https://github.com/ydb-platform/ydb/issues/18546 [#18698](https://github.com/ydb-platform/ydb/pull/18698) ([Daniil Cherednik](https://github.com/dcherednik))
* 18664:In the table description columns are returned in the same order as they were specified in CREATE TABLE. [#18664](https://github.com/ydb-platform/ydb/pull/18664) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 18601:fix a bug where node memory usage was not tracked https://github.com/ydb-platform/ydb/issues/18576 [#18601](https://github.com/ydb-platform/ydb/pull/18601) ([vporyadke](https://github.com/vporyadke))
* 18594:Issue: https://github.com/ydb-platform/ydb/issues/18598

Fixes bug with not forwarding auth token in Kafka proxy in absence of old flag. [#18594](https://github.com/ydb-platform/ydb/pull/18594) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 18553:fixes list of nodes and databases in broken environment
closes #16477 [#18553](https://github.com/ydb-platform/ydb/pull/18553) ([Alexey Efimov](https://github.com/adameat))
* 18502:Remove template specialization redundancy error issued in https://st.yandex-team.ru/TAXIARCH-558. [#18502](https://github.com/ydb-platform/ydb/pull/18502) ([Dmitry Raspopov](https://github.com/mordet))
* 18475:Avoid expensive table merge checks when operation inflight limits have already been exceeded. Fixes #18473. [#18475](https://github.com/ydb-platform/ydb/pull/18475) ([Aleksei Borzenkov](https://github.com/snaury))

### Performance

* 18461:Fixed external sources read tasks placement [#18461](https://github.com/ydb-platform/ydb/pull/18461) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))

