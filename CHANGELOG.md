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
* 18081:Added progress stats to `ydb workload query` with one thread. [#18081](https://github.com/ydb-platform/ydb/pull/18081) ([Nikolay Shumkov](https://github.com/shnikd))
* 17965:Add ability to enable followers (read replicas) for secondary indexes [#17965](https://github.com/ydb-platform/ydb/pull/17965) ([Stanislav](https://github.com/raydzast))
* 17920:Add new PDisk status to BSC, which allows to reassign VDisks from PDisk via SelfHeal without considering VDisk erroneous. Intended to be used instead of FAULTY status when node is taken to long term maintenance. [#17920](https://github.com/ydb-platform/ydb/pull/17920) ([Sergey Belyakov](https://github.com/serbel324))
* 17864:#17863 added file name and line number to logs [#17864](https://github.com/ydb-platform/ydb/pull/17864) ([Vladislav Stepanyuk](https://github.com/vladstepanyuk))
* 17687:To avoid ambiguity, we switched to using the PDisk State from BSC info to determine the PDisk status instead of the PDisk status in CMS. It is already used in the healthcheck fallback logic and reflects the actual disk condition. We plan to make it the primary source for PDisk state in healthcheck. [#17687](https://github.com/ydb-platform/ydb/pull/17687) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 16491:Added hidden option `--progress` to `ydb sql` command with default: "none", that print progress of query execution. [#16491](https://github.com/ydb-platform/ydb/pull/16491) ([Nikolay Shumkov](https://github.com/shnikd))

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
* 18121:`ALTER TABLE table ` should not fail for a table with vector index. [#18121](https://github.com/ydb-platform/ydb/pull/18121) ([azevaykin](https://github.com/azevaykin))
* 18088:Don't allow observing inconsistent results in some read-write transactions. Fixes #18064. [#18088](https://github.com/ydb-platform/ydb/pull/18088) ([Aleksei Borzenkov](https://github.com/snaury))
* 18079:Issue: https://github.com/ydb-platform/ydb/issues/18116

This PR fixes optimisation in `NKikimr::NRawSocket::TBufferedWriter`, that wrote the entire message directly to socket if message was larger than available space in buffer. When we wrote more than 6mb to socket with SSL enabled, it every time returned -11 (WAGAIN). 
As a quick fix, we replace sending of an entire message to socket with cutting this message into 1mb chunks and sending them to socket one by one. [#18079](https://github.com/ydb-platform/ydb/pull/18079) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 18072:Issue #18071

The metric value is reset to zero when the `TEvPQ::TEvPartitionCounters` event arrives.

Added a re-calculation of the values. [#18072](https://github.com/ydb-platform/ydb/pull/18072) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18062:fixing crash in /viewer/storage handler, closes https://github.com/ydb-platform/ydb/issues/17813 [#18062](https://github.com/ydb-platform/ydb/pull/18062) ([Alexey Efimov](https://github.com/adameat))
* 18059:Fix crashes by some SELECT queries with DESC in OLAP tables [#18059](https://github.com/ydb-platform/ydb/pull/18059) ([Semyon](https://github.com/swalrus1))
* 17982:`ALTER TABLE table RENAME INDEX` should not fail for a vector index. [#17982](https://github.com/ydb-platform/ydb/pull/17982) ([azevaykin](https://github.com/azevaykin))
* 17913:Issue #17915

The PQ tablet forgets about the transaction only after it receives a TEvReadSetAck from all participants. Another shard may be deleted before the PQ completes the transaction (for example, due to a split table). As a result, transactions are executed, but remain in the WAIT_RS_ACKS state.

If the PQ tablet sends a TEvReadSet to a tablet that has already been deleted, it receives a TEvClientConnected with the `Dead` flag in response. In this case, we consider that we have received a TEvReadSetAck. [#17913](https://github.com/ydb-platform/ydb/pull/17913) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 17836:Fix segfault that could happen while retrying Whiteboard requests https://github.com/ydb-platform/ydb/issues/18145 [#17836](https://github.com/ydb-platform/ydb/pull/17836) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17830:Fix a bug in writing chunk metadata that makes older versions incompatible with 24-4-analytics. #17791 [#17830](https://github.com/ydb-platform/ydb/pull/17830) ([Semyon](https://github.com/swalrus1))
* 17814:Fix BuildIndex/Export/Import operation listing order (#17817) [#17814](https://github.com/ydb-platform/ydb/pull/17814) ([Vitaliy Filippov](https://github.com/vitalif))
* 17729:Fix schema version collisions in serverless DBs. #17184 [#17729](https://github.com/ydb-platform/ydb/pull/17729) ([Semyon](https://github.com/swalrus1))

### YDB UI

* 17942:viewer: change auth for `whoami` and `capabilities` handlers [#17942](https://github.com/ydb-platform/ydb/pull/17942) ([Andrei Rykov](https://github.com/StekPerepolnen))

### Performance

* 17884:A lot more types of filters are currently getting pushed into column shards, improving performance [#17884](https://github.com/ydb-platform/ydb/pull/17884) ([Pavel Velikhov](https://github.com/pavelvelikhov))

