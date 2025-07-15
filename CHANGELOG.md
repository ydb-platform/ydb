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
* 17650:Introduced Add and IsEmpty operations to Roaring UDF. [#17650](https://github.com/ydb-platform/ydb/pull/17650) ([jsjant](https://github.com/jsjant))
* 17537:The language version may affect the computation layer, so it should be passed via dq task. [#17537](https://github.com/ydb-platform/ydb/pull/17537) ([Vitaly Stoyan](https://github.com/vitstn))
* 17360:Added a setting that specifies the initial stream offset by timestamp or duration. [#17360](https://github.com/ydb-platform/ydb/pull/17360) ([Alexey Pozdniakov](https://github.com/APozdniakov))
* 17314:Added progress stats period into execute script request API (periodic statistics updates). Added dynamic updates for query service config [#17314](https://github.com/ydb-platform/ydb/pull/17314) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 16932:Add `MaxFaultyPDisksPerNode` to CMS config, so that Sentinel could not exceed that number. This may be useful in case if a node with a large count of disks goes down but it is known that this node will be brought back in the near future. This way, we will not be moving many disks at once (which can be rather long in case of HDDs). If `MaxFaultyPDisksPerNode` is 0, then there is no limit. [#16932](https://github.com/ydb-platform/ydb/pull/16932) ([Semyon Danilov](https://github.com/SammyVimes))
* 17911:Add the list of dependent scheme objects (external tables, column tables) to the description of an external data source. [#17911](https://github.com/ydb-platform/ydb/pull/17911) ([Daniil Demin](https://github.com/jepett0))
* 17711:Add arguments `--breakpad-minidumps-path` and `--breakpad-minidumps-script` to `ydbd server` command for initialize minidumps instead environment. [#17711](https://github.com/ydb-platform/ydb/pull/17711) ([Олег](https://github.com/iddqdex))
* 17695:YDB FQ: pushdown `LIKE` expression [#17695](https://github.com/ydb-platform/ydb/pull/17695) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 17420:Added data erasure request for PQ tablets [#17420](https://github.com/ydb-platform/ydb/pull/17420) ([Andrey Molotkov](https://github.com/molotkov-and))
* 17920:Added new PDisk status to BSC, which allows to reassign VDisks from PDisk via SelfHeal without considering VDisk erroneous. Intended to be used instead of FAULTY status when node is taken to long term maintenance. [#17920](https://github.com/ydb-platform/ydb/pull/17920) ([Sergey Belyakov](https://github.com/serbel324))
* 17864:Added file name and line number to logs. [#17864](https://github.com/ydb-platform/ydb/pull/17864) ([Vladislav Stepanyuk](https://github.com/vladstepanyuk))
* 17687:To avoid ambiguity, we switched to using the PDisk State from BSC info to determine the PDisk status instead of the PDisk status in CMS. It is already used in the healthcheck fallback logic and reflects the actual disk condition. We plan to make it the primary source for PDisk state in healthcheck. [#17687](https://github.com/ydb-platform/ydb/pull/17687) ([Andrei Rykov](https://github.com/StekPerepolnen))

### Bug fixes

* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16021:Fixed a rare error that led to a VERIFY error when replicating data. #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16764:Fixed redirects from cluster endpoints (storage nodes) to database nodes, resolving inconsistent behavior where some system tables were not visible. #16763 [#16764](https://github.com/ydb-platform/ydb/pull/16764) ([Alexey Efimov](https://github.com/adameat))
* 17157:Viewer API: Fixed the retrieval of tablet list for tables implementing secondary indexes. #17103 [#17157](https://github.com/ydb-platform/ydb/pull/17157) ([Alexey Efimov](https://github.com/adameat))
* 17606:healthcheck: fix check for unknown StatusV2 in VDisks (https://github.com/ydb-platform/ydb/issues/17619) А forward compatibility issue fix [#17606](https://github.com/ydb-platform/ydb/pull/17606) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17556:Fixed EntityTooSmall error in S3 export multipart upload https://github.com/ydb-platform/ydb/issues/16873 [#17556](https://github.com/ydb-platform/ydb/pull/17556) ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 17468:Fixed output of optional structs, closes #17226 [#17468](https://github.com/ydb-platform/ydb/pull/17468) ([Alexey Efimov](https://github.com/adameat))
* 17429:https://github.com/ydb-platform/ydb/issues/16176 [#17429](https://github.com/ydb-platform/ydb/pull/17429) ([Nikolay Shestakov](https://github.com/nshestakov))
* 17418:Previously replication didn't handle StatusFlags from PDisk properly and didn't stop when available space was running low. Because of this, space was consumed until VDisk and corresponding storage group became read-only. With this change, replication will postpone on YELLOW_STOP color flag and resume after a certain delay if enough space is available. [#17418](https://github.com/ydb-platform/ydb/pull/17418) ([Sergey Belyakov](https://github.com/serbel324))
* 16208:YQ-4205 fixed query service config passing into SS [#16208](https://github.com/ydb-platform/ydb/pull/16208) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 17872:Missed #ifdef has been added. MSG_ZEROCOPY related code works only on linux. [#17872](https://github.com/ydb-platform/ydb/pull/17872) ([Daniil Cherednik](https://github.com/dcherednik))
* 17826:TGuardActor should be launched only in case of uncompleted ZC transfers. [#17826](https://github.com/ydb-platform/ydb/pull/17826) ([Daniil Cherednik](https://github.com/dcherednik))
* 17780:Make PDisk handle `TEvYardControl::PDiskStop` in Error and Init states [#17780](https://github.com/ydb-platform/ydb/pull/17780) ([Semyon Danilov](https://github.com/SammyVimes))
* 17743:Filters are more comprehensively pushed into column shards now, mostly using scalar kernels. Once we have all the scalar kernels working, we'll start adding block kernels for special cases [#17743](https://github.com/ydb-platform/ydb/pull/17743) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 18121:`ALTER TABLE table ` should not fail for a table with vector index. [#18121](https://github.com/ydb-platform/ydb/pull/18121) ([azevaykin](https://github.com/azevaykin))
* 18088:Don't allow observing inconsistent results in some read-write transactions. Fixes #18064. [#18088](https://github.com/ydb-platform/ydb/pull/18088) ([Aleksei Borzenkov](https://github.com/snaury))
* 18059:Fixed crashes by some SELECT queries with DESC in OLAP tables [#18059](https://github.com/ydb-platform/ydb/pull/18059) ([Semyon](https://github.com/swalrus1))
* 17982:`ALTER TABLE table RENAME INDEX` should not fail for a vector index. [#17982](https://github.com/ydb-platform/ydb/pull/17982) ([azevaykin](https://github.com/azevaykin))
* 17814:Fixed BuildIndex/Export/Import operation listing order (#17817) [#17814](https://github.com/ydb-platform/ydb/pull/17814) ([Vitaliy Filippov](https://github.com/vitalif))
* 17729:Fixed schema version collisions in serverless DBs. #17184 [#17729](https://github.com/ydb-platform/ydb/pull/17729) ([Semyon](https://github.com/swalrus1))

### YDB UI

* 17942:viewer: change auth for `whoami` and `capabilities` handlers [#17942](https://github.com/ydb-platform/ydb/pull/17942) ([Andrei Rykov](https://github.com/StekPerepolnen))

### Performance

* 17611:Introduce Intersect boolean operation to Roaring UDF. [#17611](https://github.com/ydb-platform/ydb/pull/17611) ([jsjant](https://github.com/jsjant))
* 17533:Constant folding now also handles deterministic UDFs. Non-deterministic UDFs are detected via a black list [#17533](https://github.com/ydb-platform/ydb/pull/17533) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 17794:Allow multi broadcast in table service by default. [#17794](https://github.com/ydb-platform/ydb/pull/17794) ([Олег](https://github.com/iddqdex))
* 17884:A lot more types of filters are currently getting pushed into column shards, improving performance [#17884](https://github.com/ydb-platform/ydb/pull/17884) ([Pavel Velikhov](https://github.com/pavelvelikhov))
