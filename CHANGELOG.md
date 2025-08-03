## Unreleased

### Functionality

* 21119:Added the ability to use familiar streaming processing tools – Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKHQ and several others smaller ones via the Kafka API when working with YDB Topics. Added support for kafka transactions, compacted topics, storing metadata during offset commit, DESCRIBE_CONFIGS, DESCRIBE_GROUPS, LIST_GROUPS requests. [#21119](https://github.com/ydb-platform/ydb/pull/21119) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 20982:Added [new protocol to Node Broker](https://github.com/ydb-platform/ydb/issues/11064), eliminating the long startup of nodes on large clusters.  [#20982](https://github.com/ydb-platform/ydb/pull/20982) ([Ilia Shakhov](https://github.com/pixcc))

### Bug fixes

* 20083:Поправил коммит оффсетов сообщений топика при чтении. До фикса пользователь получал ошибку "Unable to navigate:path: 'Root/logbroker-federation/--cut--/stable/guidance' status: PathErrorUnknown\n". Сломали начиная с 25-1-2 [#20083](https://github.com/ydb-platform/ydb/pull/20083) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20155:Fixed use after free in CPU scheduler, fixed verify fail in CS CPU limiter: https://github.com/ydb-platform/ydb/issues/20116 [#20155](https://github.com/ydb-platform/ydb/pull/20155) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 20633:Fix for Arrow arena when allocating zero-sized region. [#20633](https://github.com/ydb-platform/ydb/pull/20633) ([Ivan](https://github.com/abyss7))
* 20997:Fix for GROUP BY emitting multiple NULL keys when block processing is enabled. [#20997](https://github.com/ydb-platform/ydb/pull/20997) ([Pavel Zuev](https://github.com/pzuev))
* 21356:Added gettxtype in txwrite. [#21356](https://github.com/ydb-platform/ydb/pull/21356) ([r314-git](https://github.com/r314-git))
* 22189:Temporary fix for: https://github.com/ydb-platform/ydb/issues/21207 [#22189](https://github.com/ydb-platform/ydb/pull/22189) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 22127:Fix for SEGV under tsan: https://github.com/ydb-platform/ydb/issues/21240 [#22127](https://github.com/ydb-platform/ydb/pull/22127) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 21918:Support in asynchronous replication new kind of change record — `reset` record (in addition to `update` & `erase` records). [#21918](https://github.com/ydb-platform/ydb/pull/21918) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 21836:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/21814) where a replication instance with an unspecified `COMMIT_INTERVAL` option caused the process to crash. [#21836](https://github.com/ydb-platform/ydb/pull/21836) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 21652:Редко могли случаться verify при чтении по протоколу pqv0 во время балансировки партиции [#21652](https://github.com/ydb-platform/ydb/pull/21652) ([Nikolay Shestakov](https://github.com/nshestakov))

### YDB UI

* 17839:[Fixed](https://github.com/ydb-platform/ydb/pull/17839) an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/18615) where not all tablets are shown for pers queue group on the tablets tab in diagnostics. #15230 ([Alexey Efimov](https://github.com/adameat))
* 135:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/17813) crash in /viewer/storage handler.
* 136:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164) keep precision of double values on serialization.
* 137:Fixed long errors on vdisk evict when no pdisks are available.
* 138:Improved vdisk evict swagger and parameters handling.
* 139:Fixed handling of metadata cache requests.
* 140:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/16477) – list of nodes and databases in broken environment, closes
* 141:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/18735) – storage nodes, some other minor fixes.
* 142:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19810) - minimized returned data to avoid large responses.
* 143:Don't report fake limit as total node memory.
* 144:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19676) – make nodes less critical (to make cluster less critical). [#20053](https://github.com/ydb-platform/ydb/pull/20053) ([Alexey Efimov](https://github.com/adameat))

### Performance

* 19910:Enhanced pool scaling when using shared threads and available CPU resources. [#19910](https://github.com/ydb-platform/ydb/pull/19910) ([kruall](https://github.com/kruall))
* 19926:Significantly improved performance for single-core, dual-core, and triple-core configurations [#19926](https://github.com/ydb-platform/ydb/pull/19926) ([kruall](https://github.com/kruall))
* 20197:Added early termination optimization for `GraceJoin`: if one side is empty and the join kind guarantees an empty result, the other side is no longer read. [#20197](https://github.com/ydb-platform/ydb/pull/20197) ([Filitov Mikhail](https://github.com/lll-phill-lll))

