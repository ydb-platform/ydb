## Unreleased

### Functionality

* 21119:Added the ability to use familiar streaming processing tools – Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKHQ and several others smaller ones via the Kafka API when working with YDB Topics. Added support for kafka transactions, compacted topics, storing metadata during offset commit, DESCRIBE_CONFIGS, DESCRIBE_GROUPS, LIST_GROUPS requests. [#21119](https://github.com/ydb-platform/ydb/pull/21119) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 20982:Added [new protocol to Node Broker](https://github.com/ydb-platform/ydb/issues/11064), eliminating the long startup of nodes on large clusters.  [#20982](https://github.com/ydb-platform/ydb/pull/20982) ([Ilia Shakhov](https://github.com/pixcc))

### Bug fixes

* 18647:[Fixed](https://github.com/ydb-platform/ydb/pull/18647) an [issue](https://github.com/ydb-platform/ydb/issues/17885) where the index type was incorrectly defaulting to GLOBAL SYNC when UNIQUE was explicitly specified in the query. ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 18086:Bug fixes for direct read in topics [#18086](https://github.com/ydb-platform/ydb/pull/18086) ([qyryq](https://github.com/qyryq))
* 16797:Fixed an issue with topic auto-partitioning when the `max_active_partition` configuration parameter was set via the `ALTER TOPIC` statement. [#16797](https://github.com/ydb-platform/ydb/pull/16797) ([Nikolay Shestakov](https://github.com/nshestakov))
* 18296:Fixed replication continuing to consume disk space when storage was low, which caused VDisks to become read-only. [#18296](https://github.com/ydb-platform/ydb/pull/18296) ([Sergey Belyakov](https://github.com/serbel324))
* 18231:Fix segfault that could happen while retrying Whiteboard requests. [#18231](https://github.com/ydb-platform/ydb/pull/18231) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 19402:Make legacy signatures for Digest UDF strict again (follows up f97455e). [#19402](https://github.com/ydb-platform/ydb/pull/19402) ([Igor Munkin](https://github.com/igormunkin))
* 19350:Drop excess langver argument from `TFunctionTypeInfoBuilder` ctor in mkql_udf_ut.cpp. [#19350](https://github.com/ydb-platform/ydb/pull/19350) ([Igor Munkin](https://github.com/igormunkin))
* 19114:fixes issue in stream lookup join https://github.com/ydb-platform/ydb/issues/19083 [#19114](https://github.com/ydb-platform/ydb/pull/19114) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18920:Temporarily tweak `DeclareSignature` for `Datetime::Format` and several typeaware UDFs for the incremental upgrade. [#18920](https://github.com/ydb-platform/ydb/pull/18920) ([Igor Munkin](https://github.com/igormunkin))
* 18873:Temporarily tweak `DeclareSignature` for `DigestFunctionUdf` class for the incremental upgrade. [#18873](https://github.com/ydb-platform/ydb/pull/18873) ([Igor Munkin](https://github.com/igormunkin))
* 18938:In the table description columns are returned in the same order as they were specified in CREATE TABLE. [#18938](https://github.com/ydb-platform/ydb/pull/18938) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 18794:[Fixed](https://github.com/db-platform/adb/pull/18794) a rare [bug](https://github.com/ydb-platform/ydb/issues/18615) with PQ tablet restarts. [#18794](https://github.com/ydb-platform/ydb/pull/18794) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 19522:Add setting to configure drain timeout before node shutdown. #19323 [#19522](https://github.com/ydb-platform/ydb/pull/19522) ([Aleksei Kobzev](https://github.com/kobzonega))
* 19917:When transaction duration exceeds the topic's message retention period, writing to the topic may result in inconsistent data in the partition. [#19917](https://github.com/ydb-platform/ydb/pull/19917) ([Nikolay Shestakov](https://github.com/nshestakov))
* 21449:Cherry-pick from main https://github.com/ydb-platform/ydb/issues/21156 [#21449](https://github.com/ydb-platform/ydb/pull/21449) ([Denis Khalikov](https://github.com/denis0x0D))
* 21273:https://github.com/ydb-platform/ydb/issues/21239 [#21273](https://github.com/ydb-platform/ydb/pull/21273) ([kruall](https://github.com/kruall))
* 20083:Поправил коммит оффсетов сообщений топика при чтении. До фикса пользователь получал ошибку "Unable to navigate:path: 'Root/logbroker-federation/--cut--/stable/guidance' status: PathErrorUnknown\n". Сломали начиная с 25-1-2 [#20083](https://github.com/ydb-platform/ydb/pull/20083) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20155:Fixed use after free in CPU scheduler, fixed verify fail in CS CPU limiter: https://github.com/ydb-platform/ydb/issues/20116 [#20155](https://github.com/ydb-platform/ydb/pull/20155) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 20633:Fix for Arrow arena when allocating zero-sized region. [#20633](https://github.com/ydb-platform/ydb/pull/20633) ([Ivan](https://github.com/abyss7))
* 20997:Fix for GROUP BY emitting multiple NULL keys when block processing is enabled. [#20997](https://github.com/ydb-platform/ydb/pull/20997) ([Pavel Zuev](https://github.com/pzuev))
* 21356:Added gettxtype in txwrite. [#21356](https://github.com/ydb-platform/ydb/pull/21356) ([r314-git](https://github.com/r314-git))

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