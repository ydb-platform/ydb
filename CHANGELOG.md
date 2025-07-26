## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 17691:New UI handler for fetching data from topics. [#17691](https://github.com/ydb-platform/ydb/pull/17691) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 16688:Added to set [grpc compression](https://github.com/grpc/grpc/blob/master/doc/compression_cookbook.md) at channel level. [#16688](https://github.com/ydb-platform/ydb/pull/16688) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18017:Implemented client balancing of partitions when reading using the Kafka protocol (like Kafka itself). Previously, balancing took place on the server. [#18017](https://github.com/ydb-platform/ydb/pull/18017) ([Nikolay Shestakov](https://github.com/nshestakov))
* 17734:Added automatic cleanup of temporary tables and directories during export to S3. [#17734](https://github.com/ydb-platform/ydb/pull/17734) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 18352:Added database audit logs in console's tablet. [#18352](https://github.com/ydb-platform/ydb/pull/18352) ([flown4qqqq](https://github.com/flown4qqqq))
* 18298:Limited the creation of ReassignerActor to only one active instance to prevent [SelfHeal](https://ydb.tech/docs/ru/maintenance/manual/selfheal) from overloading BSC. [#18298](https://github.com/ydb-platform/ydb/pull/18298) ([Sergey Belyakov](https://github.com/serbel324))
* 18294:Changed version format from Year.Major.Minor.Hotfix to Year.Major.Minor.Patch.Hotfix. [#18294](https://github.com/ydb-platform/ydb/pull/18294) ([Sergey Belyakov](https://github.com/serbel324))
* 18371:merge to [stable-25-1](https://github.com/ydb-platform/ydb/tree/stable-25-1) [#18371](https://github.com/ydb-platform/ydb/pull/18371) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 19310:Added ability to enable followers (read replicas) for covered secondary indexes. [#19310](https://github.com/ydb-platform/ydb/pull/19310) ([azevaykin](https://github.com/azevaykin))
* 19504:Implemented a [vector index](./dev/vector-indexes.md) for approximate vector search. [#19504](https://github.com/ydb-platform/ydb/pull/19504) ([kungurtsev](https://github.com/kunga))
* 20691:YMQ: Do not send x-amz-crc32 HTTP header (AWS does not do it) [#20691](https://github.com/ydb-platform/ydb/pull/20691) ([qyryq](https://github.com/qyryq))
* 21038:This PR enables these frameworks to work with YDB Topics through Kafka API:
- Kafka Connect
- Confluent Schema Registry
- Kafka Streams
- Apache Flink
- AKHQ
- several others smaller ones

Features added by this PR:
- Support for kafka transactions
- Support for compacted topics
- Support for storing metadata during offset commit
- Support for DESCRIBE_CONFIGS, DESCRIBE_GROUPS, LIST_GROUPS requests
- Several critical bug fixes in Kafka API [#21038](https://github.com/ydb-platform/ydb/pull/21038) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 20982:Добавлен новый протокол в Node Broker, устраняющий долгий запуск узлов на больших кластерах https://github.com/ydb-platform/ydb/issues/11064 [#20982](https://github.com/ydb-platform/ydb/pull/20982) ([Ilia Shakhov](https://github.com/pixcc))

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
* 20997:Fix for GROUP BY emitting multiple NULL keys when block processing is enabled [#20997](https://github.com/ydb-platform/ydb/pull/20997) ([Pavel Zuev](https://github.com/pzuev))
* 20633:Fix for Arrow arena when allocating zero-sized region #20634
Original commit: 7b5e0194f5ddeab1c864112b1716b70b969ac7b2 [#20633](https://github.com/ydb-platform/ydb/pull/20633) ([Ivan](https://github.com/abyss7))
* 20242:If the CDC stream was recorded in an auto-partitioned topic, then it could stop after several splits of the topic. In this case, modification of rows in the table would result in the error that the table is overloaded. [#20242](https://github.com/ydb-platform/ydb/pull/20242) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20155:Fixed use after free in CPU scheduler, fixed verify fail in CS CPU limiter: https://github.com/ydb-platform/ydb/issues/20116 [#20155](https://github.com/ydb-platform/ydb/pull/20155) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 20083:Поправил коммит оффсетов сообщений топика при чтении. До фикса пользователь получал ошибку "Unable to navigate:path: 'Root/logbroker-federation/--cut--/stable/guidance' status: PathErrorUnknown\n". Сломали начиная с 25-1-2 [#20083](https://github.com/ydb-platform/ydb/pull/20083) ([Nikolay Shestakov](https://github.com/nshestakov))
* 21355:add gettxtype in txwrite (#21314) [#21355](https://github.com/ydb-platform/ydb/pull/21355) ([r314-git](https://github.com/r314-git))
* 21134:Cherry-pick from main fix for KIKIMR-23489 [#21134](https://github.com/ydb-platform/ydb/pull/21134) ([Denis Khalikov](https://github.com/denis0x0D))

### YDB UI

* 123:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19676) – make nodes less critical (to make cluster less critical). [#20053](https://github.com/ydb-platform/ydb/pull/20053) ([Alexey Efimov](https://github.com/adameat))
* 17839:[Fixed](https://github.com/ydb-platform/ydb/pull/17839) an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/18615) where not all tablets are shown for pers queue group on the tablets tab in diagnostics. #15230 ([Alexey Efimov](https://github.com/adameat))
* 124:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/14992) with empty nodes groups for disconnected nodes.
* 125:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/14180) when tables storage is 0.
* 126:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15256) with nested databases are childless when navigating from domain.
* 127:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/14827) with unstable version numbers in /viewer/nodes handler.
* 128:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15866) with long running queries are terminated because of inactivity on tcp socket.
* 129:Fixed [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15988) with not all follower tablets are shown on tablets tab.
* 130:Fixed [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15863) with long timings on BSC requests in a large cluster.
* 131:Fixed [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15522) with calculating load average on K8S nodes.
* 132:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/16895) with long time loading databases page on certain databases due to timeout on graph rendering.
* 133:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/17103) with no tablets shown for table index on tablets tab.
* 134:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/17226) with Optional<Struct> columns are always shown as NULLs.
* 135:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/17813) crash in /viewer/storage handler.
* 136:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164) keep precision of double values on serialization.
* 137:Fixed long errors on vdisk evict when no pdisks are available.
* 138:Improved vdisk evict swagger and parameters handling.
* 139:Fixed handling of metadata cache requests.
* 140:Fixed list of nodes and databases in broken environment, closes https://github.com/ydb-platform/ydb/issues/16477
* 141:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/18735) storage nodes, some other minor fixes.
* 142:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19810) - minimized returned data to avoid large responses.
* 143:Don't report fake limit as total node memory.
* 144:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19676) – make nodes less critical (to make cluster less critical). [#20053](https://github.com/ydb-platform/ydb/pull/20053) ([Alexey Efimov](https://github.com/adameat))
* 145:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/17226) with Optional<Struct> columns are always shown as NULLs.

### Performance

* 17712:Introduced Intersect, Add and IsEmpty operations to Roaring UDF. [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Added naive bulk And to Roaring UDF. [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant)
* 19447:Enhancements to shared threads in the actor system. We stabilized dynamic resizing of thread count in pools. Implemented instant thread pool upscaling to utilize up to 4 cores under sudden bursts of load (this improvement is particularly noticeable in the IC pool) [#19447](https://github.com/ydb-platform/ydb/pull/19447) ([kruall](https://github.com/kruall))
* 19445:Improved the actor system structures for intensive multithreaded workloads. [#19445](https://github.com/ydb-platform/ydb/pull/19445) ([kruall](https://github.com/kruall))
* 19910:Enhanced pool scaling when using shared threads and available CPU resources. [#19910](https://github.com/ydb-platform/ydb/pull/19910) ([kruall](https://github.com/kruall))
* 20197:Added early termination optimization for `GraceJoin`: if one side is empty and the join kind guarantees an empty result, the other side is no longer read. [#20197](https://github.com/ydb-platform/ydb/pull/20197) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 19926:Significantly improved performance for single-core, dual-core, and triple-core configurations [#19926](https://github.com/ydb-platform/ydb/pull/19926) ([kruall](https://github.com/kruall))

