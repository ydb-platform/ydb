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
* 21119:Added the ability to use familiar streaming processing tools – Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKHQ and several others smaller ones via the Kafka API when working with YDB Topics. Added support for kafka transactions, compacted topics, storing metadata during offset commit, DESCRIBE_CONFIGS, DESCRIBE_GROUPS, LIST_GROUPS requests. [#21119](https://github.com/ydb-platform/ydb/pull/21119) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* * 20982:Added [new protocol to Node Broker](https://github.com/ydb-platform/ydb/issues/11064), eliminating the long startup of nodes on large clusters.  [#20982](https://github.com/ydb-platform/ydb/pull/20982) ([Ilia Shakhov](https://github.com/pixcc))

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
* 21356:add gettxtype in txwrite (#21314) [#21356](https://github.com/ydb-platform/ydb/pull/21356) ([r314-git](https://github.com/r314-git))

### YDB UI

* None:Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/17226) with Optional<Struct> columns are always shown as NULLs.
* 17839:[Fixed](https://github.com/ydb-platform/ydb/pull/17839) an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/18615) where not all tablets are shown for pers queue group on the tablets tab in diagnostics. #15230 ([Alexey Efimov](https://github.com/adameat))

### Performance

* 17712:Introduced Intersect, Add and IsEmpty operations to Roaring UDF. [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Added naive bulk And to Roaring UDF. [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant)
* 19447:Enhancements to shared threads in the actor system. We stabilized dynamic resizing of thread count in pools. Implemented instant thread pool upscaling to utilize up to 4 cores under sudden bursts of load (this improvement is particularly noticeable in the IC pool) [#19447](https://github.com/ydb-platform/ydb/pull/19447) ([kruall](https://github.com/kruall))
* 19445:Improved the actor system structures for intensive multithreaded workloads. [#19445](https://github.com/ydb-platform/ydb/pull/19445) ([kruall](https://github.com/kruall))
* 19910:Enhanced pool scaling when using shared threads and available CPU resources. [#19910](https://github.com/ydb-platform/ydb/pull/19910) ([kruall](https://github.com/kruall))

