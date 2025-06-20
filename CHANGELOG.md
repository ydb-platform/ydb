## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 17691:New UI handler for fetching data from topics. [#17691](https://github.com/ydb-platform/ydb/pull/17691) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 16688:Added to set [grpc compression](https://github.com/grpc/grpc/blob/master/doc/compression_cookbook.md) at channel level. [#16688](https://github.com/ydb-platform/ydb/pull/16688) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18017:Implemented client balancing of partitions when reading using the Kafka protocol (like Kafka itself). Previously, balancing took place on the server. [#18017](https://github.com/ydb-platform/ydb/pull/18017) ([Nikolay Shestakov](https://github.com/nshestakov))
* 17734:Added automatic cleanup of temporary tables and directories during export to S3. [#17734](https://github.com/ydb-platform/ydb/pull/17734) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 18352:Added database audit logs in console's tablet.[#18352](https://github.com/ydb-platform/ydb/pull/18352) ([flown4qqqq](https://github.com/flown4qqqq))
* 18298:Limited the creation of ReassignerActor to only one active instance to prevent [SelfHeal](https://ydb.tech/docs/ru/maintenance/manual/selfheal) from overloading BSC. [#18298](https://github.com/ydb-platform/ydb/pull/18298) ([Sergey Belyakov](https://github.com/serbel324))
* 18294:Changed version format from Year.Major.Minor.Hotfix to Year.Major.Minor.Patch.Hotfix [#18294](https://github.com/ydb-platform/ydb/pull/18294) ([Sergey Belyakov](https://github.com/serbel324))
* 19310:Added ability to enable followers (read replicas) for covered secondary indexes. [#19310](https://github.com/ydb-platform/ydb/pull/19310) ([azevaykin](https://github.com/azevaykin))
* 18371:merge to [stable-25-1](https://github.com/ydb-platform/ydb/tree/stable-25-1) [#18371](https://github.com/ydb-platform/ydb/pull/18371) ([Andrei Rykov](https://github.com/StekPerepolnen))

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

### YDB UI
* Added [diagnostics](https://github.com/ydb-platform/ydb-embedded-ui/issues/2017) and [statistics](https://github.com/ydb-platform/ydb-embedded-ui/issues/15884) for long running queries.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/issues/16766) tracing for describe handler.
* 17839:[Fixed](https://github.com/ydb-platform/ydb/pull/17839) an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/18615) where not all tablets are shown for pers queue group on the tablets tab in diagnostics. #15230 ([Alexey Efimov](https://github.com/adameat))
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/14992) with empty nodes groups for disconnected nodes.
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/14180) when tables storage is 0.
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15256) with nested databases are childless when navigating from domain.
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/14827) with unstable version numbers in /viewer/nodes handler.
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15866) with long running queries are terminated because of inactivity on tcp socket.
* Fixed [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15988) with not all follower tablets are shown on tablets tab.
* Fixed [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15863) with long timings on BSC requests in a large cluster.
* Fixed [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/15522) with calculating load average on K8S nodes.
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/16895) with long time loading databases page on certain databases due to timeout on graph rendering.
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/17103) with no tablets shown for table index on tablets tab.
* Fixed an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/17226) with Optional<Struct> columns are always shown as NULLs.

### Performance

* 17712:Introduced Intersect, Add and IsEmpty operations to Roaring UDF. [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Added naive bulk And to Roaring UDF. [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant)
* 19447:Enhancements to shared threads in the actor system. We stabilized dynamic resizing of thread count in pools. Implemented instant thread pool upscaling to utilize up to 4 cores under sudden bursts of load (this improvement is particularly noticeable in the IC pool) [#19447](https://github.com/ydb-platform/ydb/pull/19447) ([kruall](https://github.com/kruall))
* 19445:Improved the actor system structures for intensive multithreaded workloads. [#19445](https://github.com/ydb-platform/ydb/pull/19445) ([kruall](https://github.com/kruall))
