## Unreleased

### Bug fixes

* 21917:Support in asynchronous replication new kind of change record — `reset` record (in addition to `update` & `erase` records). [#21917](https://github.com/ydb-platform/ydb/pull/21917) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 21835:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/21814) where a replication instance with an unspecified `COMMIT_INTERVAL` option caused the process to crash. [#21835](https://github.com/ydb-platform/ydb/pull/21835) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 21651:Fixed rare errors when reading using the pqv0 protocol from a topic during partition balancing. [#21651](https://github.com/ydb-platform/ydb/pull/21651) ([Nikolay Shestakov](https://github.com/nshestakov))

## 25.1.2

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 17691:New UI handler for fetching data from topics. [#17691](https://github.com/ydb-platform/ydb/pull/17691) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 17734:Added automatic cleanup of temporary tables and directories during export to S3. [#17734](https://github.com/ydb-platform/ydb/pull/17734) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 16688:Added to set [grpc compression](https://github.com/grpc/grpc/blob/master/doc/compression_cookbook.md) at channel level. [#16688](https://github.com/ydb-platform/ydb/pull/16688) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18017:Implemented client balancing of partitions when reading using the Kafka protocol (like Kafka itself). Previously, balancing took place on the server. [#18017](https://github.com/ydb-platform/ydb/pull/18017) ([Nikolay Shestakov](https://github.com/nshestakov))
* 18352:Added database audit logs in console's tablet.[#18352](https://github.com/ydb-platform/ydb/pull/18352) ([flown4qqqq](https://github.com/flown4qqqq))
* 18298:Limited the creation of ReassignerActor to only one active instance to prevent [SelfHeal](https://ydb.tech/docs/ru/maintenance/manual/selfheal) from overloading BSC. [#18298](https://github.com/ydb-platform/ydb/pull/18298) ([Sergey Belyakov](https://github.com/serbel324))
* 18294:Changed version format from Year.Major.Minor.Hotfix to Year.Major.Minor.Patch.Hotfix [#18294](https://github.com/ydb-platform/ydb/pull/18294) ([Sergey Belyakov](https://github.com/serbel324))
* 19310:Added ability to enable followers (read replicas) for covered secondary indexes. [#19310](https://github.com/ydb-platform/ydb/pull/19310) ([azevaykin](https://github.com/azevaykin))
* 19504:Implemented a [vector index](./dev/vector-indexes.md) for approximate vector search. [#19504](https://github.com/ydb-platform/ydb/pull/19504) ([kungurtsev](https://github.com/kunga))
* 20705:YMQ: Do not send x-amz-crc32 HTTP header (AWS does not do it) [#20705](https://github.com/ydb-platform/ydb/pull/20705) ([qyryq](https://github.com/qyryq))

### Bug fixes

* 16797:Fixed an issue with topic auto-partitioning when the `max_active_partition` configuration parameter was set via the `ALTER TOPIC` statement. [#16797](https://github.com/ydb-platform/ydb/pull/16797) ([Nikolay Shestakov](https://github.com/nshestakov))
* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18086:Fixed bug for direct read in topics. [#18086](https://github.com/ydb-platform/ydb/pull/18086) ([qyryq](https://github.com/qyryq))
* 18296:Fixed replication continuing to consume disk space when storage was low, which caused VDisks to become read-only. [#18296](https://github.com/ydb-platform/ydb/pull/18296) ([Sergey Belyakov](https://github.com/serbel324))
* 18362:Table auto partitioning: Fixed crash when selecting split key from access samples containing a mix of full key and key prefix operations (e.g. exact/range reads). [#18362](https://github.com/ydb-platform/ydb/pull/18362) ([ijon](https://github.com/ijon))
* 18301:Optimized memory usage in transactions with a large number of participants by changing the storage and resending mechanism for TEvReadSet messages. [#18302](https://github.com/ydb-platform/ydb/pull/18301) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18271:Fixed replication bug #10650 [#18271](https://github.com/ydb-platform/ydb/pull/18271) ([Alexander Rutkovsky](https://github.com/alexvru))
* 18231:Fixed segfault that could happen while retrying Whiteboard requests. [#18231](https://github.com/ydb-platform/ydb/pull/18231) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 18647:[Fixed](https://github.com/ydb-platform/ydb/pull/18647) an [issue](https://github.com/ydb-platform/ydb/issues/17885) where the index type was incorrectly defaulting to GLOBAL SYNC when UNIQUE was explicitly specified in the query. ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 18794:[Fixed](https://github.com/db-platform/adb/pull/18794) a rare [bug](https://github.com/ydb-platform/ydb/issues/18615) with PQ tablet restarts. [#18794](https://github.com/ydb-platform/ydb/pull/18794) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18873:Temporarily tweak `DeclareSignature` for `DigestFunctionUdf` class for the incremental upgrade. [#18873](https://github.com/ydb-platform/ydb/pull/18873) ([Igor Munkin](https://github.com/igormunkin))
* 18920:Temporarily tweak `DeclareSignature` for `Datetime::Format` and several typeaware UDFs for the incremental upgrade. [#18920](https://github.com/ydb-platform/ydb/pull/18920) ([Igor Munkin](https://github.com/igormunkin))
* 18938:In the table description columns are returned in the same order as they were specified in CREATE TABLE. [#18938](https://github.com/ydb-platform/ydb/pull/18938) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 19114:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19083)ю in stream lookup join. [#19114](https://github.com/ydb-platform/ydb/pull/19114) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 19402:Make legacy signatures for Digest UDF strict again. [#19402](https://github.com/ydb-platform/ydb/pull/19402) ([Igor Munkin](https://github.com/igormunkin))
* 19522:Add setting to configure drain timeout before node shutdown. #19323 [#19522](https://github.com/ydb-platform/ydb/pull/19522) ([Aleksei Kobzev](https://github.com/kobzonega))
* 20241:If the CDC stream was recorded in an auto-partitioned topic, then it could stop after several splits of the topic. In this case, modification of rows in the table would result in the error that the table is overloaded. [#20241](https://github.com/ydb-platform/ydb/pull/20241) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20054:Make nodes less critical (to make cluster less critical), closes https://github.com/ydb-platform/ydb/issues/19676 [#20054](https://github.com/ydb-platform/ydb/pull/20054) ([Alexey Efimov](https://github.com/adameat))
* 20025:[Fixed](https://github.com/ydb-platform/ydb/pull/20025) rare freezes of the topic table transaction. ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 20567:Fixed a `SCHEME_ERROR` if request contains unknown partitions. [#20567](https://github.com/ydb-platform/ydb/pull/20567) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 20543:Fixed memory travel when consumer commit offset to the topic with autopartitioning enabled [#20543](https://github.com/ydb-platform/ydb/pull/20543) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20422:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/20420) with timeout during healthcheck. [#20422](https://github.com/ydb-platform/ydb/pull/20422) ([Alexey Efimov](https://github.com/adameat))
* 20395:Fixed reporting of gRPC metrics of serverless databases. [#20395](https://github.com/ydb-platform/ydb/pull/20395) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 20355:Eliminated remove data cleanup freezes in case of counter discrepancies. [#20355](https://github.com/ydb-platform/ydb/pull/20355) ([Andrey Molotkov](https://github.com/molotkov-and))
* 20759:Fixed the [KesusQuoterService freeze](https://github.com/ydb-platform/ydb/issues/20747) in case of several unsuccessful attempts to connect to the Kesus tablet. [#20759](https://github.com/ydb-platform/ydb/pull/20759) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 20707:[Fixed](https://github.com/ydb-platform/ydb/pull/20707) an [issue](https://github.com/ydb-platform/ydb/issues/20709) where distributed reads could occasionally be processed without a snapshot. Need to acquire snapshot for dependant reads even in RW transaction. ([Nikita Vasilev](https://github.com/nikvas0))

### YDB UI

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

### Performance

* 17712:Introduced Intersect, Add and IsEmpty operations to Roaring UDF. [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Added naive bulk And to Roaring UDF. [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant)
* 19445:Improved the actor system structures for intensive multithreaded workloads. [#19445](https://github.com/ydb-platform/ydb/pull/19445) ([kruall](https://github.com/kruall))
* 19447:Enhancements to shared threads in the actor system. We stabilized dynamic resizing of thread count in pools. Implemented instant thread pool upscaling to utilize up to 4 cores under sudden bursts of load (this improvement is particularly noticeable in the IC pool) [#19447](https://github.com/ydb-platform/ydb/pull/19447) ([kruall](https://github.com/kruall))
* 19844:Users will receive faster confirmation that the server has written the message thanks to changes the retry policy settings and adds a cache of SchemeNavigate responses.[#19844](https://github.com/ydb-platform/ydb/pull/19844) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 19916:When transaction duration exceeds the topic's message retention period, writing to the topic may result in inconsistent data in the partition. [#19916](https://github.com/ydb-platform/ydb/pull/19916) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20848:Significantly improved performance for single-core, dual-core, and triple-core configurations. [#20848](https://github.com/ydb-platform/ydb/pull/20848) ([kruall](https://github.com/kruall))
* 20704:Enhanced pool scaling when using shared threads and available CPU resources. [#20704](https://github.com/ydb-platform/ydb/pull/20704) ([kruall](https://github.com/kruall))

