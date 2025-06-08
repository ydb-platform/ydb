## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 18352:Added database audit logs in console's tablet.[#18352](https://github.com/ydb-platform/ydb/pull/18352) ([flown4qqqq](https://github.com/flown4qqqq))
* 18298:Limited the creation of ReassignerActor to only one active instance to prevent [SelfHeal](https://ydb.tech/docs/ru/maintenance/manual/selfheal) from overloading BSC. [#18298](https://github.com/ydb-platform/ydb/pull/18298) ([Sergey Belyakov](https://github.com/serbel324))
* 18294:Changed version format from Year.Major.Minor.Hotfix to Year.Major.Minor.Patch.Hotfix [#18294](https://github.com/ydb-platform/ydb/pull/18294) ([Sergey Belyakov](https://github.com/serbel324))
* 19325:Enable kafka port in local-ydb docker container [#19325](https://github.com/ydb-platform/ydb/pull/19325) ([Timofey Koolin](https://github.com/rekby))
* 19310:Add ability to enable followers (read replicas) for covered secondary indexes [#19310](https://github.com/ydb-platform/ydb/pull/19310) ([azevaykin](https://github.com/azevaykin))
* 18371:merge to [stable-25-1](https://github.com/ydb-platform/ydb/tree/stable-25-1) [#18371](https://github.com/ydb-platform/ydb/pull/18371) ([Andrei Rykov](https://github.com/StekPerepolnen))

### Bug fixes

* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18362:Table auto partitioning: Fixed crash when selecting split key from access samples containing a mix of full key and key prefix operations (e.g. exact/range reads). [#18362](https://github.com/ydb-platform/ydb/pull/18362) ([ijon](https://github.com/ijon))
* 18301:Optimized memory usage in transactions with a large number of participants by changing the storage and resending mechanism for TEvReadSet messages. [#18302](https://github.com/ydb-platform/ydb/pull/18301) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18296:Fixed replication continuing to consume disk space when storage was low, which caused VDisks to become read-only. [#18296](https://github.com/ydb-platform/ydb/pull/18296) ([Sergey Belyakov](https://github.com/serbel324))
* 18271:Fix replication bug #10650 [#18271](https://github.com/ydb-platform/ydb/pull/18271) ([Alexander Rutkovsky](https://github.com/alexvru))
* 18231:Fix segfault that could happen while retrying Whiteboard requests. [#18231](https://github.com/ydb-platform/ydb/pull/18231) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 19402:Make legacy signatures for Digest UDF strict again (follows up f97455e). [#19402](https://github.com/ydb-platform/ydb/pull/19402) ([Igor Munkin](https://github.com/igormunkin))
* 19350:Drop excess langver argument from `TFunctionTypeInfoBuilder` ctor in mkql_udf_ut.cpp. [#19350](https://github.com/ydb-platform/ydb/pull/19350) ([Igor Munkin](https://github.com/igormunkin))
* 19114:fixes issue in stream lookup join https://github.com/ydb-platform/ydb/issues/19083 [#19114](https://github.com/ydb-platform/ydb/pull/19114) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18954:cherry-pick c6a21d1f, 896fcc7f, 2dcd562c, 42258a1 and fcc18ade. Relates to #18487.

... [#18954](https://github.com/ydb-platform/ydb/pull/18954) ([Igor Munkin](https://github.com/igormunkin))
* 18920:Temporarily tweak `DeclareSignature` for `Datetime::Format` and several typeaware UDFs for the incremental upgrade. [#18920](https://github.com/ydb-platform/ydb/pull/18920) ([Igor Munkin](https://github.com/igormunkin))
* 18873:Temporarily tweak `DeclareSignature` for `DigestFunctionUdf` class for the incremental upgrade. [#18873](https://github.com/ydb-platform/ydb/pull/18873) ([Igor Munkin](https://github.com/igormunkin))
* 17839:fixes issue where not all tablets are shown for pers queue group on the tablets tab in diagnostics #15230
fixes issue with empty nodes groups for disconnected nodes #14992
fixes issue when tables storage is 0 #14180
fixes issue with nested databases are childless when navigating from domain #15256
fixes issue with unstable version numbers in /viewer/nodes handler #14827
fixes issue with long running queries are terminated because of inactivity on tcp socket #15866
adds diagnostics for long running queries https://github.com/ydb-platform/ydb-embedded-ui/issues/2017
adds statistics for long running queries #15884
fixes issue with not all follower tablets are shown on tablets tab #15988
fixes issue with long timings on BSC requests in a large cluster #15863
fixes issue with calculating load average on K8S nodes #15522
improves tracing for describe handler #16766
fixes issue with long time loading databases page on certain databases due to timeout on graph rendering #16895
fixes issue with no tablets shown for table index on tablets tab #17103
fixes issue with Optional<Struct> columns are always shown as NULLs #17226
fixes potential security issue with CORS headers #17670 [#17839](https://github.com/ydb-platform/ydb/pull/17839) ([Alexey Efimov](https://github.com/adameat))

### Performance

* 19447:Enhancements to shared threads in the actor system.
We stabilized dynamic resizing of thread count in pools.
Implemented instant thread pool upscaling to utilize up to 4 cores under sudden bursts of load (this improvement is particularly noticeable in the IC pool) [#19447](https://github.com/ydb-platform/ydb/pull/19447) ([kruall](https://github.com/kruall))
* 19445:Improved the actor system structures for intensive multithreaded workloads [#19445](https://github.com/ydb-platform/ydb/pull/19445) ([kruall](https://github.com/kruall))

