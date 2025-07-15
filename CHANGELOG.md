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

### Bug fixes

* 18647:[Fixed](https://github.com/ydb-platform/ydb/pull/18647) an [issue](https://github.com/ydb-platform/ydb/issues/17885) where the index type was incorrectly defaulting to GLOBAL SYNC when UNIQUE was explicitly specified in the query. ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 18086:Bug fixes for direct read in topics [#18086](https://github.com/ydb-platform/ydb/pull/18086) ([qyryq](https://github.com/qyryq))
* 16797:Fixed an issue with topic auto-partitioning when the `max_active_partition` configuration parameter was set via the `ALTER TOPIC` statement. [#16797](https://github.com/ydb-platform/ydb/pull/16797) ([Nikolay Shestakov](https://github.com/nshestakov))
* 18296:Fixed replication continuing to consume disk space when storage was low, which caused VDisks to become read-only. [#18296](https://github.com/ydb-platform/ydb/pull/18296) ([Sergey Belyakov](https://github.com/serbel324))
* 18938:In the table description columns are returned in the same order as they were specified in CREATE TABLE. [#18938](https://github.com/ydb-platform/ydb/pull/18938) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 18794:[Fixed](https://github.com/db-platform/adb/pull/18794) a rare [bug](https://github.com/ydb-platform/ydb/issues/18615) with PQ tablet restarts. [#18794](https://github.com/ydb-platform/ydb/pull/18794) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 20242:If the CDC stream was recorded in an auto-partitioned topic, then it could stop after several splits of the topic. In this case, modification of rows in the table would result in the error that the table is overloaded. [#20242](https://github.com/ydb-platform/ydb/pull/20242) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20155:Fixed use after free in CPU scheduler, fixed verify fail in CS CPU limiter: https://github.com/ydb-platform/ydb/issues/20116 [#20155](https://github.com/ydb-platform/ydb/pull/20155) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 20083:Поправил коммит оффсетов сообщений топика при чтении. До фикса пользователь получал ошибку "Unable to navigate:path: 'Root/logbroker-federation/--cut--/stable/guidance' status: PathErrorUnknown\n"
Сломали начиная с 25-1-2 [#20083](https://github.com/ydb-platform/ydb/pull/20083) ([Nikolay Shestakov](https://github.com/nshestakov))

### YDB UI

* 20053:Fixing crash in /viewer/storage handler, closes https://github.com/ydb-platform/ydb/issues/17813
Keep precision of double values on serialization, closes https://github.com/ydb-platform/ydb-embedded-ui/issues/2164
Fix long errors on vdisk evict when no pdisks are available
Improve vdisk evict swagger and parameters handling
Fix handling of metadata cache requests
Simplify feature flags calculation
Improve annotations
Fixes list of nodes and databases in broken environment, closes https://github.com/ydb-platform/ydb/issues/16477
Support form-urlencoded in base class
AI adaptation
Fix storage nodes, some other minor fixes, closes https://github.com/ydb-platform/ydb/issues/18735
Minimize returned data to avoid large responses, closes https://github.com/ydb-platform/ydb/issues/19810
Don't report fake limit as total node memory
Make nodes less critical (to make cluster less critical), closes https://github.com/ydb-platform/ydb/issues/19676 [#20053](https://github.com/ydb-platform/ydb/pull/20053) ([Alexey Efimov](https://github.com/adameat))

### Performance

* 17712:Introduced Intersect, Add and IsEmpty operations to Roaring UDF. [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Added naive bulk And to Roaring UDF. [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant)
* 20197:Adds early termination optimization for `GraceJoin`: if one side is empty and the join kind guarantees an empty result, the other side is no longer read.
... [#20197](https://github.com/ydb-platform/ydb/pull/20197) ([Filitov Mikhail](https://github.com/lll-phill-lll))

