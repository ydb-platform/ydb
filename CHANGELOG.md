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

### Performance

* 17712:Introduced Intersect, Add and IsEmpty operations to Roaring UDF. [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Added naive bulk And to Roaring UDF. [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant)
* 19926:Significantly improved performance for single-core, dual-core, and triple-core configurations [#19926](https://github.com/ydb-platform/ydb/pull/19926) ([kruall](https://github.com/kruall))

