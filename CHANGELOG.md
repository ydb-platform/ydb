## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))

### Bug fixes

* 18716:Fixed an issue in the read session balancing of the pqv0 protocol that occurred when storage nodes of version 24.4 and dynamic nodes of version 25.1. [#18716](https://github.com/ydb-platform/ydb/pull/18716) ([Nikolay Shestakov](https://github.com/nshestakov))
* 18647:[Fixed](https://github.com/ydb-platform/ydb/pull/18647) an [issue](https://github.com/ydb-platform/ydb/issues/17885) where the index type was incorrectly defaulting to GLOBAL SYNC when UNIQUE was explicitly specified in the query. ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 18624:[Fixed](https://github.com/ydb-platform/ydb/pull/18624) [an issue](https://github.com/ydb-platform/ydb/issues/18576) that node memory usage was not tracked. ([vporyadke](https://github.com/vporyadke))
* 18620:[Fixed](https://github.com/ydb-platform/ydb/pull/18620) a [bug](https://github.com/ydb-platform/ydb/issues/16462) that caused tablet downloads to pause due to tablets that could not allowed to be launched. ([vporyadke](https://github.com/vporyadke))
* 18581:[Fixed](https://github.com/ydb-platform/ydb/pull/18577) typing [errors](https://github.com/ydb-platform/ydb/issues/18487) in UDF. ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 18377:[Fixed](https://github.com/ydb-platform/ydb/pull/18377) [a bug](https://github.com/ydb-platform/ydb/issues/16000) for handling per-dc followers created on older YDB versions. ([vporyadke](https://github.com/vporyadke))
