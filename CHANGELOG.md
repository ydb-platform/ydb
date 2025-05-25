## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))

### Bug fixes

* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18716:The balancing of the read session was broken if storage nodes have the version 24-4 and dynamic node have the version 25-1 [#18716](https://github.com/ydb-platform/ydb/pull/18716) ([Nikolay Shestakov](https://github.com/nshestakov))
* 18647:Fix parsing unique index type. There was an error that, if not specified explicitly (SYNC or ASYNC), index type remained with the default value GLOBAL SYNC, despite that it was explicitly specified as UNIQUE in query.
https://github.com/ydb-platform/ydb/issues/17885 [#18647](https://github.com/ydb-platform/ydb/pull/18647) ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 18624:fix a bug where node memory usage was not tracked https://github.com/ydb-platform/ydb/issues/18576 [#18624](https://github.com/ydb-platform/ydb/pull/18624) ([vporyadke](https://github.com/vporyadke))
* 18620:ensure the process of booting tablets does not get stuck in the presence of tablets that are not allowed to be launched https://github.com/ydb-platform/ydb/issues/16462 [#18620](https://github.com/ydb-platform/ydb/pull/18620) ([vporyadke](https://github.com/vporyadke))
* 18599:Issue: https://github.com/ydb-platform/ydb/issues/18598

Original PR to main: https://github.com/ydb-platform/ydb/pull/18594

Fixes bug with not forwarding auth token in Kafka proxy in absence of old flag. [#18599](https://github.com/ydb-platform/ydb/pull/18599) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 18581:cherry-pick of 21395e193 and c81a8971a. Fix the following issue: https://github.com/ydb-platform/ydb/issues/18487
... [#18581](https://github.com/ydb-platform/ydb/pull/18581) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 18503:Avoid expensive table merge checks when operation inflight limits have already been exceeded. Fixes #18473. [#18503](https://github.com/ydb-platform/ydb/pull/18503) ([Aleksei Borzenkov](https://github.com/snaury))
* 18377:fixes for handling per-dc followers created on older YDB versions https://github.com/ydb-platform/ydb/issues/16000 [#18377](https://github.com/ydb-platform/ydb/pull/18377) ([vporyadke](https://github.com/vporyadke))

