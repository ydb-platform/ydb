## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))

### Bug fixes

* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
