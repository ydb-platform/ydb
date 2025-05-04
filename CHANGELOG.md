## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 17691:New UI handler for fetching data from topics [#17691](https://github.com/ydb-platform/ydb/pull/17691) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 16688:related to https://github.com/ydb-platform/ydb/issues/16328 [#16688](https://github.com/ydb-platform/ydb/pull/16688) ([Vitalii Gridnev](https://github.com/gridnevvvit))

### Bug fixes

* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 17853:Changes from #17842

The transaction has entered the EXECUTED state, but has not yet saved it to disk. If the tablet receives a TEvReadSet, it will send a TEvReadSetAck in response. TEvReadSetAck and it will delete the transaction. If the tablet restarts at this point, the transaction will remain in the WAIT_RS state.

The tablet should send TEvReadSetAck only after it saves the transaction status to disk.

Issue #17843 [#17853](https://github.com/ydb-platform/ydb/pull/17853) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

### Performance

* 17756:Limit internal inflight config updates. [#17756](https://github.com/ydb-platform/ydb/pull/17756) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))

