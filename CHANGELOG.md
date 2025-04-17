## Unreleased

### Bug fixes

* 17313:Copy table should not check feature flags for columns types. If the types in original table are created then they should be allowed in destination table. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17120:Moved changed from #17116

Issue #17118

The message `TEvDeletePartition` may arrive earlier than `TEvApproveWriteQuota`. The batch did not send `TEvConsumed` and this blocked the queue of write quota requests. [#17120](https://github.com/ydb-platform/ydb/pull/17120) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

### Functionality

* 17114:Fix audit logs, except one special field 'subject' in console event. This special field 'subject' has some problems which will be resolved in the near future. 

Original PRs in main:

1. https://github.com/ydb-platform/ydb/pull/17012
2. https://github.com/ydb-platform/ydb/pull/16989 [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))

