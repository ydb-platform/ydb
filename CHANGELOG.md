## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 17325:add fast bs queue to donor for online read [#17325](https://github.com/ydb-platform/ydb/pull/17325) ([VPolka](https://github.com/VPolka))

### Bug fixes

* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 17627:cherry-pick of 55a54de1a92c64f8fdddfdcf8ac9a85d4177a301

fixes https://github.com/ydb-platform/ydb/issues/16934
description of the original bug: https://nda.ya.ru/t/_8nms0JC7C4StC

... [#17627](https://github.com/ydb-platform/ydb/pull/17627) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17520:Moved changes from #17497

The SDK user writes messages to the topic in a transaction. It does not wait for confirmation that the messages have been recorded and calls Commit. The processing of this and the following transactions in the PQ tablet stops.

When processing the `UserActionAndTransactionEvents` queue, it was not taken into account that the `TEvGetWriteInfoError` message was received. As a result, the transaction remained at the head of the queue and blocked the processing of other operations.

Issue #17499 [#17520](https://github.com/ydb-platform/ydb/pull/17520) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

### Performance

* 17712:Cherry pick latest Roaring UDF changes [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Cherry-pick NaiveBulkAnd into Roaring UDF [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant))

