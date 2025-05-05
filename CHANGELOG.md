## Unreleased

### Bug fixes

* 17122:Fixed a rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 16649:Fixed an issue where distconf would issue a VERIFY error and crash when nodes are removed through the legacy Console configuration management interface. [#16649](https://github.com/ydb-platform/ydb/pull/16649) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16635:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the `RETURNING` clause to work incorrectly with `INSERT/UPSERT` operations. [#16635](https://github.com/ydb-platform/ydb/pull/16635) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16420:Added `Ydb::StatusIds::TIMEOUT` to the list of retryable errors, which improves the ability to build large secondary indexes. [#16420](https://github.com/ydb-platform/ydb/pull/16420) ([azevaykin](https://github.com/azevaykin))
* 16269:Fixed the issue of a hanging `Drop Tablet` operation in the PQ tablet, caused by the tablet receiving duplicate TEvPersQueue::TEvProposeTransaction messages from SS, especially during delays in IC operation. [#16269](https://github.com/ydb-platform/ydb/pull/16269) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 16194:Fixed a verification failure that occurred during VDisk compaction. [#16194](https://github.com/ydb-platform/ydb/pull/16194) ([Alexander Rutkovsky](https://github.com/alexvru))
* 15570:Allow creation of views that use UDFs in their queries. [#15570](https://github.com/ydb-platform/ydb/pull/15570) ([Daniil Demin](https://github.com/jepett0))
* 15515:Fixed a topic reading hang that occurred when at least one partition had no incoming data but was being read by multiple consumers. [#15515](https://github.com/ydb-platform/ydb/pull/15515) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 17629:cherry-pick of 55a54de1a92c64f8fdddfdcf8ac9a85d4177a301

fixes https://github.com/ydb-platform/ydb/issues/16934

description of the original bug: https://nda.ya.ru/t/_8nms0JC7C4StC

fixes https://github.com/ydb-platform/ydb/issues/16934

... [#17629](https://github.com/ydb-platform/ydb/pull/17629) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17583:Copy table should not check feature flags for columns types. If the types in original table are created then they should be allowed in destination table. [#17583](https://github.com/ydb-platform/ydb/pull/17583) ([azevaykin](https://github.com/azevaykin))
* 17521:Moved changes from #17497

The SDK user writes messages to the topic in a transaction. It does not wait for confirmation that the messages have been recorded and calls Commit. The processing of this and the following transactions in the PQ tablet stops.

When processing the `UserActionAndTransactionEvents` queue, it was not taken into account that the `TEvGetWriteInfoError` message was received. As a result, the transaction remained at the head of the queue and blocked the processing of other operations.

Issue #17499 [#17521](https://github.com/ydb-platform/ydb/pull/17521) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

