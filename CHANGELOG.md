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
* 18302:Moved changes from #18289

Issue #18290

The TEvReadSet message can be sent again. When it was first sent, it was serialized and stored as a string. It turned out that the serialized message takes up a lot of memory space. For transactions with a large number of participants, this takes up a lot of space.

I changed it so that a copy of the data is stored to create a TEvReadSet when resending. [#18302](https://github.com/ydb-platform/ydb/pull/18302) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18270:Fix replication bug #10650 [#18270](https://github.com/ydb-platform/ydb/pull/18270) ([Alexander Rutkovsky](https://github.com/alexvru))

