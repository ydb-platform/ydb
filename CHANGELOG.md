## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 18352:It is only cherr-pick commit about create/remove database audit logs in console's tablet.

You can read the description from original PR: https://github.com/ydb-platform/ydb/pull/18095 [#18352](https://github.com/ydb-platform/ydb/pull/18352) ([flown4qqqq](https://github.com/flown4qqqq))
* 18298:Don't allow SelfHeal actor to create more than 1 active ReassignerActor. This change will prevent SelfHeal from overloading BSC with ReassignItem requests thus causing DoS. (#17618) [#18298](https://github.com/ydb-platform/ydb/pull/18298) ([Sergey Belyakov](https://github.com/serbel324))
* 18294:Support new version format Year.Major.Minor.Patch.Hotfix (old format is Year.Major.Minor.Hotfix) (#18063) [#18294](https://github.com/ydb-platform/ydb/pull/18294) ([Sergey Belyakov](https://github.com/serbel324))

### Bug fixes

* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18362:Table auto partitioning: Fixed crash when selecting split key from access samples containing a mix of full key and key prefix operations (e.g. exact/range reads). [#18362](https://github.com/ydb-platform/ydb/pull/18362) ([ijon](https://github.com/ijon))
* 18301:Moved changes from #18289

Issue #18290

The TEvReadSet message can be sent again. When it was first sent, it was serialized and stored as a string. It turned out that the serialized message takes up a lot of memory space. For transactions with a large number of participants, this takes up a lot of space.

I changed it so that a copy of the data is stored to create a TEvReadSet when resending. [#18301](https://github.com/ydb-platform/ydb/pull/18301) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18296:Previously replication didn't handle StatusFlags from PDisk properly and didn't stop when available space was running low. Because of this, space was consumed until VDisk and corresponding storage group became read-only. With this change, replication will postpone on YELLOW_STOP color flag and resume after a certain delay if enough space is available. (#17418)

https://github.com/ydb-platform/ydb/issues/13608 [#18296](https://github.com/ydb-platform/ydb/pull/18296) ([Sergey Belyakov](https://github.com/serbel324))
* 18271:Fix replication bug #10650 [#18271](https://github.com/ydb-platform/ydb/pull/18271) ([Alexander Rutkovsky](https://github.com/alexvru))
* 18231:Fix segfault that could happen while retrying Whiteboard requests https://github.com/ydb-platform/ydb/issues/18145
merge https://github.com/ydb-platform/ydb/pull/17836 [#18231](https://github.com/ydb-platform/ydb/pull/18231) ([Andrei Rykov](https://github.com/StekPerepolnen))

