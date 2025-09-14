## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 24581:Increase query service default query timeout to 2h. [#24581](https://github.com/ydb-platform/ydb/pull/24581) ([spuchin](https://github.com/spuchin))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24724:https://github.com/ydb-platform/ydb/issues/24701 [#24724](https://github.com/ydb-platform/ydb/pull/24724) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 24665:Do not mix scalar and block HashShuffle connections (#24033) [#24665](https://github.com/ydb-platform/ydb/pull/24665) ([Ivan](https://github.com/abyss7))
* 24631:Fixing If predicate pushdown into column shards by expanding constant folding and getting rid of the if
https://github.com/ydb-platform/ydb/issues/23731 [#24631](https://github.com/ydb-platform/ydb/pull/24631) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 24394:Wakeup callback is called from multiple threads. So, it shouldn't change the inner state of the tasks runner.
fixes https://github.com/ydb-platform/ydb/issues/24148
... [#24394](https://github.com/ydb-platform/ydb/pull/24394) ([Filitov Mikhail](https://github.com/lll-phill-lll))

