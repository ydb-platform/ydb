## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 24579:Increase query service default query timeout to 2h. [#24579](https://github.com/ydb-platform/ydb/pull/24579) ([spuchin](https://github.com/spuchin))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24725:https://github.com/ydb-platform/ydb/issues/24701 [#24725](https://github.com/ydb-platform/ydb/pull/24725) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 24668:Do not mix scalar and block HashShuffle connections (#24033) [#24668](https://github.com/ydb-platform/ydb/pull/24668) ([Ivan](https://github.com/abyss7))
* 24633:Fixing If predicate pushdown into column shards by expanding constant folding and getting rid of the if
https://github.com/ydb-platform/ydb/issues/23731 [#24633](https://github.com/ydb-platform/ydb/pull/24633) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 24393:Wakeup callback is called from multiple threads. So, it shouldn't change the inner state of the tasks runner.
fixes https://github.com/ydb-platform/ydb/issues/24148
... [#24393](https://github.com/ydb-platform/ydb/pull/24393) ([Filitov Mikhail](https://github.com/lll-phill-lll))

