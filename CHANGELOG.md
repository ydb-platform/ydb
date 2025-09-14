## Unreleased

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23332:Added support for a user-defined Certificate Authority (CA) in asynchronous replication. [#23332](https://github.com/ydb-platform/ydb/pull/23332) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 23367:[Fixed](https://github.com/ydb-platform/ydb/pull/23367) an [issue](https://github.com/ydb-platform/ydb/issues/23171) with transaction validation memory limit during execution plan increase memory limit for run execution plan. ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 24666:Do not mix scalar and block HashShuffle connections (#24033) [#24666](https://github.com/ydb-platform/ydb/pull/24666) ([Ivan](https://github.com/abyss7))
* 24632:Fixing If predicate pushdown into column shards by expanding constant folding and getting rid of the if
https://github.com/ydb-platform/ydb/issues/23731 [#24632](https://github.com/ydb-platform/ydb/pull/24632) ([Pavel Velikhov](https://github.com/pavelvelikhov))

