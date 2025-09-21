## Unreleased

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23332:Added support for a user-defined Certificate Authority (CA) in asynchronous replication. [#23332](https://github.com/ydb-platform/ydb/pull/23332) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 23367:[Fixed](https://github.com/ydb-platform/ydb/pull/23367) an [issue](https://github.com/ydb-platform/ydb/issues/23171) with transaction validation memory limit during execution plan increase memory limit for run execution plan. ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 24666:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/23895) that scalar and block hash shuffles may be incompatible and it causes incorrect results, for example, in hash joins. Now no 2 different kinds of shuffles (SCALAR and BLOCK) should be inputs to the single stage. (#24033) [#24666](https://github.com/ydb-platform/ydb/pull/24666) ([Ivan](https://github.com/abyss7))
