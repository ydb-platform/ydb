## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23346:Cherry-pick multiple commits for Distconf group configuration cache [#23346](https://github.com/ydb-platform/ydb/pull/23346) ([Sergey Belyakov](https://github.com/serbel324))
* 23213:Enable CDC support for topic data handler #23127 [#23213](https://github.com/ydb-platform/ydb/pull/23213) ([FloatingCrowbar](https://github.com/FloatingCrowbar))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 23596:Don't abort if the incoming POST body is malformed.
https://github.com/ydb-platform/ydb/issues/23581 [#23596](https://github.com/ydb-platform/ydb/pull/23596) ([ubyte](https://github.com/ubyte))
* 23495:After this PR PotentialMaxThreadCount represent the true maximum number of threads a pool could obtain, including threads that could be re‑allocated from lower‑priority pools that are currently under‑utilised

#23232 [#23495](https://github.com/ydb-platform/ydb/pull/23495) ([kruall](https://github.com/kruall))

