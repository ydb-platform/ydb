## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23331:Added support for a user-defined Certificate Authority (CA) in asynchronous replication. [#23331](https://github.com/ydb-platform/ydb/pull/23331) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 23027:Поддержана компактификация топиков для совместимости с кафка. [#23027](https://github.com/ydb-platform/ydb/pull/23027) ([Nikolay Shestakov](https://github.com/nshestakov))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 23363:increase memory limit for run execution plan
fixes https://github.com/ydb-platform/ydb/issues/23171 [#23363](https://github.com/ydb-platform/ydb/pull/23363) ([Vitalii Gridnev](https://github.com/gridnevvvit))

