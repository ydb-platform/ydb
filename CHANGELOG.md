## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 25140:IAM authentication support has been added to asynchronous replication. [#25140](https://github.com/ydb-platform/ydb/pull/25140) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 24986:Added the sending metrics from external boot tablets to hive. This new behavior hidden under the flag LockedTabletsSendMetrics that 'false' by default. We need to publish Hive after the HealthChecker and then set LockedTabletsSendMetrics to 'true'. [#24986](https://github.com/ydb-platform/ydb/pull/24986) ([Andrei Galibin](https://github.com/agalibin))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 25211:fixes https://github.com/ydb-platform/ydb/issues/24954
The issue here is the same that was fixed in https://github.com/ydb-platform/ydb/issues/24148
... [#25211](https://github.com/ydb-platform/ydb/pull/25211) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 24823:Don't leave processed checkpoints in the buffer. Fixes #24779. [#24823](https://github.com/ydb-platform/ydb/pull/24823) ([Aleksei Borzenkov](https://github.com/snaury))

