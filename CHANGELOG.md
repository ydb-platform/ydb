## Unreleased

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23331:Added support for a user-defined Certificate Authority (CA) in asynchronous replication. [#23331](https://github.com/ydb-platform/ydb/pull/23331) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 23027:Added support for [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) in [YDB Topics Kafka API](https://ydb.tech/docs/en/reference/kafka-api/). Enabled via the `enable_topic_compactification_by_key` flag.

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 23363:[Fixed](https://github.com/ydb-platform/ydb/pull/23363) an [issue](https://github.com/ydb-platform/ydb/issues/23171) with transaction validation memory limit during execution plan increase memory limit for run execution plan. [#23363] ([Vitalii Gridnev](https://github.com/gridnevvvit))
