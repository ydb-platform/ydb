## Unreleased

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23331:Added support for a user-defined Certificate Authority (CA) in asynchronous replication. [#23331](https://github.com/ydb-platform/ydb/pull/23331) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 23027:Added support for [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) in [YDB Topics Kafka API](https://ydb.tech/docs/en/reference/kafka-api/). Enabled via the `enable_topic_compactification_by_key` flag.

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 22982:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/22493) where executing LIMIT OFFSET queries on tables with empty column selections would cause a VERIFY assertion failure and crash the query engine. [#22982](https://github.com/ydb-platform/ydb/pull/22982) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 22897:Fixed for s3 provider:
  * YQ-4478 added provider name validation in kqp host (https://github.com/ydb-platform/ydb/pull/22141)
  * YQ-4447 disabled thread pool in s3 by default (https://github.com/ydb-platform/ydb/pull/22160)
  * YQ-4454 fixed clickhouse udf includes (https://github.com/ydb-platform/ydb/pull/21698)
* 22678:Fixed [false-positive unresponsive tablet issues](https://github.com/ydb-platform/ydb/issues/22390) in healthcheck during restarts. [#22678](https://github.com/ydb-platform/ydb/pull/22678) ([vporyadke](https://github.com/vporyadke))
* 23363:[Fixed](https://github.com/ydb-platform/ydb/pull/23363) an [issue](https://github.com/ydb-platform/ydb/issues/23171) with transaction validation memory limit during execution plan increase memory limit for run execution plan. [#23363] ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 24660:Do not mix scalar and block HashShuffle connections (#24033) [#24660](https://github.com/ydb-platform/ydb/pull/24660) ([Ivan](https://github.com/abyss7))
* 24630:Fixing If predicate pushdown into column shards by expanding constant folding and getting rid of the if
https://github.com/ydb-platform/ydb/issues/23731 [#24630](https://github.com/ydb-platform/ydb/pull/24630) ([Pavel Velikhov](https://github.com/pavelvelikhov))

