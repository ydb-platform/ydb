## Unreleased

### Functionality

* 23346:Cherry-pick multiple commits for Distconf group configuration cache [#23346](https://github.com/ydb-platform/ydb/pull/23346) ([Sergey Belyakov](https://github.com/serbel324))
* 23027:Added support for [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) in [YDB Topics Kafka API](https://ydb.tech/docs/en/reference/kafka-api/). Enabled via the `enable_topic_compactification_by_key` flag.

### Bug fixes

* 22982:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/22493) where executing LIMIT OFFSET queries on tables with empty column selections would cause a VERIFY assertion failure and crash the query engine. [#22982](https://github.com/ydb-platform/ydb/pull/22982) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 22897:Fixed for s3 provider:
  * YQ-4478 added provider name validation in kqp host (https://github.com/ydb-platform/ydb/pull/22141)
  * YQ-4447 disabled thread pool in s3 by default (https://github.com/ydb-platform/ydb/pull/22160)
  * YQ-4454 fixed clickhouse udf includes (https://github.com/ydb-platform/ydb/pull/21698)
* 22678:Fixed [false-positive unresponsive tablet issues](https://github.com/ydb-platform/ydb/issues/22390) in healthcheck during restarts. [#22678](https://github.com/ydb-platform/ydb/pull/22678) ([vporyadke](https://github.com/vporyadke))