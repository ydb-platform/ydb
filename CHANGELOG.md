## Unreleased

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig`. [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))

### Bug fixes

* 22982:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/22493) where executing LIMIT OFFSET queries on tables with empty column selections would cause a VERIFY assertion failure and crash the query engine. [#22982](https://github.com/ydb-platform/ydb/pull/22982) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 22897:Fixes for s3 provider (already merged in stable-25-1-3)

* [YQ-4478 added provider name validation in kqp host (](https://github.com/ydb-platform/ydb/commit/4c59f7eacef7b8c120951dcfaefc476f848001e7)https://github.com/ydb-platform/ydb/pull/22141[)](https://github.com/ydb-platform/ydb/commit/4c59f7eacef7b8c120951dcfaefc476f848001e7)
* [YQ-4447 disabled thread pool in s3 by default (](https://github.com/ydb-platform/ydb/commit/fd6815515d355e74e52f0e120b4f5c04618a2f95)https://github.com/ydb-platform/ydb/pull/22160[)](https://github.com/ydb-platform/ydb/commit/fd6815515d355e74e52f0e120b4f5c04618a2f95)
* [YQ-4454 fixed clickhouse udf includes (](https://github.com/ydb-platform/ydb/commit/8f1967d12d5546b2e20eceef3e586841c6f67523)https://github.com/ydb-platform/ydb/pull/21698[)](https://github.com/ydb-platform/ydb/commit/8f1967d12d5546b2e20eceef3e586841c6f67523) [#22897](https://github.com/ydb-platform/ydb/pull/22897) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 22678:Fixed [false-positive unresponsive tablet issues](https://github.com/ydb-platform/ydb/issues/22390) in healthcheck during restarts. [#22678](https://github.com/ydb-platform/ydb/pull/22678) ([vporyadke](https://github.com/vporyadke))

