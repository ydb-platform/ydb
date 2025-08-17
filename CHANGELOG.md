## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 22982:fix limit offset query with empty columns in read
fixes #22493 [#22982](https://github.com/ydb-platform/ydb/pull/22982) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 22928:Issue #21460

Fixed version stable-25-1 so that it can work with blob keys created by asynchronous compaction.

At the start, the partition renames the keys of the new type of blobs and deletes the blobs nested in other blobs. [#22928](https://github.com/ydb-platform/ydb/pull/22928) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 22897:Fixes for s3 provider (already merged in stable-25-1-3)

* [YQ-4478 added provider name validation in kqp host (](https://github.com/ydb-platform/ydb/commit/4c59f7eacef7b8c120951dcfaefc476f848001e7)https://github.com/ydb-platform/ydb/pull/22141[)](https://github.com/ydb-platform/ydb/commit/4c59f7eacef7b8c120951dcfaefc476f848001e7)
* [YQ-4447 disabled thread pool in s3 by default (](https://github.com/ydb-platform/ydb/commit/fd6815515d355e74e52f0e120b4f5c04618a2f95)https://github.com/ydb-platform/ydb/pull/22160[)](https://github.com/ydb-platform/ydb/commit/fd6815515d355e74e52f0e120b4f5c04618a2f95)
* [YQ-4454 fixed clickhouse udf includes (](https://github.com/ydb-platform/ydb/commit/8f1967d12d5546b2e20eceef3e586841c6f67523)https://github.com/ydb-platform/ydb/pull/21698[)](https://github.com/ydb-platform/ydb/commit/8f1967d12d5546b2e20eceef3e586841c6f67523) [#22897](https://github.com/ydb-platform/ydb/pull/22897) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 22804:Incorrect arg position was used

Fixed as part of SOLOMON-16506 [#22804](https://github.com/ydb-platform/ydb/pull/22804) ([jsjant](https://github.com/jsjant))
* 22686:Fixed a parallel modification of the HierarchyData variable in the topic SDK, which could lead to rare crashes of the process. [#22686](https://github.com/ydb-platform/ydb/pull/22686) ([Nikolay Shestakov](https://github.com/nshestakov))
* 22678:fix false-positive unresponsive tablet issues in healthcheck during restarts https://github.com/ydb-platform/ydb/issues/22390 [#22678](https://github.com/ydb-platform/ydb/pull/22678) ([vporyadke](https://github.com/vporyadke))

