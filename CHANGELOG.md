## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 24579:Increased query service default query timeout to 2h. [#24579](https://github.com/ydb-platform/ydb/pull/24579) ([spuchin](https://github.com/spuchin))
* 25981:added support to read/write `Date32`, `Datetime64`, `Timestamp64` and `Decimal(n, m)` from/to external data sources with "ObjectStorage" source type and "parquet" format [#25981](https://github.com/ydb-platform/ydb/pull/25981) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3. [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24306:Unpoison trace id to prevent false msan alert. [#24306](https://github.com/ydb-platform/ydb/pull/24306) ([Alexander Rutkovsky](https://github.com/alexvru))
* 24079:Fixed problem with access denied on /viewer/capabilities handler. Closes #24013. [#24079](https://github.com/ydb-platform/ydb/pull/24079) ([Alexey Efimov](https://github.com/adameat))
* 24725:https://github.com/ydb-platform/ydb/issues/24701 [#24725](https://github.com/ydb-platform/ydb/pull/24725) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 24668:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/23895) that scalar and block hash shuffles may be incompatible and it causes incorrect results, for example, in hash joins. Now no 2 different kinds of shuffles (SCALAR and BLOCK) should be inputs to the single stage. (#24033) [#24666](https://github.com/ydb-platform/ydb/pull/24666) (#24033) [#24668](https://github.com/ydb-platform/ydb/pull/24668) ([Ivan](https://github.com/abyss7))
* 24633:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/23731) when `IF` predicate pushdown into column shards by expanding constant folding and getting rid of the `IF`. [#24633](https://github.com/ydb-platform/ydb/pull/24633) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 26285:Исправлен код ответа для чтения из партиции по кафка протоколу (fetch). Раньше в некоторых случаях могли отвечать UNKNOWN_SERVER_ERROR, в то время как обработка этих партиций завершилась успешно. [#26285](https://github.com/ydb-platform/ydb/pull/26285) ([Nikolay Shestakov](https://github.com/nshestakov))
* 26258:Fixes missing data shard stats event handling that may lead to not splitting by size data shards [#26258](https://github.com/ydb-platform/ydb/pull/26258) ([kungurtsev](https://github.com/kunga))
* 26198:Fix blob deserialization to support empty payload [#26198](https://github.com/ydb-platform/ydb/pull/26198) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 26147:Removed wrong ensure at preparing batches for writing to ColumnShard (Fixed #25869). Excluded .tmp dir from backup/replication. Renamed temporary directory to random uuid. [#26147](https://github.com/ydb-platform/ydb/pull/26147) ([Nikita Vasilev](https://github.com/nikvas0))

