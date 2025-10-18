## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 24579:Increased query service default query timeout to 2h. [#24579](https://github.com/ydb-platform/ydb/pull/24579) ([spuchin](https://github.com/spuchin))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3. [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24306:Unpoison trace id to prevent false msan alert. [#24306](https://github.com/ydb-platform/ydb/pull/24306) ([Alexander Rutkovsky](https://github.com/alexvru))
* 24079:Fixed problem with access denied on /viewer/capabilities handler. Closes #24013. [#24079](https://github.com/ydb-platform/ydb/pull/24079) ([Alexey Efimov](https://github.com/adameat))
* 24725:https://github.com/ydb-platform/ydb/issues/24701 [#24725](https://github.com/ydb-platform/ydb/pull/24725) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 24668:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/23895) that scalar and block hash shuffles may be incompatible and it causes incorrect results, for example, in hash joins. Now no 2 different kinds of shuffles (SCALAR and BLOCK) should be inputs to the single stage. (#24033) [#24666](https://github.com/ydb-platform/ydb/pull/24666) (#24033) [#24668](https://github.com/ydb-platform/ydb/pull/24668) ([Ivan](https://github.com/abyss7))
* 24633:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/23731) when `IF` predicate pushdown into column shards by expanding constant folding and getting rid of the `IF`. [#24633](https://github.com/ydb-platform/ydb/pull/24633) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 26659:Fixed upsert to table with unique index (Fixed https://github.com/ydb-platform/ydb/issues/23122) [#26659](https://github.com/ydb-platform/ydb/pull/26659) ([Nikita Vasilev](https://github.com/nikvas0))
* 26506:Исправлено возможное падение из-за обращения к уже освобожденной памяти [#26506](https://github.com/ydb-platform/ydb/pull/26506) ([Nikolay Shestakov](https://github.com/nshestakov))
* 26443:Fix duplicated replicas selection when bad state storage config (nToSelect = 5, ringsCount = 9) applied
[Issue](https://st.yandex-team.ru/SPI-162924) [#26443](https://github.com/ydb-platform/ydb/pull/26443) ([Evgenik2](https://github.com/Evgenik2))

