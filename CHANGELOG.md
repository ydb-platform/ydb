## Unreleased

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig`. [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3. [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24306:Unpoison trace id to prevent false msan alert. [#24306](https://github.com/ydb-platform/ydb/pull/24306) ([Alexander Rutkovsky](https://github.com/alexvru))
* 24079:Fixed problem with access denied on /viewer/capabilities handler. Closes #24013. [#24079](https://github.com/ydb-platform/ydb/pull/24079) ([Alexey Efimov](https://github.com/adameat))

