## Unreleased

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3. [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24278:Fixed an issue where only the first message from a batch was saved when writing Kafka messages, with all other messages in the batch being ignored. [#24278](https://github.com/ydb-platform/ydb/pull/24278) ([Nikolay Shestakov](https://github.com/nshestakov))
* 24077:Fixed problem with access denied on /viewer/capabilities handler. Closes #24013. [#24077](https://github.com/ydb-platform/ydb/pull/24077) ([Alexey Efimov](https://github.com/adameat))

