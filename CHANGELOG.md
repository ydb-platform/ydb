## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24279:Поправлена ошибка, когда при записи сообщений kafka , использующих формат батча v0 и v1 сохранялось только первое сообщение батча (все остальные игнорировались) [#24279](https://github.com/ydb-platform/ydb/pull/24279) ([Nikolay Shestakov](https://github.com/nshestakov))
* 24223:[stable-25-1-4] VERIFY failed index_info.cpp:177: GetColumnFieldVerified #23561 [#24223](https://github.com/ydb-platform/ydb/pull/24223) ([xyliganSereja](https://github.com/xyliganSereja))
* 24076:solves problem with access denied on /viewer/capabilities handler
closes #24013 [#24076](https://github.com/ydb-platform/ydb/pull/24076) ([Alexey Efimov](https://github.com/adameat))

