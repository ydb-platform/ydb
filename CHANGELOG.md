## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23917:add health check overload shard hint
closes #13615 [#23917](https://github.com/ydb-platform/ydb/pull/23917) ([Alexey Efimov](https://github.com/adameat))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 24305:Unpoison trace id to prevent false msan alert [#24305](https://github.com/ydb-platform/ydb/pull/24305) ([Alexander Rutkovsky](https://github.com/alexvru))
* 24281:Поправлена ошибка, когда при записи сообщений kafka , использующих формат батча v0 и v1 сохранялось только первое сообщение батча (все остальные игнорировались) [#24281](https://github.com/ydb-platform/ydb/pull/24281) ([Nikolay Shestakov](https://github.com/nshestakov))
* 24224:[stable-25-1-4] VERIFY failed index_info.cpp:177: GetColumnFieldVerified #23561 [#24224](https://github.com/ydb-platform/ydb/pull/24224) ([xyliganSereja](https://github.com/xyliganSereja))
* 24078:solves problem with access denied on /viewer/capabilities handler
closes #24013 [#24078](https://github.com/ydb-platform/ydb/pull/24078) ([Alexey Efimov](https://github.com/adameat))

