## Unreleased

### Functionality

* 21474:Add the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 21000:merge https://github.com/ydb-platform/ydb/pull/20753
Return CORS headers on 403, mon. Pages show Network Error without CORS; we need CORS so the client can interpret the received response [#21000](https://github.com/ydb-platform/ydb/pull/21000) ([Andrei Rykov](https://github.com/StekPerepolnen))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3 [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 21915:Support in asynchronous replication new kind of change record — `reset` record (in addition to `update` & `erase` records). [#21915](https://github.com/ydb-platform/ydb/pull/21915) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 21833:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/21814) where a replication instance with an unspecified `COMMIT_INTERVAL` option caused the process to crash. [#21833](https://github.com/ydb-platform/ydb/pull/21833) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 21650:Редко могли случаться verify при чтении по протоколу pqv0 во время балансировки партиции [#21650](https://github.com/ydb-platform/ydb/pull/21650) ([Nikolay Shestakov](https://github.com/nshestakov))

