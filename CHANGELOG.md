## Unreleased

### Functionality

* 23917:Added health check overload shard hint. [#23917](https://github.com/ydb-platform/ydb/pull/23917) ([Alexey Efimov](https://github.com/adameat))

### Bug fixes

* 25689:Fixes [#25524](https://github.com/ydb-platform/ydb/issues/25524) – issue with importing tables with Utf8 primary keys and changefeeds [#25689](https://github.com/ydb-platform/ydb/pull/25689) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 25622:Fixes segfault in version parser [#25586](https://github.com/ydb-platform/ydb/issues/25586) Removes misleading comments in code [#25622](https://github.com/ydb-platform/ydb/pull/25622) ([Sergey Belyakov](https://github.com/serbel324))
* 25453:Listing of objects with a common prefix has been fixed when importing changefeeds [#25454](https://github.com/ydb-platform/ydb/issues/25454) [#25453](https://github.com/ydb-platform/ydb/pull/25453) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 25148:fix crash after follower alter https://github.com/ydb-platform/ydb/issues/20866 [#25148](https://github.com/ydb-platform/ydb/pull/25148) ([vporyadke](https://github.com/vporyadke))
* 25122:fix a bug where tablet deletion might get stuck [#23858](https://github.com/ydb-platform/ydb/issues/23858) [#25122](https://github.com/ydb-platform/ydb/pull/25122) ([vporyadke](https://github.com/vporyadke))
* 26657:Fixed upsert to table with unique index (Fixed https://github.com/ydb-platform/ydb/issues/23122) [#26657](https://github.com/ydb-platform/ydb/pull/26657) ([Nikita Vasilev](https://github.com/nikvas0))
* 26559:Fix backup of out-of-range decimal values which may have been accidentally inserted into tables. Fixes #26470. [#26559](https://github.com/ydb-platform/ydb/pull/26559) ([Aleksei Borzenkov](https://github.com/snaury))
* 26504:Исправлено возможное падение из-за обращения к уже освобожденной памяти [#26504](https://github.com/ydb-platform/ydb/pull/26504) ([Nikolay Shestakov](https://github.com/nshestakov))
* 26371:Fix duplicated replicas selection when bad state storage config (nToSelect = 5, ringsCount = 9) applied
[Issue](https://st.yandex-team.ru/SPI-162924) [#26371](https://github.com/ydb-platform/ydb/pull/26371) ([Evgenik2](https://github.com/Evgenik2))

