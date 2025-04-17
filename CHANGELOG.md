## Unreleased

### Functionality

* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
and timeout (by default, the maximum response time from healthcheck). Documentation is under construction. [#15693](https://github.com/ydb-platform/ydb/pull/15693) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17282:https://github.com/ydb-platform/ydb/issues/16468 [#17282](https://github.com/ydb-platform/ydb/pull/17282) ([Олег](https://github.com/iddqdex))
* 17230:* YDB FQ: enhance error occurring during Generic::TPartition parsing [#17230](https://github.com/ydb-platform/ydb/pull/17230) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 17222:https://github.com/ydb-platform/ydb/issues/16944 [#17222](https://github.com/ydb-platform/ydb/pull/17222) ([Олег](https://github.com/iddqdex))
* 17148:YDB FQ: support `Prometheus` as an external data source [#17148](https://github.com/ydb-platform/ydb/pull/17148) ([Stepan](https://github.com/pstpn))
* 17095:lower severity for FAULTY pdiks [#17095](https://github.com/ydb-platform/ydb/pull/17095) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17012:The pull request is aimed at clarifying the reason why the user could not log in. [#17012](https://github.com/ydb-platform/ydb/pull/17012) ([flown4qqqq](https://github.com/flown4qqqq))
* 17007:YQ: Add support of an iceberg data source [#17007](https://github.com/ydb-platform/ydb/pull/17007) ([Slusarenko Igor](https://github.com/buhtr))
* 16994:Create 30 groups of columns instead of 100 under asan to eliminate timeout

... [#16994](https://github.com/ydb-platform/ydb/pull/16994) ([Alexander Avdonkin](https://github.com/aavdonkin))
* 16993:do not propagate disk issues to group level in health check when the disk is operational [#16993](https://github.com/ydb-platform/ydb/pull/16993) ([vporyadke](https://github.com/vporyadke))
* 16989:In schemeshard, we have audit_log_component, which outputs audit logs. There was not details of operation MODIFY(ALTER) USER. So, we need to know what this operation do. There are some ways:

1. Change the password (or hash);
2. Block user;
3. Unblock user.

This PR adds details into audit_log. [#16989](https://github.com/ydb-platform/ydb/pull/16989) ([flown4qqqq](https://github.com/flown4qqqq))
* 16982:Add predicate selectivity with Histogram (#14564)... [#16982](https://github.com/ydb-platform/ydb/pull/16982) ([Denis Khalikov](https://github.com/denis0x0D))
* 16957:* YDB FQ: support Redis as an external data source [#16957](https://github.com/ydb-platform/ydb/pull/16957) ([Gleb Solomennikov](https://github.com/Glebbs))
* 16857:Information about the processing lag of committed messages has been added to DescribeConsumer. [#16857](https://github.com/ydb-platform/ydb/pull/16857) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16834:* YDB FQ: enable pushdown of string types in Generic provider [#16834](https://github.com/ydb-platform/ydb/pull/16834) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 16652:FQ: Add ability to create an external data sources for iceberg tables. [#16652](https://github.com/ydb-platform/ydb/pull/16652) ([Slusarenko Igor](https://github.com/buhtr))
* 16076:Automatic deletion of temporary tables and directories during export [#16076](https://github.com/ydb-platform/ydb/pull/16076) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))

### Bug fixes

* 15721:Fixed a bug in YDB UUID column handling in ReadTable SDK method. [#15721](https://github.com/ydb-platform/ydb/pull/15721) ([Ivan Nikolaev](https://github.com/lex007in))
* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16060:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the **RETURNING** clause  work incorrectly with INSERT/UPSERT operations. [#16060](https://github.com/ydb-platform/ydb/pull/16060) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16021:Fixed a rare error that led to a VERIFY error when replicating data. #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16768:Fixed an issue with topic auto-partitioning when the `max_active_partition` configuration parameter was set via the `ALTER TOPIC` statement. [#16768](https://github.com/ydb-platform/ydb/pull/16768) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16764:Fixed redirects from cluster endpoints (storage nodes) to database nodes, resolving inconsistent behavior where some system tables were not visible. #16763 [#16764](https://github.com/ydb-platform/ydb/pull/16764) ([Alexey Efimov](https://github.com/adameat))
* 17285:https://github.com/ydb-platform/ydb/issues/16984
Нашел неоднозначность в q21 clickbench [#17285](https://github.com/ydb-platform/ydb/pull/17285) ([Олег](https://github.com/iddqdex))
* 17198:Add handling of UUID in YDB CLI backup/restore operations. [#17198](https://github.com/ydb-platform/ydb/pull/17198) ([Semyon Danilov](https://github.com/SammyVimes))
* 17192:Fixed https://github.com/ydb-platform/ydb-cpp-sdk/issues/399 register of GZIP and ZSTD codecs in C++ SDK topic client [#17192](https://github.com/ydb-platform/ydb/pull/17192) ([Bulat](https://github.com/Gazizonoki))
* 17157:fixes filters for tablets on nodes
closes #17103 [#17157](https://github.com/ydb-platform/ydb/pull/17157) ([Alexey Efimov](https://github.com/adameat))
* 17116:Issue #17118

The message `TEvDeletePartition` may arrive earlier than `TEvApproveWriteQuota`. The batch did not send `TEvConsumed` and this blocked the queue of write quota requests. [#17116](https://github.com/ydb-platform/ydb/pull/17116) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 17009:Fixed #16938: error while compiling JOIN with one side been empty constant. [#17009](https://github.com/ydb-platform/ydb/pull/17009) ([Nikita Vasilev](https://github.com/nikvas0))
* 16958:Fixed unauthorized error in `ydb admin database restore` when multiple database admins are in dump. https://github.com/ydb-platform/ydb/issues/16833 [#16958](https://github.com/ydb-platform/ydb/pull/16958) ([Ilia Shakhov](https://github.com/pixcc))
* 16943:Fixed scheme error in `ydb admin cluster dump` when specifying a domain database. https://github.com/ydb-platform/ydb/issues/16262 [#16943](https://github.com/ydb-platform/ydb/pull/16943) ([Ilia Shakhov](https://github.com/pixcc))
* 16901:RateLimiter service: INTERNAL_ERROR replaced with SCHEME_ERROR when using not existing coordination nodes or resources.
https://github.com/ydb-platform/ydb/issues/16914 [#16901](https://github.com/ydb-platform/ydb/pull/16901) ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 16879:Fix crashes on scan queries with a predicate and a limit in OLAP tables #16878 [#16879](https://github.com/ydb-platform/ydb/pull/16879) ([Semyon](https://github.com/swalrus1))
* 16837:There was a problem with git->arc and arc->git syncs. Because logger in dq_tasks_runner (merged in git) was incompatible with comp nodes logging (merged in arc). When logging is turned off, the logger should be nullptr. But initially it was done so that the logger would not be nullptr, but the logging function inside the logger was empty. Which led to std::bad_function_call exception during the sync.

Arc changes were reverted here: https://a.yandex-team.ru/review/8380329/commits

Exception bt: https://paste.yandex-team.ru/fb6d23ba-8c02-4d2b-b32c-e5cfcc877dbb
... [#16837](https://github.com/ydb-platform/ydb/pull/16837) ([Filitov Mikhail](https://github.com/lll-phill-lll))

### Backward incompatible change

* 17194:Partially Revert https://github.com/ydb-platform/ydb/commit/5eed854386cd5d71eb2bf411106aeb6bf231d4d8 because It does not work

https://github.com/ydb-platform/ydb/issues/16944 [#17194](https://github.com/ydb-platform/ydb/pull/17194) ([Олег](https://github.com/iddqdex))

### Performance

* 16867:Use binsearch to find predicate bound in portions in OLAP tables [#16867](https://github.com/ydb-platform/ydb/pull/16867) ([Semyon](https://github.com/swalrus1))

