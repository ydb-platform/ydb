## Unreleased

### Functionality
* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* None:15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
* 16834:* YDB FQ: enable pushdown of string types in Generic provider [#16834](https://github.com/ydb-platform/ydb/pull/16834) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 16792:The patchset makes `ReplicateScalars` work only with WideStream I/O type in DQ. [#16792](https://github.com/ydb-platform/ydb/pull/16792) ([Igor Munkin](https://github.com/igormunkin))

### Bug fixes
* 15721:Fixed a bug in YDB UUID column handling in ReadTable SDK method. [#15721](https://github.com/ydb-platform/ydb/pull/15721) ([Ivan Nikolaev](https://github.com/lex007in))
* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16060:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the **RETURNING** clause  work incorrectly with INSERT/UPSERT operations. [#16060](https://github.com/ydb-platform/ydb/pull/16060) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 10650:21:Fixed a rare error that led to a VERIFY error when replicating data. #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16837:There was a problem with git->arc and arc->git syncs. Because logger in dq_tasks_runner (merged in git) was incompatible with comp nodes logging (merged in arc). When logging is turned off, the logger should be nullptr. But initially it was done so that the logger would not be nullptr, but the logging function inside the logger was empty. Which led to std::bad_function_call exception during the sync.

Arc changes were reverted here: https://a.yandex-team.ru/review/8380329/commits

Exception bt: https://paste.yandex-team.ru/fb6d23ba-8c02-4d2b-b32c-e5cfcc877dbb
... [#16837](https://github.com/ydb-platform/ydb/pull/16837) ([Filitov Mikhail](https://github.com/lll-phill-lll))

