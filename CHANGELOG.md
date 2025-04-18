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
* 16792:The patchset makes `ReplicateScalars` work only with WideStream I/O type in DQ. [#16792](https://github.com/ydb-platform/ydb/pull/16792) ([Igor Munkin](https://github.com/igormunkin))
* 16751:Supported C++ SDK build with gcc [#16751](https://github.com/ydb-platform/ydb/pull/16751) ([Bulat](https://github.com/Gazizonoki))
* 16686:enable gzip compression in main by default
resolves [#16328](https://github.com/ydb-platform/ydb/issues/16328) [#16686](https://github.com/ydb-platform/ydb/pull/16686) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16652:FQ: Add ability to create an external data sources for iceberg tables. [#16652](https://github.com/ydb-platform/ydb/pull/16652) ([Slusarenko Igor](https://github.com/buhtr))
* 16614:new parameter in AuthConfig — ClusterAccessResourceId — for defining a new resource with new roles for `Nebius_v1` [#16614](https://github.com/ydb-platform/ydb/pull/16614) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 16368:New logger for comp nodes was introduced here: https://a.yandex-team.ru/review/8203099/details.
This task is to support setting this logger in kqp. Currently I connected it to the KQP_TASKS_RUNNER logger which is enabled only in debug mode. Maybe it's worth moving it to KQP_YQL, but for now KQP_TASKS_RUNNER is simpler.

This logger will be used like this: 38e1a4ce5989a3c678b20914a779ca518858020d
... [#16368](https://github.com/ydb-platform/ydb/pull/16368) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 16347:Add version option to check current version of dstool. [#16347](https://github.com/ydb-platform/ydb/pull/16347) ([kruall](https://github.com/kruall))
* 16309:adds real cores and system threads stats to whiteboard system information [#16309](https://github.com/ydb-platform/ydb/pull/16309) ([Alexey Efimov](https://github.com/adameat))
* 16266:"--no-discovery" option allows to skip discovery and use user provided endpoint to connect to YDB cluster. [#16266](https://github.com/ydb-platform/ydb/pull/16266) ([Daniil Cherednik](https://github.com/dcherednik))
* 16251:fixes [#16276](https://github.com/ydb-platform/ydb/issues/16276) [#16251](https://github.com/ydb-platform/ydb/pull/16251) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16227:The patchset makes `ReplicateScalars` work with both WideFlow and WideStream I/O types in DQ. [#16227](https://github.com/ydb-platform/ydb/pull/16227) ([Igor Munkin](https://github.com/igormunkin))
* 16226:in some cases (k8s clusters, other network LB solutions) we need to skip client balancer and use database endpoint to perform query.

Old way to do it - configure it for each request. [#16226](https://github.com/ydb-platform/ydb/pull/16226) ([Daniil Cherednik](https://github.com/dcherednik))
* 16222:This test implements zip bomb. It writes equal columns in many rows and checks used memory during selects [#16222](https://github.com/ydb-platform/ydb/pull/16222) ([Alexander Avdonkin](https://github.com/aavdonkin))
* 16189:Supported parse parameters type in CLI [#16189](https://github.com/ydb-platform/ydb/pull/16189) ([Bulat](https://github.com/Gazizonoki))
* 16140:Add missing functions for topic data handler [#16140](https://github.com/ydb-platform/ydb/pull/16140) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 16129:Support coordination nodes in `ydb scheme rmdir --recursive`. [#16129](https://github.com/ydb-platform/ydb/pull/16129) ([Daniil Demin](https://github.com/jepett0))
* 16090:class TPrettyTable: improve compliance with the C++20 standard. [#16090](https://github.com/ydb-platform/ydb/pull/16090) ([ubyte](https://github.com/ubyte))
* 16076:Automatic deletion of temporary tables and directories during export [#16076](https://github.com/ydb-platform/ydb/pull/16076) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 16065:In case of investigation problems with unexpected timeout response It is helpful to know actual timeout for grpc call from the server perspective [#16065](https://github.com/ydb-platform/ydb/pull/16065) ([Daniil Cherednik](https://github.com/dcherednik))
* 16064:Add support for CMS API to address single PDisk for REPLACE_DEVICES action. Since PDisks can be restarted, there is no need to lock and shutdown entire host. [#16064](https://github.com/ydb-platform/ydb/pull/16064) ([Semyon Danilov](https://github.com/SammyVimes))
* 15968:This test checks that writing are disable after exceeding quota and deletions are always enabled [#15968](https://github.com/ydb-platform/ydb/pull/15968) ([Alexander Avdonkin](https://github.com/aavdonkin))
* 15911:- `ic_port` is now again recognized as correct in `template.yaml` format [#15911](https://github.com/ydb-platform/ydb/pull/15911) ([Egor Tarasov](https://github.com/Jorres))
* 15829:Cast unaligned pointer to pointer to multiple bytes integers  is UB [#15829](https://github.com/ydb-platform/ydb/pull/15829) ([Daniil Cherednik](https://github.com/dcherednik))
* 15823:Make shuffle elimination in JOIN work by default . [#15823](https://github.com/ydb-platform/ydb/pull/15823) ([pilik](https://github.com/pashandor789))
* 15800:YDB CLI help message improvements. Different display for detailed help and brief help. [#15800](https://github.com/ydb-platform/ydb/pull/15800) ([Vasily Gerasimov](https://github.com/UgnineSirdis))
* 15743:healthcheck report storage group layout incorrect [#15743](https://github.com/ydb-platform/ydb/pull/15743) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 15690:Добавлены время создания и время последнего обновления задачи обслуживания [#15690](https://github.com/ydb-platform/ydb/pull/15690) ([Ilia Shakhov](https://github.com/pixcc))
* 15662:Unlimited reading from solomon is now supported through external data sources [#15662](https://github.com/ydb-platform/ydb/pull/15662) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))
* 15599:remove automatic secondary indices warning [#15599](https://github.com/ydb-platform/ydb/pull/15599) ([Mikhail Surin](https://github.com/ssmike))
* 15595:Cache has been added to restrict to calculate argone2 hash function [#15595](https://github.com/ydb-platform/ydb/pull/15595) ([Andrey Molotkov](https://github.com/molotkov-and))
* 15587:Yield fullsync processing when it takes too long. [#15587](https://github.com/ydb-platform/ydb/pull/15587) ([Sergey Belyakov](https://github.com/serbel324))
* 15483:- added option `--backport-to-template` to ydb_configure [#15483](https://github.com/ydb-platform/ydb/pull/15483) ([Egor Tarasov](https://github.com/Jorres))
* 15477:add validation to never have tablet migration from root hive to itself [#15477](https://github.com/ydb-platform/ydb/pull/15477) ([vporyadke](https://github.com/vporyadke))

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
* 16722:Here was new feature ALTER DATABASE OWNER TO on SQL: https://github.com/ydb-platform/ydb/pull/14403

Then, it was [discovered](https://github.com/ydb-platform/ydb/issues/16430) that unit-test in KQP was flapping. There is fix with waiting tenant's up. [#16722](https://github.com/ydb-platform/ydb/pull/16722) ([flown4qqqq](https://github.com/flown4qqqq))
* 16588:Fix vanishing node bug [#16588](https://github.com/ydb-platform/ydb/pull/16588) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16438:Fixed the problem of reading from a column shard table with no columns specified: https://github.com/ydb-platform/ydb/issues/15845
Currently reads from column shards in Generic Query mode are only supported if at least one column in read.
This will be fixed in the future, but the temporary fix is to leave one key column in the reads.
Ticket for column shards: https://github.com/ydb-platform/ydb/issues/16599 [#16438](https://github.com/ydb-platform/ydb/pull/16438) ([Pavel Velikhov](https://github.com/pavelvelikhov))
* 16413:Currently, messages from the topic are deleted upon the occurrence of one of the events: the size of messages stored in the topic has been exceeded or the message lifetime has expired. [#16413](https://github.com/ydb-platform/ydb/pull/16413) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16389:fix database mapping and add name filter, closes https://github.com/ydb-platform/ydb/issues/13279 [#16389](https://github.com/ydb-platform/ydb/pull/16389) ([Alexey Efimov](https://github.com/adameat))
* 16315:YQ-4170 fixed external table alter [#16315](https://github.com/ydb-platform/ydb/pull/16315) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 16297:YQ-4207 used database name from table by default [#16297](https://github.com/ydb-platform/ydb/pull/16297) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 16216:The `PQ` tablet can receive the message `TEvPersQueue::TEvProposeTransaction` twice from the `SS`. For example, when there are delays in the operation of the `IC`. As a result, the `Drop Tablet` operation may hang in the `PQ` tablet.

#16218 [#16216](https://github.com/ydb-platform/ydb/pull/16216) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 16151:Fix VDisk compaction bug [#16151](https://github.com/ydb-platform/ydb/pull/16151) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16049:YQ-3655 added block splitting into DQ output channel [#16049](https://github.com/ydb-platform/ydb/pull/16049) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 15931:Don't crash after portion has stopped being optimized in tiering actializer. [#15684](https://github.com/ydb-platform/ydb/issues/15684) [#15931](https://github.com/ydb-platform/ydb/pull/15931) ([Semyon](https://github.com/swalrus1))
* 15889:At the start, the partition loads the list of consumers not only from the config, but also from the KV. Fixed a bug where the background partition had an empty list of config consumers, but the consumers were stored in KV.

Issue #15826 [#15889](https://github.com/ydb-platform/ydb/pull/15889) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 15878:Initialize field Engine before write to local database, to make possible to work by branch stable-24-3-15
... [#15878](https://github.com/ydb-platform/ydb/pull/15878) ([Alexander Avdonkin](https://github.com/aavdonkin))
* 15874:* Fixed `BackoffTimeout_` overflow in IAM credentials provider [#15874](https://github.com/ydb-platform/ydb/pull/15874) ([Aleksey Myasnikov](https://github.com/asmyasnikov))
* 15850:Fix optional columns handling in read_rows rpc #15701 [#15850](https://github.com/ydb-platform/ydb/pull/15850) ([Ivan Nikolaev](https://github.com/lex007in))
* 15720:Fixes invalid data shards histograms when `enable_local_dbbtree_index ` is enabled (#15235) [#15720](https://github.com/ydb-platform/ydb/pull/15720) ([kungurtsev](https://github.com/kunga))
* 15557:At startup, the background partition used the configuration of the main partition.

#15559 [#15557](https://github.com/ydb-platform/ydb/pull/15557) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 15516:Fix bug when sanitizer caused nullptr dereference https://github.com/ydb-platform/ydb/issues/15519
Add stress tests for sanitizer with INACTIVE/FAULTY statuses [#15516](https://github.com/ydb-platform/ydb/pull/15516) ([Sergey Belyakov](https://github.com/serbel324))

### Backward incompatible change

* 17194:Partially Revert https://github.com/ydb-platform/ydb/commit/5eed854386cd5d71eb2bf411106aeb6bf231d4d8 because It does not work

https://github.com/ydb-platform/ydb/issues/16944 [#17194](https://github.com/ydb-platform/ydb/pull/17194) ([Олег](https://github.com/iddqdex))

### Performance

* 16867:Use binsearch to find predicate bound in portions in OLAP tables [#16867](https://github.com/ydb-platform/ydb/pull/16867) ([Semyon](https://github.com/swalrus1))
* 15890:Регистрация узлов базы данных на стороне сервера теперь выполняется не полностью последовательно, а в режиме конвеера [#15890](https://github.com/ydb-platform/ydb/pull/15890) ([Ilia Shakhov](https://github.com/pixcc))
* 15792:Introduce NaiveBulkAnd and NaiveBulkAndWithBinary operations
Intersection of bitmaps performed finding smallest bitmap first, and then intersecting others bitmap with it
Smallest bitmap is always copied, as there's no way to ensure that modifying it state will not interfere with other data (item can be used in another part of the query) [#15792](https://github.com/ydb-platform/ydb/pull/15792) ([jsjant](https://github.com/jsjant))
* 15540:XDC allows to send huge events without splitting into small IC chunks. [#15540](https://github.com/ydb-platform/ydb/pull/15540) ([Daniil Cherednik](https://github.com/dcherednik))

### YDB UI

* 15530:Add configuration version in configs_dispatcher page [#15530](https://github.com/ydb-platform/ydb/pull/15530) ([mregrock](https://github.com/mregrock))

