## Unreleased

## 25.1.4

### Functionality

* 21474:Added the ability to configure tablet boot priorities via `HiveConfig` [#21474](https://github.com/ydb-platform/ydb/pull/21474) ([Constantine Gamora](https://github.com/ya-ksgamora))
* 23332:Added support for a user-defined Certificate Authority (CA) in asynchronous replication. [#23332](https://github.com/ydb-platform/ydb/pull/23332) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))

### Performance

* 23496:Added the new mode of operating [actor system](./concepts/glossary#actor-system), which allows for more efficient use of computing resources – the performance of a 4-core node is increased by 40%, now the performance per core is the same as that of a 10-core node; the performance of a 2-core node is increased by 110%, the performance per core is only 5% less than that of a 10-core node (previously it was 50% less). It is enabled by setting the `use_shared_threads` flag in the [actor-system configuration](./devops/configuration-management/configuration-v1/change_actorsystem_configs). [#23496](https://github.com/ydb-platform/ydb/pull/23496) ([kruall](https://github.com/kruall))

### Bug fixes

* 21472:Fixed temp dir owner id column name due to compatibility fail with 25-1-3. [#21472](https://github.com/ydb-platform/ydb/pull/21472) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 23367:[Fixed](https://github.com/ydb-platform/ydb/pull/23367) an [issue](https://github.com/ydb-platform/ydb/issues/23171) with transaction validation memory limit during execution plan increase memory limit for run execution plan. ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 24278:Fixed an issue where only the first message from a batch was saved when writing Kafka messages, with all other messages in the batch being ignored. [#24278](https://github.com/ydb-platform/ydb/pull/24278) ([Nikolay Shestakov](https://github.com/nshestakov))
* 24077:Fixed problem with access denied on /viewer/capabilities handler. Closes #24013. [#24077](https://github.com/ydb-platform/ydb/pull/24077) ([Alexey Efimov](https://github.com/adameat))
* 24666:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/23895) that scalar and block hash shuffles may be incompatible and it causes incorrect results, for example, in hash joins. Now no 2 different kinds of shuffles (SCALAR and BLOCK) should be inputs to the single stage. (#24033) [#24666](https://github.com/ydb-platform/ydb/pull/24666) ([Ivan](https://github.com/abyss7))

