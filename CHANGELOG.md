## Unreleased

### Functionality

* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
and timeout (by default, the maximum response time from healthcheck). Documentation is under construction. [#15693](https://github.com/ydb-platform/ydb/pull/15693) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17394:Added counters to the spilling IO queue to track the number of waiting operations. Documentation is under construction #17599. [#17394](https://github.com/ydb-platform/ydb/pull/17394) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17362:Add support for [google breakpad](https://chromium.googlesource.com/breakpad/breakpad) inside YDB. Now you can set a minidumps path using an environment variable.
[#17362](https://github.com/ydb-platform/ydb/pull/17362) ([Олег](https://github.com/iddqdex))
* 17148:Extended federated query capabilities to support a new external data source [Prometheus](https://en.wikipedia.org/wiki/Prometheus_(software)). Documentation is under construction YQ-4261 [#17148](https://github.com/ydb-platform/ydb/pull/17148) ([Stepan](https://github.com/pstpn))
* 17007:Extended federated query capabilities to support a new external data source [Apache Iceberg](https://iceberg.apache.org). Documentation is under construction YQ-4266 [#17007](https://github.com/ydb-platform/ydb/pull/17007) ([Slusarenko Igor](https://github.com/buhtr))
* 16076:Added automatic cleanup of temporary tables and directories created during S3 export operations. Previously, users had to manually remove temporary directories and tables that were created as part of the export pipeline. [#16076](https://github.com/ydb-platform/ydb/pull/16076) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 18339:grpc check actor: check folder_id user attribute instead of container_id [#18339](https://github.com/ydb-platform/ydb/pull/18339) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 18214:Supported import to s3 for topic configuration [#18214](https://github.com/ydb-platform/ydb/pull/18214) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 18138:Export topic's configuration to S3 [#18138](https://github.com/ydb-platform/ydb/pull/18138) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 18095:In cms/console/ add audit logging.

There are fields of BeginCreate:
1. Component = console
2. Subject
3. Sanitized_token
4. Database
5. Status = SUCCESS
6. Operation

In EndCreate there is `reason` field, because in this case status can be ERROR. [#18095](https://github.com/ydb-platform/ydb/pull/18095) ([flown4qqqq](https://github.com/flown4qqqq))
* 18063:Support new version format Year.Major.Minor.Patch.Hotfix (old format is Year.Major.Minor.Hotfix) [#18063](https://github.com/ydb-platform/ydb/pull/18063) ([Sergey Belyakov](https://github.com/serbel324))
* 17957:This PR adds BS Controller settings to config.yaml
Example:

```yaml
blob_storage_config:
  bsc_settings:
    enable_self_heal: false
``` [#17957](https://github.com/ydb-platform/ydb/pull/17957) ([Semyon Danilov](https://github.com/SammyVimes))
* 17952:Store the access resource ID for cluster information as the `container_id` user attribute in the root database. Users can check permissions on this resource to access cluster-related information. [#17952](https://github.com/ydb-platform/ydb/pull/17952) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17804:Added CPU limit per process in CS for integration with Workload Manager [#17804](https://github.com/ydb-platform/ydb/pull/17804) ([Pisarenko Grigoriy](https://github.com/GrigoriyPA))
* 17751:Added Create Time, End Time and Created By (user SID) tracking for BuildIndex operations. [#17751](https://github.com/ydb-platform/ydb/pull/17751) ([Vitaliy Filippov](https://github.com/vitalif))

### Bug fixes

* 15721:Fixed a bug in YDB UUID column handling in ReadTable SDK method. [#15721](https://github.com/ydb-platform/ydb/pull/15721) ([Ivan Nikolaev](https://github.com/lex007in))
* 16061:Fixed a bug in handling OLAP scan queries with predicates. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16060:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the **RETURNING** clause  work incorrectly with INSERT/UPSERT operations. [#16060](https://github.com/ydb-platform/ydb/pull/16060) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16021:Fixed a rare error that led to a VERIFY error when replicating data. #10650 [#16021](https://github.com/ydb-platform/ydb/pull/16021) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16768:Fixed an issue with topic auto-partitioning when the `max_active_partition` configuration parameter was set via the `ALTER TOPIC` statement. [#16768](https://github.com/ydb-platform/ydb/pull/16768) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16764:Fixed redirects from cluster endpoints (storage nodes) to database nodes, resolving inconsistent behavior where some system tables were not visible. #16763 [#16764](https://github.com/ydb-platform/ydb/pull/16764) ([Alexey Efimov](https://github.com/adameat))
* 17198:Fixed an issue with UUID data type handling in YDB CLI backup/restore operations. [#17198](https://github.com/ydb-platform/ydb/pull/17198) ([Semyon Danilov](https://github.com/SammyVimes))
* 17157:Viewer API: Fixed the retrieval of tablet list for tables implementing secondary indexes. #17103 [#17157](https://github.com/ydb-platform/ydb/pull/17157) ([Alexey Efimov](https://github.com/adameat))
* 18401:Do not save to local backups destination tables of `ASYNC REPLICATION` and its changefeeds. It prevents duplication of changefeeds and reduces the amount of space the backup takes on disk. [#18401](https://github.com/ydb-platform/ydb/pull/18401) ([Daniil Demin](https://github.com/jepett0))
* 18307:Issue #18290

The TEvReadSet message can be sent again. When it was first sent, it was serialized and stored as a string. It turned out that the serialized message takes up a lot of memory space. For transactions with a large number of participants, this takes up a lot of space.

I changed it so that a copy of the data is stored to create a TEvReadSet when resending. [#18307](https://github.com/ydb-platform/ydb/pull/18307) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18234:Don't allow conflicting read-write transactions to violate serializability after shard restarts. Fixes #18065. [#18234](https://github.com/ydb-platform/ydb/pull/18234) ([Aleksei Borzenkov](https://github.com/snaury))
* 18222:Fix `ydb operation get` not working for running operations (#17001) [#18222](https://github.com/ydb-platform/ydb/pull/18222) ([Vitaliy Filippov](https://github.com/vitalif))
* 18201:Table auto splitting-merging: Fixed crash when selecting split key from access samples containing a mix of full key and key prefix operations (e.g. exact/range reads). [#18201](https://github.com/ydb-platform/ydb/pull/18201) ([ijon](https://github.com/ijon))
* 18056:fix to keep precision of double values on serialization
closes https://github.com/ydb-platform/ydb-embedded-ui/issues/2164 [#18056](https://github.com/ydb-platform/ydb/pull/18056) ([Alexey Efimov](https://github.com/adameat))

