## Unreleased

### Functionality

* 15186:Increased [the query text limit size](../dev/system-views#query-metrics) in system views from 4 KB to 10 KB. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15693:Added a health check configuration that administrators can customize: the number of node restarts, tablets, the time difference between database dynodes,
and timeout (by default, the maximum response time from healthcheck). Documentation is under construction. [#15693](https://github.com/ydb-platform/ydb/pull/15693) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 17394:Added counters to the spilling IO queue to track the number of waiting operations. Documentation is under construction #17599. [#17394](https://github.com/ydb-platform/ydb/pull/17394) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17362: Add support for [google breakpad](https://chromium.googlesource.com/breakpad/breakpad) inside YDB. Now you can set a minidumps path using an environment variable.
[#17362](https://github.com/ydb-platform/ydb/pull/17362) ([Олег](https://github.com/iddqdex))
* 17148:Extended federated query capabilities to support a new external data source [Prometheus](https://en.wikipedia.org/wiki/Prometheus_(software)). Documentation is under construction YQ-4261 [#17148](https://github.com/ydb-platform/ydb/pull/17148) ([Stepan](https://github.com/pstpn))
* 17007:Extended federated query capabilities to support a new external data source [Apache Iceberg](https://iceberg.apache.org). Documentation is under construction YQ-4266 [#17007](https://github.com/ydb-platform/ydb/pull/17007) ([Slusarenko Igor](https://github.com/buhtr))
* 16652:Added support for creating external data sources for iceberg tables. [#16652](https://github.com/ydb-platform/ydb/pull/16652) ([Slusarenko Igor](https://github.com/buhtr))
* 16957:Added support for a new external data source ([Redis](https://redis.io/)) in federated queries. [#16957](https://github.com/ydb-platform/ydb/pull/16957) ([Gleb Solomennikov](https://github.com/Glebbs))
* 17095:Lowered the severity level for FAULTY pdisks. [#17095](https://github.com/ydb-platform/ydb/pull/17095) ([Andrei Rykov](https://github.com/StekPerepolnen))
* 16989:Added information about `MODIFY(ALTER) USER` operations to audit_log: change a password (or hash), block a user, unblock a user. [#16989](https://github.com/ydb-platform/ydb/pull/16989) ([flown4qqqq](https://github.com/flown4qqqq))
* 16857:Added information about the processing lag of committed messages to `DescribeConsumer`. [#16857](https://github.com/ydb-platform/ydb/pull/16857) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16834:Enabled pushdown of string types in Generic provider. [#16834](https://github.com/ydb-platform/ydb/pull/16834) ([Vitaly Isaev](https://github.com/vitalyisaev2))

### Bug fixes

* 16061:Fixed a bug in handling scan queries with predicates in column-oriented tables. [#16061](https://github.com/ydb-platform/ydb/pull/16061) ([Semyon](https://github.com/swalrus1))
* 16879:Fix crashes on scan queries with a predicate and a limit in column-oriented tables. #16878 [#16879](https://github.com/ydb-platform/ydb/pull/16879) ([Semyon](https://github.com/swalrus1))
* 16016:Fixed rare node failures during read session balancing. https://github.com/ydb-platform/ydb/issues/16017 [#16016](https://github.com/ydb-platform/ydb/pull/16016) ([Nikolay Shestakov](https://github.com/nshestakov))
* 16423:Changed behavior — `SHOW CREATE TABLE` now fails on views instead of producing wrong output. [#16423](https://github.com/ydb-platform/ydb/pull/16423) ([Daniil Demin](https://github.com/jepett0))
* 16764:Fixed redirects from cluster endpoints (storage nodes) to database nodes, resolving inconsistent behavior where some system tables were not visible. #16763 [#16764](https://github.com/ydb-platform/ydb/pull/16764) ([Alexey Efimov](https://github.com/adameat))
* 17157:Viewer API: Fixed the retrieval of tablet list for tables implementing secondary indexes. #17103 [#17157](https://github.com/ydb-platform/ydb/pull/17157) ([Alexey Efimov](https://github.com/adameat))
* 17230:Added more information to the error message that occurs during Generic::TPartition parsing. [#17230](https://github.com/ydb-platform/ydb/pull/17230) ([Vitaly Isaev](https://github.com/vitalyisaev2))
* 17157:Fixed an issue with filters for tablets on nodes. Resolves issue #17103. [#17157](https://github.com/ydb-platform/ydb/pull/17157) ([Alexey Efimov](https://github.com/adameat))
* 17009:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/16938) with compiling a `JOIN` operation, in which one side was an empty constant. [#17009](https://github.com/ydb-platform/ydb/pull/17009) ([Nikita Vasilev](https://github.com/nikvas0))
* 16901:Changed the error response in the RateLimiter service from INTERNAL_ERROR to SCHEME_ERROR when using non-existent coordination nodes or resources. https://github.com/ydb-platform/ydb/issues/16914 [#16901](https://github.com/ydb-platform/ydb/pull/16901) ([Vasily Gerasimov](https://github.com/UgnineSirdis))

### Performance

* 16867:Enhanced column-oriented table query performance by applying binsearch for predicate bound detection in portions. [#16867](https://github.com/ydb-platform/ydb/pull/16867) ([Semyon](https://github.com/swalrus1))