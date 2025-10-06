## Unreleased

### Functionality

* 23917:Added health check overload shard hint. [#23917](https://github.com/ydb-platform/ydb/pull/23917) ([Alexey Efimov](https://github.com/adameat))
* 25979:added support to read/write `Date32`, `Datetime64`, `Timestamp64` and `Decimal(n, m)` from/to external data sources with "ObjectStorage" source type and "parquet" format [#25979](https://github.com/ydb-platform/ydb/pull/25979) ([Ivan Sukhov](https://github.com/evanevanevanevannnn))

### Bug fixes

* 25689:Fixes [#25524](https://github.com/ydb-platform/ydb/issues/25524) – issue with importing tables with Utf8 primary keys and changefeeds [#25689](https://github.com/ydb-platform/ydb/pull/25689) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 25622:Fixes segfault in version parser [#25586](https://github.com/ydb-platform/ydb/issues/25586) Removes misleading comments in code [#25622](https://github.com/ydb-platform/ydb/pull/25622) ([Sergey Belyakov](https://github.com/serbel324))
* 25453:Listing of objects with a common prefix has been fixed when importing changefeeds [#25454](https://github.com/ydb-platform/ydb/issues/25454) [#25453](https://github.com/ydb-platform/ydb/pull/25453) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))
* 25148:fix crash after follower alter https://github.com/ydb-platform/ydb/issues/20866 [#25148](https://github.com/ydb-platform/ydb/pull/25148) ([vporyadke](https://github.com/vporyadke))
* 25122:fix a bug where tablet deletion might get stuck [#23858](https://github.com/ydb-platform/ydb/issues/23858) [#25122](https://github.com/ydb-platform/ydb/pull/25122) ([vporyadke](https://github.com/vporyadke))
* 26283:The response code for reading from a partition via the Kafka protocol (fetch) has been fixed. Previously, in some cases, UNKNOWN_SERVER_ERROR responses could be returned, even though the processing of these partitions had completed successfully. [#26283](https://github.com/ydb-platform/ydb/pull/26283) ([Nikolay Shestakov](https://github.com/nshestakov))
* 26256:Fixes missing data shard stats event handling that may lead to not splitting by size data shards [#26256](https://github.com/ydb-platform/ydb/pull/26256) ([kungurtsev](https://github.com/kunga))
* 26197:Fix blob deserialization to support empty payload [#26197](https://github.com/ydb-platform/ydb/pull/26197) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 26069:Removed wrong ensure at preparing batches for writing to ColumnShard (Fixed #25869). Excluded .tmp dir from backup/replication. Renamed temporary directory to random uuid. [#26069](https://github.com/ydb-platform/ydb/pull/26069) ([Nikita Vasilev](https://github.com/nikvas0))

