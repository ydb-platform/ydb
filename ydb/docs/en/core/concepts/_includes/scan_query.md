# Scan queries in {{ ydb-short-name }}

Scan queries are a separate data access interface primarily intended for running analytical ad-hoc queries against a database.

This execution mode has the following distinctive properties:

* It is *read-only*.
* In *SERIALIZABLE_RW* mode, a data snapshot is taken, and all further work happens on top of that snapshot. As a result, the impact on the OLTP transaction stream is minimal (only snapshot acquisition).
* The query result is a data stream ([gRPC stream](https://grpc.io/docs/what-is-grpc/core-concepts/)). Therefore, scan queries do not have a limit on the number of rows in the result.
* Due to high overhead, it is suitable only for ad-hoc queries.

{% note info %}

You can query [system views](../../dev/system-views.md) through the *Scan Queries* interface.

{% endnote %}

Scan queries are not considered a full-fledged way to run OLAP workloads because they currently have a number of technical limitations (which will be lifted over time):

* Query duration is limited to 10 minutes.
* Many operations (including sorting) are executed entirely in memory, so complex queries may fail with an out-of-resources error.
* For joins, only one strategy is currently supported — *MapJoin* (aka *Broadcast Join*), where the "right" table is converted into a map, so it must be no more than a few gigabytes in size.
* Prepared statements are not supported, i.e. the query is compiled on every call.
* There are no optimizations for point reads or small-range reads.
* Automatic retries are not supported in SDKs.

For OLAP workloads, {{ ydb-short-name }} provides a specialized table type — [column-oriented](../datamodel/table.md#column-tables) tables. They store each column's data separately from other columns, so only the columns directly involved in the query are read during execution.

{% note info %}

Even though *Scan Queries* do not directly interfere with OLTP transactions, they still consume shared database resources: CPU, memory, disk, and network. Therefore, running heavy queries **may cause resource starvation**, which will affect the performance of the entire database.

{% endnote %}

## How to use {#how-use}

Like other query types, *Scan Queries* are available through {% if link-console-main %} the [management console]({{ link-console-main }}) (you must add the pragma `PRAGMA Kikimr.ScanQuery = "true";` to the query), {% endif %} [CLI](../../reference/ydb-cli/commands/scan-query.md) and [SDK](../../reference/ydb-sdk/index.md).

{% if oss %}

### C++ SDK {#cpp}

To run a query via *Scan Queries*, `Ydb::TTableClient` provides two methods:

```cpp
class TTableClient {
 ...
 TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query,
 const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());

 TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query, const TParams& params,
 const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());
 ...
};
```

{% endif %}
