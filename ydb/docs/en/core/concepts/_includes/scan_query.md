# Scan queries in {{ ydb-short-name }}

<<<<<<< HEAD
Scan Queries is a separate data access interface designed primarily for running analytical ad hoc queries against a DB.

This method of executing queries has the following unique features:

* Only *Read-Only* queries.
* In `SERIALIZABLE_RW` mode, a data snapshot is taken and then used for all subsequent operations. As a result, the impact on OLTP transactions is minimal (only taking a snapshot).
* The output of a query is a data stream ([grpc stream](https://grpc.io/docs/what-is-grpc/core-concepts/)). This means scan queries have no limit on the number of rows in the result.
* Due to the high overhead, it is only suitable for ad hoc queries.

{% note info %}

From the *Scan Queries* interface, you can query [system tables](../../dev/system-views.md).

{% endnote %}

Scan queries cannot currently be considered an effective solution for running OLAP queries due to their technical limitations (which will be removed in time):

* The query duration is limited to 10 minutes.
* Many operations (including sorting) are performed entirely in memory, which may lead to resource shortage errors when running complex queries.
* A single strategy is currently in use for joins: *MapJoin* (a.k.a. *Broadcast Join*) where the "right" table is converted to a map; and therefore, must be no more than single gigabytes in size.
* Prepared form isn't supported, so for each call, a query is compiled.
* There is no optimization for point reads or reading small ranges of data.
* The SDK doesn't support automatic retry.

For handling OLAP workloads in {{ ydb-short-name }}, there is a specialized type of table — [column-oriented](../datamodel/table.md#column-oriented-tables) tables. These tables store the data of each column separately from other columns. This allows only the columns directly involved in the query to be read during execution.
{% note info %}

Despite the fact that *Scan Queries* obviously don't interfere with the execution of OLTP transactions, they still use common DB resources: CPU, memory, disk, and network. Therefore, running complex queries **may lead to resource hunger**, which will affect the performance of the entire DB.

{% endnote %}

## How do I use it? {#how-use}

Like other types of queries, *Scan Queries* are available via the {% if link-console-main %}[management console]({{ link-console-main }}) (the query must specify `PRAGMA Kikimr.ScanQuery = "true";`),{% endif %} [CLI](../../reference/ydb-cli/commands/scan-query.md), and [SDK](../../reference/ydb-sdk/index.md).
=======
Scan queries are a separate data access interface primarily intended for running analytical ad-hoc queries against a database.

This execution mode has the following distinctive properties:

* It is *read-only*.
* In *SERIALIZABLE_RW* mode, a data snapshot is taken, and all further work happens on top of that snapshot. As a result, the impact on the OLTP transaction stream is minimal (only snapshot acquisition).
* The query result is a data stream ([gRPC stream](https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc)). Therefore, scan queries do not have a limit on the number of rows in the result.
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

For OLAP workloads, {{ ydb-short-name }} provides a specialized table type — [column-oriented](../datamodel/table.md#column-oriented-tables) tables. They store each column's data separately from other columns, so only the columns directly involved in the query are read during execution.

{% note info %}

Even though *Scan Queries* do not directly interfere with OLTP transactions, they still consume shared database resources: CPU, memory, disk, and network. Therefore, running heavy queries **may cause resource starvation**, which will affect the performance of the entire database.

{% endnote %}

## How to use {#how-use}

Like other query types, *Scan Queries* are available through {% if link-console-main %} the [management console]({{ link-console-main }}) (you must add the pragma `PRAGMA Kikimr.ScanQuery = "true";` to the query), {% endif %} [CLI](../../reference/ydb-cli/commands/scan-query.md) and [SDK](../../reference/ydb-sdk/index.md).
>>>>>>> 23d71c75863 ([YDBDOCS-2043] Вернуть описание scan_queries (#38583))

{% if oss %}

### C++ SDK {#cpp}

<<<<<<< HEAD
To run a query using *Scan Queries*, use 2 methods from the `Ydb::TTableClient` class:

```cpp
class TTableClient {
    ...
    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query,
        const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());

    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query, const TParams& params,
        const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());
    ...
=======
To run a query via *Scan Queries*, `Ydb::TTableClient` provides two methods:

```cpp
class TTableClient {
 ...
 TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query,
 const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());

 TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query, const TParams& params,
 const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());
 ...
>>>>>>> 23d71c75863 ([YDBDOCS-2043] Вернуть описание scan_queries (#38583))
};
```

{% endif %}
<<<<<<< HEAD

=======
>>>>>>> 23d71c75863 ([YDBDOCS-2043] Вернуть описание scan_queries (#38583))
