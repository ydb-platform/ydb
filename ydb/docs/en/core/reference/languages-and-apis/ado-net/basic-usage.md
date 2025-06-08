# Basic Usage with ADO.NET

This article covers core [ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/) usage scenarios for {{ ydb-short-name }}, including database connections, query execution, and result processing. See the main [documentation](index.md) for additional details.

## Connections

A connection to {{ ydb-short-name }} is established using `YdbConnection`.

1. **Using an empty connection**:

   The following code creates a connection with the default settings:

    ```c#
    await using var ydbConnection = new YdbConnection("");
    await ydbConnection.OpenAsync();
    ```

   This option creates a connection to the database at the URL `grpc://localhost:2136/local` with anonymous authentication.

2. **Using the constructor with a connection string**:

   In the following example, a connection is created using a [connection string in ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings):

   ```c#
   await using var ydbConnection = new YdbConnection(
       "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
   await ydbConnection.OpenAsync();
   ```

   In this case, the connection is established at the URL `grpc://database-sample-grpc:2135/root/database-sample`. The supported set of settings is explained on the [connection parameters page](connection-parameters.md).

3. **Using the constructor with a `YdbConnectionStringBuilder` argument**:

   The example using `YdbConnectionStringBuilder` is demonstrated in the code below:

    ```c#
    var ydbConnectionBuilder = new YdbConnectionStringBuilder
    {
        Host = "server",
        Port = 2135,
        Database = "/ru-prestable/my-table",
        UseTls = true
    };
    await using var ydbConnection = new YdbConnection(ydbConnectionBuilder);
    await ydbConnection.OpenAsync();
    ```

   `YdbConnectionStringBuilder` supports additional [configuration](connection-parameters.md#connection-builder-parameters) beyond the connection string, such as logging, advanced authentication options.

## Pooling

Opening and closing a logical connection to {{ ydb-short-name }} is an expensive and time-consuming process. Therefore, connections to {{ ydb-short-name }} are pooled. Closing or disposing of a connection does not close the underlying logical connection; rather, it returns it to a pool managed by `Ydb.Sdk.Ado`. When a connection is needed again, a pooled connection is returned. This makes opening and closing operations extremely fast. Do not hesitate to open and close connections often if necessary, rather than keeping a connection open unnecessarily for a long period of time.

### ClearPool

Closes idle connections immediately. Active connections close when returned.

```c#
YdbConnection.ClearPool(ydbConnection)
```

### ClearAllPools

Closes all idle connections across all pools. Active connections close on return.

```c#
YdbConnection.ClearAllPools()
```

## Data Source

Starting with .NET 7.0, the starting point for any database operation is [DbDataSource](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatasource).

The simplest way to create a data source is the following:

```c#
await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
```

Or

```c#
var ydbConnectionBuilder = new YdbConnectionStringBuilder
{
    Host = "localhost",
    Port = 2136,
    Database = "/local",
    UseTls = false
};

await using var dataSource = new YdbDataSource(ydbConnectionBuilder);
```

## Basic SQL Execution

Once you have a `YdbConnection`, an `YdbCommand` can be used to execute SQL against it:

```c#
await using var command = dataSource.CreateCommand("SELECT some_field FROM some_table")
await using var reader = await command.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

## Other Execution Methods

Above, SQL is executed via [ExecuteReaderAsync](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbcommand.executereaderasync). There are various ways to execute a command, depending on the results you expect from it:

1. [ExecuteNonQueryAsync](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbcommand.executenonqueryasync): executes SQL that doesn't return any results, typically `INSERT`, `UPDATE`, or `DELETE` statements.

   {% note warning %}

   {{ ydb-short-name }} does not return the number of rows affected.

   {% endnote %}

2. [ExecuteScalarAsync](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbcommand.executescalarasync): executes SQL that returns a single scalar value.
3. [ExecuteReaderAsync](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbcommand.executereaderasync): executes SQL that returns a full result set. Returns a `YdbDataReader`, which can be used to access the result set (as in the example above).

For example, to execute a simple SQL `INSERT` that does not return anything, you can use `ExecuteNonQueryAsync` as follows:

```c#
await using var command = dataSource.CreateCommand("INSERT INTO some_table (some_field) VALUES ('Hello YDB!'u)");
await command.ExecuteNonQueryAsync();
```

## Parameters

When sending data values to the database, always consider using parameters rather than including the values in the SQL, as shown in the following example:

```c#
await using var connection = new YdbConnection(_cmdOptions.SimpleConnectionString);
await connection.OpenAsync();

var ydbCommand = connection.CreateCommand();
ydbCommand.CommandText = """
                         DECLARE $series_id AS Uint64;
                         DECLARE $season_id AS Uint64;
                         DECLARE $limit_size AS Uint64;

                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes WHERE series_id = $series_id AND season_id > $season_id
                         ORDER BY series_id, season_id, episode_id
                         LIMIT $limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("$series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$limit_size", DbType.UInt64, 3U));

var ydbDataReader = await ydbCommand.ExecuteReaderAsync();
```

SQL query parameters can be set using the `YdbParameter` class.

In this example, the parameters `$series_id`, `$season_id`, and `$limit_size` are declared within the SQL query and then added to the command using `YdbParameter` objects.

## Alternative Parameter Style with `@` Prefix

Parameters can also be specified using the `@` prefix. In this case, there is no need to declare variables within the query itself. The query will look like this:

```c#
ydbCommand.CommandText = """
                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes
                         WHERE series_id = @series_id AND season_id > @season_id
                         ORDER BY series_id, season_id, episode_id
                         LIMIT @limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("limit_size", DbType.UInt64, 3U));
```

With ADO.NET, the query will be prepared for you so that the variables match [YQL](../../../yql/reference/index.md). The type will be determined according to the [DbType](https://learn.microsoft.com/en-us/dotnet/api/system.data.dbtype) or the .NET type of the value itself.

## Parameter Types

{{ ydb-short-name }} has a strongly-typed type system: columns and parameters have a type, and types are usually not implicitly converted to other types. This means you have to think about which type you will be sending: trying to insert a string into an integer column (or vice versa) will fail.

For more information on supported types and their mappings, see this [page](type-mapping.md).

## Transactions

To create a client transaction, use the standard ADO.NET `ydbConnection.BeginTransaction()` method.

There are two signatures of this method with a single isolation level parameter:

- `BeginTransaction(TxMode txMode)`<br>
  The `Ydb.Sdk.Services.Query.TxMode` is a  {{ ydb-short-name }} specific isolation level, you can read more about it [here](../../../concepts/transactions.md).

- `BeginTransaction(IsolationLevel isolationLevel)`<br>
  The `System.Data.IsolationLevel` parameter from the standard ADO.NET. The following isolation levels are supported: `Serializable` and `Unspecified`. Both are equivalent to the `TxMode.SerializableRW`.

Calling `BeginTransaction()` without parameters opens a transaction with level the `TxMode.SerializableRW`.

Consider the following example of using a transaction:

```c#
await using var connection = await dataSource.OpenConnectionAsync();
await using var transaction = await connection.BeginTransactionAsync();

await using var command1 = new YdbCommand(connection) { CommandText = "...", Transaction = transaction };
await command1.ExecuteNonQueryAsync();

await using var command2 = new YdbCommand(connection) { CommandText = "...", Transaction = transaction };
await command2.ExecuteNonQueryAsync();

await transaction.CommitAsync();
```

{{ ydb-short-name }} does not support nested or concurrent transactions. At any given moment, only one transaction per connection can be in progress, and starting a new transaction while another is already running throws an exception. Therefore, there is no need to pass the `YdbTransaction` object returned by `BeginTransaction()` to commands you execute. When a transaction is started, all subsequent commands are automatically included until a commit or rollback is made. To ensure maximum portability, however, it is best to set the transaction scope for your commands explicitly.

## Error Handling

All exceptions related to database operations are subclasses of `YdbException`.

To safely handle errors that might occur during command execution, you can use a `try-catch` block. Here is an example:

```c#
try
{
    await command.ExecuteNonQueryAsync();
}
catch (YdbException e)
{
    Console.WriteLine($"Error executing command: {e}");
}
```

### Properties of `YdbException`

The `YdbException` exception has the following properties, which can help you handle errors properly:

- `IsTransient` returns `true` if the error is temporary and can be resolved by retrying. For example, this might occur in cases of a transaction lock violation when the transaction fails to complete its commit.

- `IsTransientWhenIdempotent` returns `true` if the error is temporary and can be resolved by retrying the operation, provided that the database operation is idempotent.

- `StatusCode` contains the database error code, which is helpful for logging and detailed analysis of the issue.

{% note warning %}

Please note that ADO.NET does not automatically retry failed operations, and you must implement retry logic in your code.

{% endnote %}

## Examples

Examples are provided on GitHub at [link](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples/src/AdoNet).
