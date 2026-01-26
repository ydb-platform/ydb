# Basic Usage with ADO.NET

This article covers core [ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/) usage scenarios for {{ ydb-short-name }}, including database connections, query execution, and result processing. See the main [documentation](index.md) for additional details.

## Data Source {#data_source}

The entry point for any database operation is [DbDataSource](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatasource).

You can create a YdbDataSource in following ways.

1. **Without parameters**:

   The following code creates a data source with default settings:

    ```c#
    await using var ydbDataSource = new YdbDataSource();
    ```

   In this case, the connection URL is `grpc://localhost:2136/local` with anonymous authentication.

2. **Using a connection string**:

   Create a data source with an [ADO.NET connection string](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings)

    ```c#
    await using var ydbDataSource = new YdbDataSource(
        "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
    ```

   The data source will use URL: `grpc://database-sample-grpc:2135/root/database-sample.` The supported settings are described on [the connection parameters page](./connection-parameters.md).

3. **Using a YdbConnectionStringBuilder**:

    ```c#
    var ydbConnectionBuilder = new YdbConnectionStringBuilder
    {
        Host = "localhost",
        Port = 2136,
        Database = "/local",
        UseTls = false
    };
    
    await using var ydbDataSource = new YdbDataSource(ydbConnectionBuilder);
    ```

   YdbConnectionStringBuilder also supports additional [options](connection-parameters.md#connection-builder-parameters) beyond the connection string, such as logging and advanced authentication.

## Connections

A connection to {{ ydb-short-name }} is established via `YdbConnection`. You obtain connections from `YdbDataSource` using the following methods:

1. **YdbDataSource.OpenConnectionAsync**:

   Opens a connection to YDB using the parameters set on YdbDataSource (see the [YDB Data Source section](#data_source)).

    ```c#
    await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();
    ```

   Recommended for long-running read queries.

2. **YdbDataSource.OpenRetryableConnectionAsync**:

   Opens a connection with automatic operation retries that follow the YDB Retry Policy (see the [retries section](#retry_policy)).

   ```c#
   await using var ydbConnection = await ydbDataSource.OpenRetryableConnectionAsync();
   ```

   Mode specifics:
    - Interactive transactions are not supported.
    - Commands (YdbCommand) created from this connection automatically retry single operations on transient errors.
    - Attempting to use a transaction will throw an exception (see the [transactions section](#transactions)).

   {% note warning %}

   Be careful with “long” reads in this mode: they may lead to Out Of Memory (OOM), because the entire result set is read into memory to obtain final statuses from the server.

   {% endnote %}

3. **Not recommended**: CreateConnection/constructor:

   Using `YdbDataSource.CreateConnection` and the `YdbConnection` constructor (legacy ADO.NET API) is not recommended.
   If you still need it, open the connection manually:

    - With a connection string:

    ```c#
    await using var ydbConnection = new YdbConnection(
        "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
    await ydbConnection.OpenAsync(); 
    ```

    - With `YdbConnectionStringBuilder`:

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

    - With `YdbDataSource.CreateConnection`:

    ```c#
    await using var ydbConnection = ydbDataSource.CreateConnection();
    await ydbConnection.OpenAsync();
    ```

## Pooling

Opening a new connection to {{ ydb-short-name }} is an expensive operation, so the provider uses a connection pool. When a connection object is disposed or closed, it is not actually closed — instead, it’s returned to the pool managed by Ydb.Sdk.Ado. On subsequent requests, a pooled connection is reused. This makes open/close operations fast: open and close connections as needed, and avoid keeping them open unnecessarily for a long time.

{% note info %}

Pooling is in effect for connections opened via YdbDataSource (e.g., OpenConnectionAsync/OpenRetryableConnectionAsync), as well as for connections created manually with the YdbConnection constructor followed by OpenAsync().

{% endnote %}

{% note info %}

How it works under the hood: for application code, a “connection” is logical. Under the hood, operations are RPC calls over a small pool of gRPC/HTTP/2 channels. The provider also manages a table session pool. These details are transparent to the user and are controlled by pooling parameters (see Pooling settings: connection-parameters.md#pooling).

{% endnote %}

To clear the pool and close network channels to YDB nodes:

- `YdbDataSource.DisposeAsync()`: Disposes the data source. Closes all associated pools and network channels tied to the `ConnectionString`.

- `YdbConnection.ClearPool`: Immediately closes all idle connections in the pool associated with the specified connection’s `ConnectionString`. Active connections are closed when returned to the pool.

    ```c#
    await YdbConnection.ClearPool(ydbConnection);
    ```

- `YdbConnection.ClearAllPools()`: Immediately closes all idle connections in all pools. Active connections are closed when returned to the pool.  

    ```c#
    YdbConnection.ClearAllPools();
    ```

## Basic SQL Execution

Once you have a `YdbConnection`, an `YdbCommand` can be used to execute SQL against it:

```c#
await using var ydbCommand = new YdbCommand("SELECT some_field FROM some_table", ydbConnection);
await using var ydbDataReader = await ydbCommand.ExecuteReaderAsync();

while (await ydbDataReader.ReadAsync())
{
    Console.WriteLine(ydbDataReader.GetString(0));
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
await using var ydbCommand = new YdbCommand("INSERT INTO some_table (some_field) VALUES ('Hello YDB!'u)", ydbConnection);
await ydbCommand.ExecuteNonQueryAsync();
```

## Parameters

When sending data values to the database, always consider using parameters rather than including the values in the SQL, as shown in the following example:

```c#
await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();

var ydbCommand = ydbConnection.CreateCommand();
ydbCommand.CommandText = """
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

With ADO.NET, the query will be prepared for you so that the variables match [YQL](../../../yql/reference/index.md). The type of each parameter will be determined by YdbDbType, in its absence — by [DbType](https://learn.microsoft.com/en-us/dotnet/api/system.data.dbtype), otherwise it is derived from .NET is the type of value.

## Parameter Types

{{ ydb-short-name }} has a strongly-typed type system: columns and parameters have a type, and types are usually not implicitly converted to other types. This means you have to think about which type you will be sending: trying to insert a string into an integer column (or vice versa) will fail.

For more information on supported types and their mappings, see this [page](type-mapping.md).

## Transactions {#transactions}

Interactive transactions and retries are a key part of working with {{ ydb-short-name }}.

YdbDataSource provides helper methods that make it easier to run code in a transaction with automatic retries.

```c#
await ydbDataSource.ExecuteInTransactionAsync(async ydbConnection =>
{
    var count = (int)(await new YdbCommand(ydbConnection)
        { CommandText = $"SELECT count FROM {tableName} WHERE id = 1" }
        .ExecuteScalarAsync())!;

    await new YdbCommand(ydbConnection)
    {
        CommandText = $"UPDATE {tableName} SET count = @count + 1 WHERE id = 1",
        Parameters = { new YdbParameter { Value = count, ParameterName = "count" } }
    }.ExecuteNonQueryAsync();
},
new YdbRetryPolicyConfig { MaxAttempts = 5 });
```

Retries are performed according to YDB policies (see the [retries section](#retry_policy)).

## Transactions with YdbConnection

You can create a transaction in the standard ADO.NET way:

```c#
await using var connection = await dataSource.OpenConnectionAsync();
await using var transaction = await connection.BeginTransactionAsync();
// ... commands within the transaction ...
await transaction.CommitAsync();
```

{% note warning %}

In this mode, error handling (for example, [Transaction Lock Invalidated](https://ydb.tech/docs/en/troubleshooting/performance/queries/transaction-lock-invalidation)) is your responsibility. {{ ydb-short-name }} may roll back a transaction on MVCC lock invalidation.

{% endnote %}

{% note info %}

This is acceptable and recommended for long reads. Use [snapshot read-only](../../../concepts/transactions.md#modes) transactions for consistent snapshots; they do not take write locks and minimize conflicts.

{% endnote %}

There are two signatures of this method with a single isolation level parameter:

- `BeginTransaction(TxMode txMode)`<br>
  The `Ydb.Sdk.Services.Query.TxMode` is a  {{ ydb-short-name }} specific isolation level, you can read more about it [here](../../../concepts/transactions.md).

- `BeginTransaction(IsolationLevel isolationLevel)`<br>
  The `System.Data.IsolationLevel` parameter from the standard ADO.NET. The following isolation levels are supported: `Serializable` and `Unspecified`. Both are equivalent to the `TxMode.SerializableRW`.

Calling `BeginTransaction()` without parameters opens a transaction with level the `TxMode.SerializableRW`.

{{ ydb-short-name }} does not support nested or concurrent transactions. At any given moment, only one transaction per connection can be in progress, and starting a new transaction while another is already running throws an exception. Therefore, there is no need to pass the `YdbTransaction` object returned by `BeginTransaction()` to commands you execute. When a transaction is started, all subsequent commands are automatically included until a commit or rollback is made. To ensure maximum portability, however, it is best to set the transaction scope for your commands explicitly.

## Retries {#retry_policy}

Retries are an important part of {{ ydb-short-name }}’s design. The ADO.NET provider offers a flexible retry policy tailored to {{ ydb-short-name }} specifics.

Recommendations for choosing an approach:

- Single write operations (without interactive transactions): use `YdbDataSource.OpenRetryableConnectionAsync`.
- Transactional scenarios: use the `YdbDataSource.ExecuteInTransactionAsync` family.
- Executing code with automatic retries outside a transaction: use the `YdbDataSource.ExecuteAsync` family.
- Long read operations: use a regular `YdbConnection`. For consistent snapshots, use [snapshot read-only](../../../concepts/transactions.md#modes) transactions. Avoid retry connections for “long” reads to prevent excessive result buffering.

### Passing policy settings

You can pass `YdbRetryPolicyConfig` into `OpenRetryableConnectionAsync`, `ExecuteInTransactionAsync`, and `ExecuteAsync`.

- Connection with retries:

   ```c#
   await using var conn = await ydbDataSource.OpenRetryableConnectionAsync(
       new YdbRetryPolicyConfig { MaxAttempts = 5 });
   ```
  
- Transaction with retries:

   ```c#
   await ydbDataSource.ExecuteInTransactionAsync(
       async conn => { /* your code */ },
       new YdbRetryPolicyConfig { MaxAttempts = 5 });
   ```

- Executing a code block with retries:

   ```c#
   await ydbDataSource.ExecuteAsync(
       async conn => { /* your code */ },
       new YdbRetryPolicyConfig { MaxAttempts = 5 });
   ```

| **Параметр**           | **Описание**                                                                                                                                                                   | **Значение по умолчанию** |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| MaxAttempts            | Total number of attempts, including the initial one. A value of 1 disables retries entirely.                                                                                   | 10                        |
| EnableRetryIdempotence | Enables retries for statuses with unknown execution outcome on the server. Use only for idempotent operations — otherwise the operation may be applied twice.                  | false                     |
| FastBackoffBaseMs      | Base delay (ms) for fast retries: errors that typically resolve quickly (e.g., temporary unavailability, TLI — Transaction Lock Invalidated). Exponential backoff with jitter. | 5                         |
| FastCapBackoffMs       | Maximum delay (ms) for fast retries. Exponential backoff with jitter will not exceed this cap.                                                                                 | 500                       |
| SlowBackoffBaseMs      | Base delay (ms) for slow retries: overload, resource exhaustion, etc. Exponential backoff with jitter.                                                                         | 50                        |
| SlowCapBackoffMs       | Maximum delay (ms) for slow retries. Exponential backoff with jitter will not exceed this cap.                                                                                 | 5000                      |

### Custom retry policy

For edge cases, you can implement your own policy by implementing `Ydb.Sdk.Ado.Retry.IRetryPolicy`. The policy receives a `YdbException` and the current attempt number and must return whether to retry and with what delay.

{% note warning %}

If you choose this approach, be certain you understand what you’re doing: you are opting out of well‑tuned default settings.

{% endnote %}

## Error Handling

{% note info %}

This section is relevant if you do not use the provider’s built-in retries (see the [retries section](#retry_policy)).

{% endnote %}

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

Examples are provided on GitHub at [link](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples).
