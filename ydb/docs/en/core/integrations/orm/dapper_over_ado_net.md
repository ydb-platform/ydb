# Using Dapper

[Dapper](https://dappertutorial.net/) is a micro ORM (Object-Relational Mapping) tool that provides a simple and flexible way to interact with databases. It operates on top of the [ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/) standard and offers various features that simplify database operations.

## ADO.NET

ADO.NET is a set of classes that provide developers with data access using the [.NET Framework](https://dotnet.microsoft.com/en-us/download/dotnet-framework) platform.

The [{{ ydb-short-name }} SDK for C#](https://github.com/ydb-platform/ydb-dotnet-sdk) offers a set of classes that implement the ADO.NET standard.

### Installation

The ADO.NET implementation for {{ ydb-short-name }} is available via NuGet.

### Creating a connection

A connection to {{ ydb-short-name }} is established using `YdbConnection`.

1. **Using the parameterless constructor**:

   The following code creates a connection with the default settings:

    ```c#
    await using var ydbConnection = new YdbConnection();
    await ydbConnection.OpenAsync();
    ```

   This option creates a connection to the database at the URL `grpc://localhost:2136/local` with anonymous authentication.

2. **Using the constructor with a connection string**:

   In the following example, a connection is created using a connection string:

   ```c#
   await using var ydbConnection = new YdbConnection(
       "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
   await ydbConnection.OpenAsync();
   ```

  In this case, the connection is established at the URL `grpc://database-sample-grpc:2135/root/database-sample`. When using the connection string method, parameters are specified as key-value pairs, separated by semicolons (`key1=value1;key2=value2`). The supported set of keys is explained [below](#connection-parameters).

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

### Connection parameters {#connection-parameters}

All available connection parameters are defined as properties in the `YdbConnectionStringBuilder`.

Here is a list of parameters that can be specified in the connection string:

| Parameter         | Description                                                                                         | Default Value |
|-------------------|-----------------------------------------------------------------------------------------------------|---------------|
| `Host`            | Specifies the server host                                                                           | `localhost`   |
| `Port`            | Specifies the server port                                                                           | `2136`        |
| `Database`        | Specifies database name                                                                             | `/local`      |
| `User`            | Specifies the username                                                                              | Not defined   |
| `Password`        | Specifies the user password                                                                         | Not defined   |
| `UseTls`          | Indicates whether to use the TLS protocol (grpc or grpcs)                                           | `false`       |
| `MaxSessionPool`  | Specifies the maximum session pool size                                                             | `100`         |
| `RootCertificate` | Specifies the path to the trusted server certificate. If this parameter is set, UseTls will be true | Not defined   |

There are also additional parameters that do not participate in forming the ConnectionString. These can only be specified using `YdbConnectionStringBuilder`:

| Parameter             | Description                                                     | Default Value |
|-----------------------|-----------------------------------------------------------------|---------------|
| `LoggerFactory`       | This parameter serves as a factory for creating logging classes | Not defined   |
| `CredentialsProvider` | Authenticates the user using an external IAM provider           | Not defined   |

### Usage

Executing queries is done through the YdbCommand object:

```c#
await using var ydbConnection = new YdbConnection();
await ydbConnection.OpenAsync();

var ydbCommand = ydbConnection.CreateCommand();
ydbCommand.CommandText = "SELECT 'Hello world!'u";
Console.WriteLine(await ydbCommand.ExecuteScalarAsync());
```

This example demonstrates outputting `Hello World!` to the console.

### Transactions

To create a client transaction, use the `ydbConnection.BeginTransaction()` method.

Optionally, this method can accept a parameter of type `IsolationLevel`, which specifies the transaction isolation level. The following isolation levels are supported:

- `Serializable`: Provides full transaction isolation using optimistic locks
- `Unspecified`: Allows the database to determine the most appropriate isolation level

You can also specify the `TxMode` parameter. For YDB-specific isolation levels, you can learn more [here](../../concepts/transactions.md).

The `Serializable` isolation level used with the `TxMode.SerializableRW` parameter is equivalent to calling `BeginTransaction()` without parameters.

Consider the following example of using a transaction:

```c#
await using var ydbConnection = new YdbConnection();
await ydbConnection.OpenAsync();

var ydbCommand = ydbConnection.CreateCommand();

ydbCommand.Transaction = ydbConnection.BeginTransaction();
ydbCommand.CommandText = """
                            UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
                            VALUES (2, 5, 13, "Test Episode", Date("2018-08-27"))
                         """;
await ydbCommand.ExecuteNonQueryAsync();

ydbCommand.CommandText = """
                         INSERT INTO episodes(series_id, season_id, episode_id, title, air_date)
                         VALUES
                             (2, 5, 21, "Test 21", Date("2018-08-27")),
                             (2, 5, 22, "Test 22", Date("2018-08-27"))
                         """;
await ydbCommand.ExecuteNonQueryAsync();
await ydbCommand.Transaction.CommitAsync();
```

Here, a transaction with the `Serializable` isolation level is opened and two inserts into the `episodes` table are executed.

### Using Parameters

SQL query parameters can be set using the YdbParameter class:

In this example, we declare the parameters $series_id, $season_id, and $limit_size within the SQL query and then add them to the command using YdbParameter objects.

```c#
await using var connection = new YdbConnection(_cmdOptions.SimpleConnectionString);
await connection.OpenAsync();

var ydbCommand = connection.CreateCommand();
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

var ydbDataReader = await ydbCommand.ExecuteReaderAsync();

_logger.LogInformation("Selected rows:");
while (await ydbDataReader.ReadAsync())
{
    _logger.LogInformation(
        "series_id: {series_id}, season_id: {season_id}, episode_id: {episode_id}, air_date: {air_date}, title: {title}",
        ydbDataReader.GetUint64(0), ydbDataReader.GetUint64(1), ydbDataReader.GetUint64(2),
        ydbDataReader.GetDateTime(3), ydbDataReader.GetString(4));
}
```

In this example, we declare the parameters `series_id`, `season_id`, and `limit_size` within the SQL query and then add them to the command using `YdbParameter` objects.

### Alternative style with @ prefix

Parameters can also be specified using the `@` prefix. In this case, there is no need to declare variables within the query itself. The query will look like this:

```c#
ydbCommand.CommandText = """
                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes
                         WHERE series_id = @series_id AND season_id > @season_id
                         ORDER BY series_id, season_id, episode_id
                         LIMIT @limit_size;
                         """;
```

ADO.NET will prepare the query for you so that the variables conform to the YQL standard. The type will be determined according to the `DbType` or the `System.Type` of the value itself.

### Type Mapping Table for Writing

| {{ ydb-short-name }}       | DbType                                                                                    | .Net types                   |
|----------------------------|-------------------------------------------------------------------------------------------|------------------------------|
| `Bool`                     | `Boolean`                                                                                 | `bool`                       |
| `Text` (synonym `Utf8`)    | `String`, `AnsiString`, `AnsiStringFixedLength`, `StringFixedLength`                      | `string`                     |
| `Bytes` (synonym `String`) | `Binary`                                                                                  | `byte[]`                     |
| `Uint8`                    | `Byte`                                                                                    | `byte`                       |
| `Uint16`                   | `UInt16`                                                                                  | `ushort`                     |
| `Uint32`                   | `UInt32`                                                                                  | `uint`                       |
| `Uint64`                   | `UInt64`                                                                                  | `ulong`                      |
| `Int8`                     | `SByte`                                                                                   | `sbyte`                      |
| `Int16`                    | `Int16`                                                                                   | `short`                      |
| `Int32`                    | `Int32`                                                                                   | `int`                        |
| `Int64`                    | `Int64`                                                                                   | `long`                       |
| `Float`                    | `Single`                                                                                  | `float`                      |
| `Double`                   | `Double`                                                                                  | `double`                     |
| `Date`                     | `Date`                                                                                    | `DateTime`                   |
| `Datetime`                 | `DateTime`                                                                                | `DateTime`                   |
| `Timestamp`                | `DateTime2` (for .NET type `DateTime`), `DateTimeOffset` (for .NET type `DateTimeOffset`) | `DateTime`, `DateTimeOffset` |
| `Decimal(22,9)`            | `Decimal`, `Currency`                                                                     | `decimal`                    |

It's important to understand that if the `DbType` is not specified, the parameter will be inferred from the `System.Type`.

You can also specify any {{ ydb-short-name }} type using the constructors from `Ydb.Sdk.Value.YdbValue`. For example:

```с#
var parameter = new YdbParameter("$parameter", YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")); 
```

### Error Handling

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

## Integration of {{ ydb-short-name }} and Dapper

To get started, you need an additional dependency [Dapper](https://www.nuget.org/packages/Dapper/).

Let's consider a complete example:

```c#
await using var connection = await new YdbDataSource().OpenConnectionAsync();

await connection.ExecuteAsync("""
                              CREATE TABLE Users(
                                  Id Int32,
                                  Name Text,
                                  Email Text,
                                  PRIMARY KEY (Id)   
                              );
                              """);

await connection.ExecuteAsync("INSERT INTO Users(Id, Name, Email) VALUES (@Id, @Name, @Email)",
    new User { Id = 1, Name = "Name", Email = "Email" });

Console.WriteLine(await connection.QuerySingleAsync<User>("SELECT * FROM Users WHERE Id = @Id", new { Id = 1 }));

await connection.ExecuteAsync("DROP TABLE Users");

internal class User
{
    public int Id { get; init; }
    public string Name { get; init; } = null!;
    public string Email { get; init; } = null!;

    public override string ToString()
    {
        return $"Id: {Id}, Name: {Name}, Email: {Email}";
    }
}
```

For more information, refer to the official [documentation](https://dappertutorial.net/).

### Important aspects

For Dapper to interpret `DateTime` values as the {{ ydb-short-name }} type `DateTime`, execute the following code:

```c#
SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime);
```

By default, `DateTime` is interpreted as `Timestamp`.
