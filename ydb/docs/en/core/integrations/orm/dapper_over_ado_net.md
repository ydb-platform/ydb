# Using Dapper

Dapper is a micro ORM (Object Relational Mapping) that provides a simple and flexible way to interact with databases. It works on top of the ADO.NET standard and offers numerous features that simplify database operations.

## ADO.NET

ADO.NET is a set of classes that provide data access for developers using the .NET Framework platform.

The YDB SDK C# provides a set of classes implementing the ADO.NET standard.

### Installation

The ADO.NET implementation for YDB is available through NuGet.

### Creating a Connection

A connection to YDB is established using YdbConnection.

1. **Using the parameterless constructor**:

    The following code creates a connection with default settings:

    ```c#
    await using var ydbConnection = new YdbConnection();
    await ydbConnection.OpenAsync();
    ```
    
    This option creates a connection to the database at the URL: grpc://localhost:2136/local, with anonymous authentication.

2. **Using the constructor with a connection string**:

    In the following example, a connection is created using a connection string:

    ```c#
    await using var ydbConnection = new YdbConnection(
       "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
    await ydbConnection.OpenAsync();
    ```

    In this case, the connection will be established at the URL: grpc://database-sample-grpc:2135/root/database-sample. When using the connection string method, parameters are specified as key=value pairs separated by a semicolon (`key1=value1;key2=value2`). The set of keys has fixed values, which will be discussed in detail in the following sections.

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

### Connection Parameters

All available connection parameters are defined as properties in the YdbConnectionStringBuilder class.

Here is a list of parameters that can be specified in the ConnectionString:

| Parameter         | Description                                                                                         | Default Value     |
|-------------------|-----------------------------------------------------------------------------------------------------|-------------------|
| `Host`            | Specifies the server host                                                                           | `localhost`       |
| `Port`            | Specifies the server port                                                                           | `2136`            |
| `Database`        | Specifies database name                                                                             | `/local`          |
| `User`            | Specifies the username                                                                              | Not defined       |
| `Password`        | Specifies the user password                                                                         | Not defined       |
| `UseTls`          | Indicates whether to use the TLS protocol (grpc or grpcs)                                           | `false`           |
| `MaxSessionPool`  | Specifies the maximum session pool size                                                             | `100`             |
| `RootCertificate` | Specifies the path to the trusted server certificate. If this parameter is set, UseTls will be true | Not defined       |

There are also additional parameters that do not participate in forming the ConnectionString. These can only be specified using `YdbConnectionStringBuilder`:

| Parameter             | Description                                                     | Default Value       |
|-----------------------|-----------------------------------------------------------------|---------------------|
| `LoggerFactory`       | This parameter serves as a factory for creating logging classes | Not defined         |
| `CredentialsProvider` | Authenticates the user using an external IAM provider           | Not defined         |

### Usage

Executing queries is done through the YdbCommand object: 

```c#
await using var ydbConnection = new YdbConnection();
await ydbConnection.OpenAsync();

var ydbCommand = ydbConnection.CreateCommand();
ydbCommand.CommandText = "SELECT 'Hello world!'u";
Console.WriteLine(await ydbCommand.ExecuteScalarAsync());
```

This example demonstrates outputting "Hello World!" to the console.
