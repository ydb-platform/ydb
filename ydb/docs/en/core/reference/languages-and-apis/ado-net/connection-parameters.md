# ADO.NET Connection Parameters

To connect to a database, the application provides a connection string that specifies parameters such as the host, user, password, and so on. Connection strings have the form `keyword1=value; keyword2=value;`. For more information, [see the official doc page on connection strings](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings).

All available connection parameters are defined as properties in the `YdbConnectionStringBuilder`.

Below are the connection string parameters that `Ydb.Sdk.Ado` supports.

## Basic Connection

| Parameter         | Description                                                                                                | Default value |
|-------------------|------------------------------------------------------------------------------------------------------------|---------------|
| `Host`            | Specifies the {{ ydb-short-name }} server host                                                             | `localhost`   |
| `Port`            | Specifies the {{ ydb-short-name }} server port                                                             | `2136`        |
| `Database`        | Specifies the database name                                                                                | `/local`      |
| `User`            | Specifies the username                                                                                     | Not defined   |
| `Password`        | Specifies the user password                                                                                | Not defined   |

## Security and Encryption

| Parameter         | Description                                                                                                                            | Default value |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `UseTls`          | Indicates whether to use the TLS protocol (`grpcs` or `grpc`)                                                                          | `false`       |
| `RootCertificate` | Specifies the path to the trusted server TLS certificate. If this parameter is set, the `UseTls` parameter will be forced to be `true` | Not defined   |


## Pooling

| Parameter         | Description                                                                                                 | Default value |
|-------------------|-------------------------------------------------------------------------------------------------------------|---------------|
| `MaxSessionPool`  | Specifies the maximum session pool size                                                                     | `100`         |


## Keepalive

| Parameter              | Description                                                                                                                                                                                                                                                                                                                             | Default value |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `KeepAlivePingDelay`   | The client sends a keep-alive ping to the server if it doesn't receive any frames on a connection for this period of time. This property is used together with `KeepAlivePingTimeout` to check whether the connection is broken. The delay value must be greater than or equal to `1` second. Set to `0` to disable the keep-alive ping | `10` seconds  |
| `KeepAlivePingTimeout` | Keep-alive pings are sent when a period of inactivity exceeds the configured `KeepAlivePingDelay` value. The client closes the connection if it doesn't receive any frames within the timeout. The timeout must be greater than or equal to `1` second. Set to `0` to disable the keep-alive ping timeout                               | `10` seconds  |

## Connection Builder Parameters {#connection-builder-parameters}

There are also additional parameters that do not participate in forming the connection string. These can only be specified using `YdbConnectionStringBuilder`:

| Parameter             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Default value                |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `LoggerFactory`       | This parameter accepts an instance that implements the [ILoggerFactory](https://learn.microsoft.com/en-us/dotnet/api/microsoft.extensions.logging.iloggerfactory) interface. The `ILoggerFactory` is a standard interface for logging factories in .NET. It is possible to use popular logging frameworks such as [NLog](https://github.com/NLog/NLog), [serilog](https://github.com/serilog/serilog), [log4net](https://github.com/apache/logging-log4net) | `NullLoggerFactory.Instance` |
| `CredentialsProvider` | An authentication provider that implements the `Ydb.Sdk.Auth.ICredentialsProvider`. Standard ways for authentication: <br> 1) `Ydb.Sdk.Auth.TokenProvider`. Token authentication for OAuth-like tokens. <br> 2) For Yandex Cloud specific authentication methods, consider using **[ydb-dotnet-yc](https://github.com/ydb-platform/ydb-dotnet-yc)**                                                                                                         | `Anonymous`                  |
| `ServerCertificates`  | Specifies custom server certificates used for TLS/SSL validation. This is useful when working with cloud providers (e.g., Yandex Cloud) that use custom root or intermediate certificates not trusted by default                                                                                                                                                                                                                                            | Not defined                  |
