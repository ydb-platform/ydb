# ADO.NET Connection Parameters

To connect to a database, the application provides a connection string that specifies parameters such as the host, user, password, and so on. Connection strings have the form `keyword1=value; keyword2=value;`. For more information, [see the official doc page on connection strings](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings).

All available connection parameters are defined as properties in the `YdbConnectionStringBuilder`.

Below are the connection string parameters that `Ydb.Sdk.Ado` supports.

## Basic Connection

| **Parameter** | **Description**                          | **Default value** |
|---------------|------------------------------------------|-------------------|
| Host          | Host of the {{ ydb-short-name }} server. | localhost         |
| Port          | Port of the {{ ydb-short-name }} server. | 2136              |
| Database      | Database path.                           | /local            |
| User          | User name.                               | Not defined       |
| Password      | User password.                           | Not defined       |

## Security and Encryption

| **Parameter**   | **Description**                                                                                                     | **Default value** |
|-----------------|---------------------------------------------------------------------------------------------------------------------|-------------------|
| UseTls          | Determines whether to establish a secure connection using TLS (`grpcs`); when false, TLS is not used (`grpc`).      | false             |
| RootCertificate | Path to a trusted root/intermediate server certificate file (PEM). When set, `UseTls` is automatically set to true. | Not defined       |


## Pooling

| **Parameter**      | **Description**                                                                                                          | **Default value** |
|--------------------|--------------------------------------------------------------------------------------------------------------------------|-------------------|
| MinPoolSize        | Minimum session pool size.                                                                                               | 0                 |
| MaxPoolSize        | Maximum session pool size.                                                                                               | 100               |
| SessionIdleTimeout | Time to wait (seconds) before closing idle sessions in the pool when the total number of sessions exceeds `MinPoolSize`. | 300               |

## Keepalive

| **Parameter**        | **Description**                                                                                                                                                                                                                                                                                                      | **Default value** |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| ConnectTimeout       | Time to wait (in seconds) when establishing a connection to the server. Must be greater than or equal to 0. Set 0 for infinite wait.                                                                                                                                                                                 | 10                |
| CreateSessionTimeout | Time to wait (in seconds) when creating a new session. Must be greater than or equal to 0. Set 0 for infinite wait.                                                                                                                                                                                                  | 5                 |
| KeepAlivePingDelay   | Idle period (in seconds) after which a keepalive ping is sent when there is no traffic. This property is used together with the `KeepAlivePingTimeout` parameter to check whether the connection is broken. The value must be greater than or equal to 1 second. Set 0 to disable the keep alive connectivity check. | 10                |
| KeepAlivePingTimeout | Time to wait (in seconds) after sending a keepalive ping; if no messages are received during this time, the connection is closed. The time to wait must be greater than or equal to 1 second. Set 0 to disable the timeout for maintaining the connection in idle mode.                                              | 10                |

## Performance

| **Параметр**                   | **Описание**                                                                                                                                                                                                                                                                    | **Значение по умолчанию** |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| EnableMultipleHttp2Connections | Determines whether to automatically scale HTTP/2 connections within a single gRPC channel to a single cluster node. This is rarely required, but can improve performance in scenarios with high load on a single node. When false, a single HTTP/2 connection per node is used. | false                     |
| MaxSendMessageSize             | Maximum size of outgoing messages in bytes. Note: a 64 MB limit applies on the server; larger messages are rejected with the `ResourceExhausted` error.                                                                                                                         | 67108864 (64 MB)          |
| MaxReceiveMessageSize          | Maximum size of incoming messages, bytes.                                                                                                                                                                                                                                       | 67108864 (64 MB)          |
| DisableServerBalancer          | Determines whether to disable session balancing on the server side; when false, balancing is enabled.                                                                                                                                                                           | false                     |

## Parameters useful for Serverless applications

| **Параметр**          | **Описание**                                                                                                                                                                                                                           | **Значение по умолчанию** |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| DisableDiscovery      | Determines whether to disable automatic node discovery and use a direct gRPC connection to the address from the connection string; when false, node discovery (discovery) is performed for client-side load balancing.                 | false                     |
| EnableImplicitSession | Determines whether implicit session management is enabled: the server creates and deletes sessions for each operation; on the client, a session pool is not used. **In this mode, interactive client transactions are not supported.** | false                     |

## Connection Builder Parameters {#connection-builder-parameters}

There are also additional parameters that do not participate in forming the connection string. These can only be specified using `YdbConnectionStringBuilder`:

| **Parameter**         | **Description**                                                                                                                                                                                                                                                                                                                                                                                                                                              | **Default value**            |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `LoggerFactory`       | This parameter accepts an instance that implements the [ILoggerFactory](https://learn.microsoft.com/en-us/dotnet/api/microsoft.extensions.logging.iloggerfactory) interface. The `ILoggerFactory` is a standard interface for logging factories in .NET. It is possible to use popular logging frameworks such as [NLog](https://github.com/NLog/NLog), [serilog](https://github.com/serilog/serilog), [log4net](https://github.com/apache/logging-log4net)  | `NullLoggerFactory.Instance` |
| `CredentialsProvider` | An authentication provider that implements the `Ydb.Sdk.Auth.ICredentialsProvider`. Standard ways for authentication: <br> 1) `Ydb.Sdk.Auth.TokenProvider`. Token authentication for OAuth-like tokens. <br> 2) For Yandex Cloud specific authentication methods, consider using **[ydb-dotnet-yc](https://github.com/ydb-platform/ydb-dotnet-yc)**                                                                                                          | `Anonymous`                  |
| `ServerCertificates`  | Specifies custom server certificates used for TLS/SSL validation. This is useful when working with cloud providers (e.g., Yandex Cloud) that use custom root or intermediate certificates not trusted by default                                                                                                                                                                                                                                             | Not defined                  |
