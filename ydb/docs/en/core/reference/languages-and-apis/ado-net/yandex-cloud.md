# Connection ADO.NET to Yandex Cloud

## Installation

To use [Yandex Cloud](https://yandex.cloud/en) authentication in your .NET application, install the `Ydb.Sdk.Yc.Auth` [NuGet package](https://www.nuget.org/packages/Ydb.Sdk.Yc.Auth/):

```bash
dotnet add package Ydb.Sdk.Yc.Auth
```

This package provides the necessary tools for authenticating with Yandex Cloud services.

## Authentication

Supported Yandex.Cloud authentication methods:

- `Ydb.Sdk.Yc.ServiceAccountProvider`. [Service account](https://yandex.cloud/en/docs/iam/concepts/users/service-account) authentication, sample usage:

    ```c#
    var saProvider = new ServiceAccountProvider(
        saFilePath: file, // Path to file with service account JSON info
        loggerFactory: loggerFactory
    );
    ```

- `Ydb.Sdk.Yc.MetadataProvider`. [Metadata service](https://yandex.cloud/en/docs/compute/operations/vm-connect/auth-inside-vm) authentication, works inside Yandex Cloud VMs and Cloud Functions. Sample usage:

    ```c#
    var metadataProvider = new MetadataProvider(loggerFactory: loggerFactory);
    ```

## Certificates

The library includes default Yandex Cloud server certificates, which are required for connectivity with dedicated {{ ydb-short-name }} databases:

```c#
var certs = Ydb.Sdk.Yc.YcCerts.GetYcServerCertificates();
```

## How to Connect with ADO.NET

To establish a secure connection to {{ ydb-short-name }} using ADO.NET, configure `YdbConnectionStringBuilder` with the required authentication and TLS settings. Below is a detailed example:

```c#
var builder = new YdbConnectionStringBuilder
{
    // More settings ...
    UseTls = true,
    Port = 2135,
    CredentialsProvider = saProvider, // For service account
    ServerCertificates = YcCerts.GetYcServerCertificates() // custom certificates Yandex Cloud
};
```

## Example

[ADO.NET connect to Yandex Cloud](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples/src/YC)
