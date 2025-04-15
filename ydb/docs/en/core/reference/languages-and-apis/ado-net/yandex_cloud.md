# Yandex Cloud

[![Nuget](https://img.shields.io/nuget/v/Ydb.Sdk.Yc.Auth)](https://www.nuget.org/packages/Ydb.Sdk.Yc.Auth/)

## Installation

To use Yandex Cloud authentication in your .NET application, install the `Ydb.Sdk.Yc.Auth` [nuget package](https://www.nuget.org/packages/Ydb.Sdk.Yc.Auth/):

```bash
dotnet add package Ydb.Sdk.Yc.Auth
```

This package provides the necessary tools for authenticating with Yandex Cloud services, including support for [service accounts](https://yandex.cloud/en/docs/iam/concepts/users/service-accounts) and [metadata-based](https://yandex.cloud/en/docs/compute/operations/vm-connect/auth-inside-vm) authentication.

## Authentication

Supported Yandex.Cloud authentication methods:

- `Ydb.Sdk.Yc.ServiceAccountProvider`. Service account authentication, sample usage:
    
    ```c#
    var saProvider = new ServiceAccountProvider(
        saFilePath: file, // Path to file with service account JSON info
        loggerFactory: loggerFactory
    );
    ```

- `Ydb.Sdk.Yc.MetadataProvider`. Metadata service authentication, works inside Yandex Cloud VMs and Cloud Functions. Sample usage:
    
    ```c#
    var metadataProvider = new MetadataProvider(loggerFactory: loggerFactory);
    ```

## Certificates

Library includes default Yandex Cloud server certificate, which is required for connectivity with dedicated YDB databases:

```c#
var certs = Ydb.Sdk.Yc.YcCerts.GetYcServerCertificates();
```

## How connect to ADO.NET

To establish a secure connection to {{ ydb-short-name }} using ADO.NET, configure YdbConnectionStringBuilder with the required authentication and TLS settings. Below are detailed example:

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