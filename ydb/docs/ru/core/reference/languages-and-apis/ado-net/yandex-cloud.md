# Yandex Cloud

[![Nuget](https://img.shields.io/nuget/v/Ydb.Sdk.Yc.Auth)](https://www.nuget.org/packages/Ydb.Sdk.Yc.Auth/)

## Установка

Чтобы использовать аутентификацию [Yandex Cloud](https://yandex.cloud/ru) в вашем .NET приложении, установите `Ydb.Sdk.Yc.Auth` [NuGet пакет](https://www.nuget.org/packages/Ydb.Sdk.Yc.Auth/):

```bash
dotnet add package Ydb.Sdk.Yc.Auth
```

Этот пакет предоставляет необходимые инструменты для аутентификации в сервисах Yandex Cloud, включая поддержку для [сервисных аккаунтов](https://yandex.cloud/ru/docs/iam/concepts/users/service-accounts) и [аутентификация на основе метаданных](https://yandex.cloud/ru/docs/compute/operations/vm-connect/auth-inside-vm).

## Аутентификация

Поддерживаемые Yandex Cloud методы аутентификации:

- `Ydb.Sdk.Yc.ServiceAccountProvider`. Аутентификация через сервисный аккаунт, пример использования:
    
    ```c#
    var saProvider = new ServiceAccountProvider(
        saFilePath: file, // Path to file with service account JSON info
        loggerFactory: loggerFactory
    );
    ```

- `Ydb.Sdk.Yc.MetadataProvider`. Аутентификация в сервисе метаданных, работает внутри облачных виртуальных машин Яндекса и облачных функций. Пример использования:
    
    ```c#
    var metadataProvider = new MetadataProvider(loggerFactory: loggerFactory);
    ```

## Сертификаты

Библиотека включает в себя сертификаты Yandex Cloud, который необходимы для подключения к Dedicated YDB:

```c#
var certs = Ydb.Sdk.Yc.YcCerts.GetYcServerCertificates();
```

## Как подключиться с ADO.NET

Чтобы установить безопасное соединение с {{ ydb-short-name }} с помощью ADO.NET, с требуемыми параметрами аутентификации и TLS. Ниже приведен пример:

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

## Пример

[ADO.NET подключение к Yandex Cloud](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples/src/YC)
