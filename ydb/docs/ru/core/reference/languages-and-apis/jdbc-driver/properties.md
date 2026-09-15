# Свойства JDBC-драйвера

JDBC-драйвер для {{ ydb-short-name }} поддерживает следующие конфигурационные свойства, которые можно указать в [JDBC URL](#jdbc-url) или передать через дополнительные свойства:

* `saKeyFile` — путь к файлу с ключом сервисной учётной записи (Service Account Key). {#saFileFile}

* `iamEndpoint` — IAM-эндпойнт для аутентификации с помощью ключа сервисной учётной записи (Service Account Key).

* `token` — токен для аутентификации. {#token}

* `tokenFile` — путь к файлу с токеном для аутентификации. {#tokenFile}

* `useMetadata` — использование режима аутентификации **Metadata**. Возможные значения: {#metadata}

    - `true` — использовать режим аутентификации **Metadata**;
    - `false` — не использовать режим аутентификации **Metadata**.

    Значение по умолчанию: `false`.

* `metadataURL` — эндпойнт для получения токена в режиме аутентификации **Metadata**.

* `localDatacenter` — название локального датацентра, в котором выполняется приложение.

* `secureConnectionCertificate` — путь к файлу с сертификатом CA для TLS-соединения.

{% note info %}

Значения свойства `saKeyFile`, `tokenFile` или `secureConnectionCertificate` могут быть как абсолютными от корня файловой системы, так и относительными от домашней директории пользователя. Примеры:

* `saKeyFile=~/mysakey1.json`

* `tokenFile=/opt/secret/token-file`

* `secureConnectionCertificate=/etc/ssl/cacert.cer`

{% endnote %}

## Примеры JDBC URL {#jdbc-url}

{% include notitle [примеры](_includes/jdbc-url-examples.md) %}