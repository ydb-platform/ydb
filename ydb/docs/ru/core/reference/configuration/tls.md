# Настройка TLS

{{ ydb-short-name }} поддерживает [шифрование данных при передаче по сети](../../security/encryption/data-in-transit.md), и каждый сетевой протокол может иметь свои настройки [TLS](https://ru.wikipedia.org/wiki/Transport_Layer_Security). Этот раздел документации предоставляет справочную информацию по настройке TLS в {{ ydb-short-name }}.

## Interconnect

[Интерконнект акторной системы {{ ydb-short-name }}](../../concepts/glossary.md#actor-system-interconnect) — это специализированный протокол для обмена данными между узлами {{ ydb-short-name }}.

Пример включения TLS для интерконнекта:

```yaml
interconnect_config:
   start_tcp: true
   encryption_mode: REQUIRED # или OPTIONAL
   path_to_certificate_file: "/opt/ydb/certs/node.crt"
   path_to_private_key_file: "/opt/ydb/certs/node.key"
   path_to_ca_file: "/opt/ydb/certs/ca.crt"
```

## {{ ydb-short-name }} в роли сервера

### gRPC

[ОсновнойAPI {{ ydb-short-name }}](../../reference/ydb-sdk/overview-grpc-api.md) основан на [gRPC](https://grpc.io/). Он используется для внешнего взаимодействия с клиентскими приложениями, которые работают напрямую с {{ ydb-short-name }} через [SDK](../../reference/ydb-sdk/index.md) или [CLI](../../reference/ydb-cli/index.md).

Пример включения TLS для gRPC API:

```yaml
grpc_config:
   cert: "/opt/ydb/certs/node.crt"
   key: "/opt/ydb/certs/node.key"
   ca: "/opt/ydb/certs/ca.crt"
```

### Протокол PostgreSQL

{{ ydb-short-name }} открывает отдельный сетевой порт для [протокола PostgreSQL](../../postgresql/intro.md). Этот протокол используется для внешнего взаимодействия с клиентскими приложениями, изначально разработанными для работы с [PostgreSQL](https://www.postgresql.org/).

Пример включения TLS для протокола PostgreSQL:

```yaml
local_pg_wire_config:
    ssl_certificate: "/opt/ydb/certs/node.crt"
```

### Протокол Kafka

{{ ydb-short-name }} открывает отдельный сетевой порт для [протокола Kafka](../../reference/kafka-api/index.md). Этот протокол используется для внешнего взаимодействия с клиентскими приложениями, изначально разработанными для работы с [Apache Kafka](https://kafka.apache.org/).

Пример включения TLS для протокола Kafka с использованием файла, содержащего как сертификат, так и закрытый ключ:

```yaml
kafka_proxy_config:
    ssl_certificate: "/opt/ydb/certs/node.crt"
```

Пример включения TLS для протокола Kafka с раздельными файлами сертификата и закрытого ключа:

```yaml
kafka_proxy_config:
    cert: "/opt/ydb/certs/node.crt"
    key: "/opt/ydb/certs/node.key"
```

### HTTP

{{ ydb-short-name }} открывает отдельный HTTP порт для работы [встроенного интерфейса](../../reference/embedded-ui/index.md), отображения [метрик](../../devops/manual/monitoring.md) и других вспомогательных команд.

Пример включения TLS на HTTP-порту, что делает его использования HTTPS:

```yaml
monitoring_config:
    monitoring_certificate_file: "/opt/ydb/certs/node.crt"
```

## {{ ydb-short-name }} в роли клиента

### LDAP

{{ ydb-short-name }} поддерживает [LDAP](../../concepts/auth.md#ldap) для аутентификации пользователей. Протокол LDAP имеет два варианта включения TLS.

Пример включения TLS для LDAP через расширение протокола `StartTls`:

```yaml
auth_config:
  ldap_authentication:
    use_tls:
      enable: true
      ca_cert_file: "/path/to/ca.pem"
      cert_require: DEMAND
  scheme: "ldap"
```

Пример включения TLS для LDAP через `ldaps`:

```yaml
auth_config:
  ldap_authentication:
    use_tls:
      enable: false
      ca_cert_file: "/path/to/ca.pem"
      cert_require: DEMAND
  scheme: "ldaps"
```

Подробнее этот механизм описан в [{#T}](index.md#ldap-auth-config).

### Федеративные запросы

[Федеративные запросы](../../concepts/federated_query/index.md) позволяют {{ ydb-short-name }} выполнять запросы к различным внешним источникам данных. Использование TLS при выполнении таких запросов контролируется параметром `USE_TLS` в запросах [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md). Изменения в серверной конфигурации не требуются.

### Трассировка

{{ ydb-short-name }} может отправлять данные [трассировки](../../reference/observability/tracing/setup.md) на внешний коллектор через gRPC.

Пример включения TLS для данных трассировки посредством указания протокола `grpcs://`:

```yaml
tracing_config:
  backend:
    opentelemetry:
      collector_url: grpcs://example.com:4317
      service_name: ydb
```

## Асинхронная репликация

[Асинхронная репликация](../../concepts/async-replication.md) синхронизирует данные между двумя базами данных {{ ydb-short-name }}, одна из которых выступает в роли клиента для другой. Использование TLS при такой коммуникации контролируется параметром `CONNECTION_STRING` в запросах [CREATE ASYNC REPLICATION](../../yql/reference/syntax/create-async-replication.md). Для TLS-соединений используйте протокол `grpcs://`. Изменения в серверной конфигурации не требуются.