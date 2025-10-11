# Конфигурация внешних источников

Секция `query_service_config` описываем параметры работы {{ ydb-short-name }} с внешними источниками данных с помощью функциональности [федеративных запросов](../../concepts/federated_query/index.md)

Если для доступа к нужному вам источнику требуется развернуть коннектор, необходимо также настроить [Коннектор](../../concepts/federated_query/architecture.md#connectors) по [инструкции](../../devops/deployment-options/manual/federated-queries/connector-deployment.md).

Параметры `query_service_config.all_external_data_sources_are_available` и `query_service_config.available_external_data_sources` используются в случаях, когда требуется подключить только некоторые из внешних источников данных.

## Описание параметров

#|
|| `query_service_config.generic.connector.endpoint.host`
| `localhost`
| `Коннектор` - имя хоста.
||
|| `query_service_config.generic.connector.endpoint.port`
| `2130`
| `Коннектор` - TCP порт.
||
|| `query_service_config.generic.connector.use_ssl`
| `false`
| `Коннектор` — необходимость использования шифрования. При размещении коннектора и динамического узла {{ ydb-short-name }} на одном сервере шифрованное соединение между ними не требуется, но при необходимости его можно включить.
||
|| `query_service_config.generic.connector.ssl_ca_crt`
|
| `Коннектор` — путь к сертификату CA, используемому для шифрования.
||
|| `query_service_config.generic.default_settings.name.UsePredicatePushdown`
| `false`
| Включает пушдаун предикатов во внешние источники данных: некоторые части SQL-запросов (например, фильтры) будут переданы на исполнение во внешний источник. Это позволит существенно снизить объёмы данных, передаваемых по сети источником данных в сторону федеративной YDB, сэкономить её вычислительные ресурсы и значительно уменьшить время обработки федеративного запроса.
||
|| `query_service_config.available_external_data_sources`
| пустой список
| Список с разрешенными типами внешними источниками. Возможные значения: ObjectStorage, ClickHouse, PostgreSQL, MySQL, Greenplum, MsSQLServer, Ydb
Применяется при `all_external_data_sources_are_available: false`.
||
|| `query_service_config.all_external_data_sources_are_available`
| `false`
| Включение всех типов внешних источников.
При true значение `available_external_data_sources` не используется.
||
|#

## Пример конфига (из внешних источников данных доступен только ClickHouse и MySQL)

```yaml
query_service_config:
    generic:
        connector:
            endpoint:
                host: localhost                 # имя хоста, где развернут коннектор
                port: 2130                      # номер порта коннектора
            use_ssl: false                      # флаг, включающий шифрование соединений
            ssl_ca_crt: "/opt/ydb/certs/ca.crt" # путь к сертификату CA
        default_settings:
        - name: UsePredicatePushdown
          value: "true"
    all_external_data_sources_are_available: false
    available_external_data_sources:
    - ClickHouse
    - MySQL
```

## Пример конфига (доступны все типы внешних источников данных)

```yaml
query_service_config:
    generic:
        connector:
            endpoint:
                host: localhost                 # имя хоста, где развернут коннектор
                port: 2130                      # номер порта коннектора
            use_ssl: false                      # флаг, включающий шифрование соединений
            ssl_ca_crt: "/opt/ydb/certs/ca.crt" # путь к сертификату CA
        default_settings:
        - name: UsePredicatePushdown
          value: "true"
    all_external_data_sources_are_available: true
```

## См. также

- [Развёртывание YDB с функцией Federated Query](../../devops/deployment-options/manual/federated-queries/index.md)
