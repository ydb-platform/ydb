# Конфигурация внешних источников

Параметры работы {{ ydb-short-name }} с внешними источниками данных с помощью функциональности федеративных запросов настраиваиваются в секциях конфига [feature_flags](../../reference/configuration/feature_flags.md), `query_service_config`.
Если для доступа к нужному вам источнику требуется развернуть коннектор, необходимо также настроить [коннектор](architecture.md#connectors) по [инструкции](../../devops/deployment-options/manual/federated-queries/connector-deployment.md).
Имеется возможность разрешить подключение {{ ydb-short-name }} либо ко всем типам внешних источников данных, либо только к определенным типам.

## Описание параметров

#|
|| Параметр | Описание | Значение по умолчанию ||
|| `feature_flags.enable_external_data_sources`
| Включение внешних источников.
| `false` (все внешние источники недоступны) ||
|| `query_service_config.generic.connector.endpoint.host`
| Имя хоста коннектора.
| `localhost`||
|| `query_service_config.generic.connector.endpoint.port`
| TCP порт коннектора.
| `2130`||
|| `query_service_config.generic.connector.use_ssl`
| Использование SSL.
 При совместном размещении коннектора и динамического узла {{ ydb-short-name }} на одном сервере установка шифрованных соединений между ними *не требуется*, но в случае необходимости вы можете включить шифрование.
| `false`||
|| `query_service_config.generic.connector.ssl_ca_crt`
| Путь до сертификата CA, использованного для подписи TLS-ключей коннектора.||
|| `query_service_config.available_external_data_sources`
| Список с разрешенными типами внешними источниками.
Применяется при `all_external_data_sources_are_available: false`.
| пустой список. ||
|| `query_service_config.all_external_data_sources_are_available`
| Включение всех типов внешних источников.
При true значение `available_external_data_sources` не используется.  
| `false` ||
|#

## Пример конфига (доступны только выбранные типы)

```yaml
feature_flags:
    enable_external_data_sources: true
...
query_service_config:
    generic:
        connector:
            endpoint:
                host: localhost                 # имя хоста, где развернут коннектор
                port: 2130                      # номер порта для слушающего сокета коннектора
            use_ssl: false                      # флаг, включающий шифрование соединений
            ssl_ca_crt: "/opt/ydb/certs/ca.crt" # (опционально) путь к сертификату CA
        default_settings:
        - name: DateTimeFormat
          value: string
        - name: UsePredicatePushdown
          value: "true"
    all_external_data_sources_are_available: false
    available_external_data_sources: !append
    - ObjectStorage
    - ClickHouse
    - PostgreSQL
    - MySQL
    - Greenplum
    - MsSQLServer
    - Ydb
```

## Пример конфига (доступны все типы внешних источников данных)

```yaml
feature_flags:
    enable_external_data_sources: true
...
query_service_config:
    generic:
        connector:
        ...
    all_external_data_sources_are_available: true
```

## См. также

- [Развёртывание YDB с функцией Federated Query](../../devops/deployment-options/manual/federated-queries/index.md)
