# query_service_config

Секция `query_service_config` описывает параметры работы {{ ydb-short-name }} с внешними источниками данных с использованием [федеративных запросов](../../concepts/query_execution/federated_query/index.md).

Если для доступа к нужному вам источнику требуется развернуть [коннектор](../../concepts/query_execution/federated_query/architecture.md#connectors), его необходимо также настроить по [инструкции](../../devops/deployment-options/manual/federated-queries/connector-deployment.md).

## Описание параметров

#|
|| **Параметр** | **Значение по умолчанию** | **Описание**  ||
|| `generic.connector.endpoint.host`
| `localhost`
|  Имя хоста коннектора.
||
|| `generic.connector.endpoint.port`
| `2130`
| TCP порт коннектора.
||
|| `generic.connector.use_ssl`
| `false`
| Использовать ли шифрование соединения. При размещении коннектора и динамического узла {{ ydb-short-name }} на одном сервере шифрованное соединение между ними не требуется, но при необходимости его можно включить.
||
|| `generic.connector.ssl_ca_crt`
| пустая строка
| Путь к сертификату CA, который используется для шифрования.
||
|| `generic.default_settings.name.UsePredicatePushdown`
| `false`
| Включает пушдаун предикатов во внешние источники данных: некоторые части SQL-запросов (например, фильтры) будут переданы на исполнение во внешний источник. Это позволит существенно снизить объёмы данных, передаваемых по сети источником данных в сторону федеративной {{ ydb-short-name }}, сэкономить её вычислительные ресурсы и значительно уменьшить время обработки федеративного запроса.
||
|| `available_external_data_sources`
| пустой список
| Список с разрешенными типами внешних источников. Применяется при `all_external_data_sources_are_available: false`.

Возможные значения:

* `ObjectStorage`;
* `ClickHouse`;
* `PostgreSQL`;
* `MySQL`;
* `Greenplum`;
* `MsSQLServer`;
* `Ydb`.

||
|| `all_external_data_sources_are_available`
| `false`
| Включение всех типов внешних источников. Если включено, настройка `available_external_data_sources` игнорируется.
||
|#

## Примеры {#examples}

### Включение внешних источников ClickHouse и MySQL

```yaml
query_service_config:
  generic:
    connector:
      endpoint:
        host: localhost                   # имя хоста, где развернут коннектор
        port: 2130                        # номер порта коннектора
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

### Включение всех типов внешних источников

```yaml
query_service_config:
  generic:
    connector:
      endpoint:
        host: localhost                   # имя хоста, где развернут коннектор
        port: 2130                        # номер порта коннектора
      use_ssl: false                      # флаг, включающий шифрование соединений
      ssl_ca_crt: "/opt/ydb/certs/ca.crt" # путь к сертификату CA
    default_settings:
    - name: UsePredicatePushdown
      value: "true"
  all_external_data_sources_are_available: true
```

## См. также

- [{#T}](../../devops/deployment-options/manual/federated-queries/index.md)
