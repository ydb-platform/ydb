# Конфигурация внешних источников

Внешние источники настраиваиваются в секциях конфига [feature_flags](../../reference/configuration/feature_flags.md), `query_service_config`, а также необходимо настроить [коннектор](architecture.md#connectors) по [инструкции](../../devops/deployment-options/manual/federated-queries/connector-deployment.md).

Имеется возможность разрешить как все типы внешних источников, так и только определенные типы.

#|
|| Параметр | Описание ||
|| `feature_flags.enable_external_data_sources`
| Включение внешних источников.
Значение по умолчанию: false (внешние источники недоступны) ||
|| `query_service_config.available_external_data_sources`
| Список с разрешенным типами внешними источниками.
Применяется при `all_external_data_sources_are_available: false`.
Значение по умолчанию: пустой список. ||
|| `query_service_config.all_external_data_sources_are_available`
| Включение всех типов внешних источников.
При true, значение `available_external_data_sources` не используется.  
Значение по умолчанию: false ||
|#

Пример конфига (доступны все типы внешних источников):

```yaml
feature_flags: !inherit
    enable_external_data_sources: true
```

Пример конфига (доступны только выбранные типы):

```yaml
feature_flags: !inherit
    enable_external_data_sources: true
...
query_service_config:
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
