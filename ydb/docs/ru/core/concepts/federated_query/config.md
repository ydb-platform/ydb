# Конфигурация внешних источников

Внешние источники настраиваиваются в секциях конфига [feature_flags](../../reference/configuration/feature_flags.md), `query_service_config`, а также необходимо настроить [коннектор](../architecture.md#connectors) по [инструкции](../../devops/deployment-options/manual/federated-queries/connector-deployment.md).

| Параметр | Назначение | Значение по умолчанию |
|----------|------------|-----------------------|
| `feature_flags.enable_external_data_sources` | Включение внешних источников | False (внешние источники недоступны) |
| `query_service_config.available_external_data_sources` | Список с разрешенным типами внешними источниками (применяется при `all_external_data_sources_are_available: false`)| Пустой |
| `query_service_config.all_external_data_sources_are_available` | Включение всех типов внешних источников.  | True |

Пример конфига (доступны все типы внешних источников):
```yaml
feature_flags: !inherit
    enable_external_data_sources: true
```

Пример конфига (доступны только ObjectStorage и ClickHouse):
```yaml
feature_flags: !inherit
    enable_external_data_sources: true
...
query_service_config:
    all_external_data_sources_are_available: false
    available_external_data_sources: !append
    - ObjectStorage
    - ClickHouse
```
