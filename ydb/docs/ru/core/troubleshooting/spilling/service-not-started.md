# Spilling Service not started

Попытка использования спиллинга при выключенном Spilling Service. Это происходит, когда сервис спиллинга неправильно настроен или отключен в конфигурации.

## Диагностика

Проверьте конфигурацию сервиса спиллинга:

- Убедитесь, что [`table_service_config.spilling_service_config.local_file_config.enable`](../../reference/configuration/table_service_config.md#local-file-config-enable) установлен в `true`.

## Рекомендации

Для включения спиллинга:

1. Установите [`table_service_config.spilling_service_config.local_file_config.enable`](../../reference/configuration/table_service_config.md#local-file-config-enable): `true` в вашей конфигурации.

{% note info %}

Подробнее об архитектуре спиллинга см. в разделе [Архитектура спиллинга в {{ ydb-short-name }}](../../concepts/query_execution/spilling.md#architecture).

{% endnote %}
