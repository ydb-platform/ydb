# Spilling Service not started

Попытка использования спиллинга при выключенном Spilling Service. Это происходит, когда сервис спиллинга неправильно настроен или отключен в конфигурации.

## Диагностика

Проверьте конфигурацию сервиса спиллинга:

- Убедитесь, что `table_service_config.spilling_service_config.local_file_config.enable` установлен в `true`.
- Просмотрите логи {{ ydb-short-name }} на наличие ошибок запуска сервиса спиллинга.

## Рекомендации

Для включения спиллинга:

1. Установите `table_service_config.spilling_service_config.local_file_config.enable: true` в вашей конфигурации.

{% note info %}

Подробнее об архитектуре спиллинга см. в разделе [Архитектура спиллинга в {{ ydb-short-name }}](../../concepts/spilling.md#architecture).

{% endnote %}
