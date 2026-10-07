# Spilling Service not started

Спиллинг выключен, но при выполнении запроса закончилась память. Когда спиллинг выключен, {{ ydb-short-name }} не может выгрузить промежуточные данные на диск, что приводит к сбою запроса при исчерпании доступной памяти.

## Диагностика

Проверьте конфигурацию сервиса спиллинга:

- Убедитесь, что [`table_service_config.spilling_service_config.local_file_config.enable`](../../reference/configuration/table_service_config.md#local-file-config-enable) установлен в `true`.

## Рекомендации

Для исправления этой ошибки включите спиллинг:

1. Установите [`table_service_config.spilling_service_config.local_file_config.enable`](../../reference/configuration/table_service_config.md#local-file-config-enable): `true` в вашей конфигурации.

Подробнее о включении спиллинга см. в разделе [Включение](../../reference/configuration/table_service_config.md#enable).

{% note info %}

Подробнее об архитектуре спиллинга см. в разделе [Архитектура спиллинга в {{ ydb-short-name }}](../../concepts/query_execution/spilling.md#architecture).

{% endnote %}
