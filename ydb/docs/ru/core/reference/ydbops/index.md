# Обзор утилиты ydbops

{% include [warning.md](_includes/warning.md) %}

Утилита `ydbops` облегчает выполнение объемных сценариев на кластерах {{ ydb-short-name }}. Утилита поддерживает кластеры, развернутые с помощью [Ansible](../../devops/ansible/index.md), [Kubernetes](../../devops/kubernetes/index.md) или [вручную](../../devops/manual/index.md).

## Смотрите также

* Для установки утилиты следуйте [инструкциям](install.md).
* Для настройки утилиты смотрите [справочник по конфигурации](configuration.md).
* Исходный код `ydbops` доступен [на GitHub](https://github.com/ydb-platform/ydbops).

## Поддерживаемые сценарии

- Выполнение [перезагрузки кластера](rolling-restart-scenario.md).

## Сценарии в разработке

- Запрос разрешения на вывод узлов {{ ydb-short-name }} на обслуживание без нарушения инвариантов модели отказа {{ ydb-short-name }}.
