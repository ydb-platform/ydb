# cms_config

[Cluster Management System (CMS)](../../concepts/glossary.md#cms) — компонент {{ ydb-short-name }}, с помощью которого можно выполнять [безопасное обслуживание кластера {{ ydb-short-name }}](../../devops/concepts/maintenance-without-downtime.md), например, обновлять его версию или заменять сломавшиеся диски без потери доступности. Поведение CMS конфигурируется в секции `cms_config` конфигурации {{ ydb-short-name }}.

## Синтаксис

```yaml
cms_config:
  tenant_limits:
    disabled_nodes_limit: 2
    disabled_nodes_ratio_limit: 5
  cluster_limits:
    disabled_nodes_limit: 3
    disabled_nodes_ratio_limit: 5
  disable_maintenance: true
```

## Параметры

| Параметр                                      | Значение по умолчанию | Описание                                                                                                                                                                                                                                                                                                                          |
|-----------------------------------------------|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `tenant_limits.disabled_nodes_limit`          | -                     | Максимальное количество [узлов базы данных](../../concepts/glossary.md#database-node), которые могут быть одновременно недоступны или заблокированы.                                                                                                                          |
| `tenant_limits.disabled_nodes_ratio_limit`    | `13`                  | Максимальный процент [узлов базы данных](../../concepts/glossary.md#database-node), которые могут быть одновременно недоступны или заблокированы.                                                                                                                          |
| `cluster_limits.disabled_nodes_limit`         | -                     | Максимальное количество узлов [кластера](../../concepts/glossary.md#cluster), которые могут быть одновременно недоступны или заблокированы.                                                                           |
| `cluster_limits.disabled_nodes_ratio_limit`   | `13`                  | Максимальный процент узлов [кластера](../../concepts/glossary.md#cluster), которые могут быть одновременно недоступны.                                                                                                                             |
| `disable_maintenance`                         | `false`               | Флаг [приостанавливает](../../devops/concepts/maintenance-without-downtime.md#disable-maintenance) новые работы по обслуживанию кластера.       |
| `sentinel_config.enable` | `true` | Включает Sentinel, компонент CMS, который следит за состоянием дисков и инициирует [SelfHeal хранилища](../../devops/concepts/selfheal-storage.md). |
| `sentinel_config.dry_run` | `false` | Если `true`, Sentinel не отправляет изменения статусов дисков в Blob Storage Controller. |
| `sentinel_config.evict_vdisks_status` | `FAULTY` | Выбирает [режим вывода VDisk](#evict-vdisks-status) перед обслуживанием хоста: `FAULTY`, `MAINTENANCE` или `DISABLED`. Значение `UNKNOWN` эквивалентно `FAULTY`. |

## Режим вывода VDisk перед обслуживанием {#evict-vdisks-status}

Параметр `sentinel_config.evict_vdisks_status` определяет, как Sentinel инициирует перенос [VDisk](../../concepts/glossary.md#vdisk) при запросе CMS с признаком `EvictVDisks`. В команде `ydbd cms request` этот признак задаётся флагом `--evict-vdisks`. Само изменение параметра не создаёт запрос на обслуживание.

| Значение | Поведение |
| --- | --- |
| `FAULTY` | Sentinel помечает [PDisk](../../concepts/glossary.md#pdisk) обслуживаемого хоста как `FAULTY`, чтобы SelfHeal перенёс VDisk на другие диски. Эти PDisk учитываются как неисправные при проверках отказоустойчивости группы хранения. |
| `MAINTENANCE` | Sentinel устанавливает отдельный статус обслуживания `LONG_TERM_MAINTENANCE_PLANNED`. Исправный PDisk остаётся `ACTIVE` и не учитывается как неисправный только из-за планового обслуживания. SelfHeal переносит VDisk с соблюдением [условий планового переноса](../../devops/concepts/selfheal-storage.md#planned-maintenance). |
| `DISABLED` | CMS отклоняет запросы с `EvictVDisks`. Обычный мониторинг неисправностей дисков продолжает работать. |

Если параметр не задан или равен `UNKNOWN`, используется режим `FAULTY`.

Чтобы включить режим планового обслуживания, добавьте в секцию `config` [конфигурации кластера](../../devops/configuration-management/index.md) следующие настройки:

```yaml
cms_config:
  sentinel_config:
    enable: true
    dry_run: false
    evict_vdisks_status: MAINTENANCE
```

Пример показывает фрагмент конфигурации. Для переноса VDisk также должен быть [включён SelfHeal хранилища](../../devops/concepts/selfheal-storage.md#on-off).

{% note warning "Совместимость конфигурации" %}

Перед откатом на версию {{ ydb-short-name }}, которая не поддерживает `MAINTENANCE`, удалите параметр `evict_vdisks_status` из YAML-конфигурации или замените его значение на `FAULTY`. Такая версия не сможет запуститься с неизвестным значением перечисления.

{% endnote %}
