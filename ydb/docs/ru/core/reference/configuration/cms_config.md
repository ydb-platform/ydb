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
