# self_management_config

Секция `self_management_config` настраивает [распределённую конфигурацию V2](../../concepts/glossary.md#distributed-configuration) и автоматическое управление компонентами кластера.

## Синтаксис

```yaml
self_management_config:
  enabled: true
  automatic_static_group_management: true
  static_group_self_heal_allowed_nodes:
  - 1
  - 2
  - 3
```

## Параметры {#parameters}

| Параметр | Значение по умолчанию | Описание |
|----------|-----------------------|----------|
| `enabled` | `false` | Включает механизм распределённой конфигурации V2. |
| `automatic_static_group_management` | `false` | Разрешает распределённой конфигурации автоматически изменять размещение VDisk статической группы. |
| `static_group_self_heal_allowed_nodes` | `[]` | Ограничивает набор узлов, на которые SelfHeal может переносить VDisk статической группы. Укажите список идентификаторов узлов. Пустой список означает отсутствие ограничений. |
