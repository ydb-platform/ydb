# SelfHeal статической группы

{% include [_](../_includes/experimental_v2.md) %}

При использовании [конфигурации V2](index.md) механизм [SelfHeal](../../../maintenance/manual/selfheal.md) может автоматически переносить VDisk статической группы с неисправных PDisk и восстанавливать отказоустойчивость группы.

Общий механизм SelfHeal обнаруживает неисправный PDisk и инициирует перенос VDisk. Для динамических групп конфигурацию изменяет Blob Storage Controller, а конфигурацию статической группы изменяет распределённая конфигурация.

Чтобы разрешить распределённой конфигурации автоматически изменять статическую группу, включите параметр [`automatic_static_group_management`](../../../reference/configuration/self_management_config.md#parameters). По умолчанию этот параметр выключен.

## Включение и выключение SelfHeal статической группы {#on-off}

Для работы SelfHeal статической группы должны быть включены:

* распределённая конфигурация V2 — [`self_management_config.enabled: true`](../../../reference/configuration/self_management_config.md#parameters);
* общий механизм SelfHeal, который [включён по умолчанию](../../../maintenance/manual/selfheal.md#on-off).

Чтобы включить или выключить автоматическое управление статической группой:

1. Получите текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. В конфигурационном файле `config.yaml` установите значение параметра `automatic_static_group_management`:

    ```yaml
    config:
      self_management_config:
        enabled: true
        automatic_static_group_management: true
    ```

    Значение `true` включает автоматическое управление статической группой, а `false` — выключает.

1. Примените новую конфигурацию с помощью команды [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

## Ограничение набора целевых узлов {#allowed-nodes}

По умолчанию SelfHeal может перенести VDisk статической группы на любой подходящий узел. Чтобы ограничить набор целевых узлов, укажите их идентификаторы в параметре [`static_group_self_heal_allowed_nodes`](../../../reference/configuration/self_management_config.md#parameters):

```yaml
config:
  self_management_config:
    enabled: true
    automatic_static_group_management: true
    static_group_self_heal_allowed_nodes:
    - 1
    - 2
    - 3
```

Пустой список означает отсутствие ограничений. На разрешённых узлах должны быть подходящие PDisk и достаточно свободного места для переноса VDisk с учётом модели отказа.
