# SelfHeal статической группы

{% include [_](../_includes/experimental_v2.md) %}

При использовании [конфигурации V2](index.md) механизм [SelfHeal](../../../maintenance/manual/selfheal.md) может автоматически переносить VDisk статической группы с неисправных PDisk и восстанавливать отказоустойчивость группы.

{% note warning %}

На кластерах с [конфигурацией V1](../configuration-v1/config-overview.md) SelfHeal статической группы включить нельзя.

{% endnote %}

Общий механизм SelfHeal обнаруживает неисправный PDisk и инициирует перенос VDisk. Для динамических групп конфигурацию изменяет [Blob Storage Controller](../../../concepts/glossary.md#ds-controller), а конфигурацию статической группы изменяет [распределённая конфигурация](../../../concepts/glossary.md#distributed-configuration).

Чтобы разрешить распределённой конфигурации автоматически изменять статическую группу, включите параметр [`self_management_config.automatic_static_group_management`](../../../reference/configuration/self_management_config.md#parameters). По умолчанию этот параметр выключен.

## Включение и выключение SelfHeal статической группы {#on-off}

Для работы SelfHeal статической группы должны быть включены:

* [распределённая конфигурация](../../../concepts/glossary.md#distributed-configuration) V2 — [`self_management_config.enabled: true`](../../../reference/configuration/self_management_config.md#parameters);
* общий механизм SelfHeal, который [включён по умолчанию](../../../maintenance/manual/selfheal.md#on-off).

Параметр `self_management_config.enabled` включает саму распределённую конфигурацию. Параметр `self_management_config.automatic_static_group_management` отдельно разрешает автоматический перенос VDisk статической группы.

Чтобы включить или выключить автоматическое управление статической группой:

1. Получите текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. В конфигурационном файле `config.yaml` установите значения параметров `self_management_config.enabled` и `self_management_config.automatic_static_group_management`:

    ```yaml
    config:
      self_management_config:
        enabled: true
        automatic_static_group_management: true
    ```

    Значение `self_management_config.automatic_static_group_management: true` включает автоматическое управление статической группой, а `false` — выключает.

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

Пустой список означает отсутствие ограничений. Для каждого разрешённого узла выберите подходящий PDisk и проверьте, что его свободная ёмкость не меньше объёма, занятого VDisk на исходном PDisk, плюс эксплуатационный запас до порога предупреждения о заполнении. Единого числового значения запаса нет: ориентируйтесь на порог предупреждения, используемый в мониторинге вашего кластера. Размещение после переноса должно соответствовать [модели отказа](../../../concepts/topology.md#cluster-config).
