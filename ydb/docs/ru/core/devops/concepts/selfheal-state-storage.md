# SelfHeal подсистем распространения метаданных

SelfHeal подсистем распространения метаданных — механизм автоматического управления конфигурациями реплик [State Storage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board) и [SchemeBoard](../../concepts/glossary.md#scheme-board).

В конфигурации параметры механизма используют историческое имя `state_storage_self_heal_config`, которое относится ко всем трём подсистемам.

{% note warning %}

Инструкция относится только к кластерам {{ ydb-short-name }} с **конфигурацией V2** и **распределённой конфигурацией**. На кластерах с **конфигурацией V1** эти шаги и команды (в том числе получение конфигурации через `ydb admin cluster config fetch`) недоступны или не дадут ожидаемого результата. Альтернатив для V1 здесь не приводится — см. [Миграция на конфигурацию V2](../configuration-management/migration/migration-to-v2.md).

{% endnote %}

Механизм обнаруживает неисправности узлов и, если их нельзя быстро восстановить, переносит затронутые реплики на другие узлы. При росте кластера он также может автоматически увеличивать число реплик с учётом конфигурации и доступных узлов.

Механизм запускает Sentinel, компонент [системы управления кластером CMS](../../concepts/glossary.md#cms).

## Включение и выключение SelfHeal подсистем распространения метаданных {#on-off}

Вы можете включать и выключать SelfHeal подсистем распространения метаданных с помощью изменения конфигурации:

1. Получите текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

2. Измените конфигурационный файл `config.yaml`. Для этого поменяйте значение параметра `state_storage_self_heal_config.enable` на `true` или на `false`:

    ```yaml
    config:
        self_management_config:
            enabled: true # Включение распределённой конфигурации
        cms_config:
            sentinel_config:
                enable: true # Включение Sentinel
                state_storage_self_heal_config:
                    enable: true # Включение SelfHeal подсистем распространения метаданных
    ```

    {% note info %}

    Для работы механизма требуется активация как [CMS Sentinel](../../concepts/glossary.md#cms), так и [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration). Убедитесь, что они включены.

    См. подробнее: [Миграция на конфигурацию V2 и включение распределённой конфигурации](../configuration-management/migration/migration-to-v2.md).

    {% endnote %}

    При значении `true` у параметра `state_storage_self_heal_config.enable` включается механизм сохранения работоспособности и отказоустойчивости [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board).

3. Обновите конфигурацию кластера с учетом выполненных изменений с помощью [ydb admin cluster config replace](../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

## Управление автоматическим изменением конфигурации {#automatic-management}

Помимо общего [включения/выключения](#on-off) SelfHeal подсистем распространения метаданных (параметр `state_storage_self_heal_config.enable`), в секции `self_management_config` конфигурационного файла `config.yaml` вы можете по отдельности управлять автоматическим изменением конфигурации каждой из подсистем распространения метаданных, а также ограничивать множество узлов, на которые SelfHeal может переносить реплики.

```yaml
config:
    self_management_config:
        enabled: true
        automatic_state_storage_management: true
        automatic_state_storage_board_management: true
        automatic_scheme_board_management: true
        state_storage_self_heal_allowed_nodes: [1, 2, 3, 4, 5, 6, 7, 8]
        state_storage_board_self_heal_allowed_nodes: [1, 2, 3, 4, 5, 6, 7, 8]
        scheme_board_self_heal_allowed_nodes: [1, 2, 3, 4, 5, 6, 7, 8]
```

| Параметр | Значение по умолчанию | Описание |
|---|---|---|
| `automatic_state_storage_management` | `true` | Разрешает SelfHeal автоматически изменять конфигурацию [State Storage](../../concepts/glossary.md#state-storage). При значении `false` SelfHeal не изменяет текущую конфигурацию State Storage. |
| `automatic_state_storage_board_management` | `true` | То же самое для [Board](../../concepts/glossary.md#board): разрешает или запрещает SelfHeal автоматически изменять его конфигурацию. |
| `automatic_scheme_board_management` | `true` | То же самое для [SchemeBoard](../../concepts/glossary.md#scheme-board): разрешает или запрещает SelfHeal автоматически изменять его конфигурацию. |
| `state_storage_self_heal_allowed_nodes` | `[]` (без ограничений) | Список идентификаторов узлов, на которые SelfHeal может переносить или на которых может добавлять реплики [State Storage](../../concepts/glossary.md#state-storage). Пустой список означает, что ограничений нет и могут быть использованы любые узлы кластера. |
| `state_storage_board_self_heal_allowed_nodes` | `[]` (без ограничений) | То же самое для реплик [Board](../../concepts/glossary.md#board). |
| `scheme_board_self_heal_allowed_nodes` | `[]` (без ограничений) | То же самое для реплик [SchemeBoard](../../concepts/glossary.md#scheme-board). |

## Дополнительные параметры SelfHeal подсистем распространения метаданных {#self-heal-config-parameters}

В секции `cms_config.sentinel_config.state_storage_self_heal_config` конфигурационного файла `config.yaml` вы можете настроить дополнительные параметры работы механизма SelfHeal подсистем распространения метаданных. Они влияют на то, как быстро реагирует механизм на изменения и сколько реплик подсистем распространения метаданных создаётся. В примере ниже все параметры показаны со значениями по умолчанию:

```yaml
config:
    cms_config:
        sentinel_config:
            enable: true
            state_storage_self_heal_config:
                enable: true
                wait_for_config_step: 60000000
                relax_time: 600000000
                pileup_replicas: false
                override_replicas_in_ring_count: 0
                override_rings_count: 0
                replicas_specific_volume: 200
```

| Параметр | Значение по умолчанию | Описание |
|---|---|---|
| `wait_for_config_step` | `60000000` (микросекунды, 60 секунд) | Время ожидания между промежуточными шагами применения новой конфигурации подсистем распространения метаданных (добавление/удаление групп колец, снятие флага `WriteOnly`, см. [Конфигурирование State Storage](../configuration-management/configuration-v2/state-storage-reconfiguration.md#metadata-subsystems-reconfig-rules)). Значение задаётся в микросекундах. |
| `relax_time` | `600000000` (микросекунды, 600 секунд) | Минимальный интервал между двумя последовательными срабатываниями SelfHeal подсистем распространения метаданных. Пока не прошло указанное время с момента предыдущего срабатывания, повторное изменение конфигурации не запускается, даже если обнаружены неисправные узлы. Значение задаётся в микросекундах. |
| `pileup_replicas` | `false` | Разрешает размещать реплики разных подсистем (State Storage, Board, SchemeBoard) на одном и том же наборе узлов. При значении `false` SelfHeal старается использовать разные узлы для реплик разных подсистем там, где это возможно; при значении `true` узлы, уже занятые под одну подсистему, могут повторно использоваться для остальных. |
| `override_replicas_in_ring_count` | `0` (рассчитывается автоматически) | Принудительно задаёт количество реплик в одном кольце. Если значение `0`, количество реплик в кольце вычисляется автоматически на основе `replicas_specific_volume` и числа доступных узлов. |
| `override_rings_count` | `0` (рассчитывается автоматически) | Принудительно задаёт количество колец в конфигурации. Если значение `0`, количество колец вычисляется автоматически на основе числа доступных узлов и топологии кластера. |
| `replicas_specific_volume` | `200` | Определяет, сколько узлов кластера должно приходиться на одну дополнительную реплику в кольце: одна дополнительная реплика добавляется на каждые `replicas_specific_volume` узлов в кластере. Используется при автоматическом расчёте количества реплик, если `override_replicas_in_ring_count` не задан (равен `0`). |

## Проверка результата {#verify-result}

Проверить, что изменения применились, можно в разделе `CMS` в [{{ ydb-ui-name }}](../../reference/ydb-ui/index.md) кластера (доступен на порту 8765): перейдите на вкладку `Sentinel` для просмотра статуса Sentinel и SelfHeal подсистем распространения метаданных.
