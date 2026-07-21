# Self Heal State Storage

{% note warning %}

Инструкция относится только к кластерам {{ ydb-short-name }} с **конфигурацией V2** и **распределённой конфигурацией**. На кластерах с **конфигурацией V1** эти шаги и команды (в том числе получение конфигурации через `ydb admin cluster config fetch`) недоступны или не дадут ожидаемого результата. Альтернатив для V1 здесь не приводится — см. [Миграция на конфигурацию V2](../../devops/configuration-management/migration/migration-to-v2.md).

{% endnote %}

В процессе работы кластеров узлы, на которых работает {{ ydb-short-name }} могут выходить из строя целиком.

Self Heal State Storage обеспечивает сохранение работоспособности [подсистем распространения метаданных](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board) кластера, если невозможно быстро восстановить вышедшие из строя узлы, и автоматически увеличивать количество реплик этих подсистем при добавлении новых узлов в кластер.

Self Heal State Storage обеспечивает:

* обнаружение неисправных узлов кластера {{ ydb-short-name }};
* перенос реплик [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board) на другие узлы или добавление новых реплик.

Компонент Self Heal State Storage, является частью системы управления кластером [CMS Sentinel](../../concepts/glossary.md#cms).

## Включение и выключение Self Heal State Storage {#on-off}

Вы можете включать и выключать Self Heal State Storage с помощью изменения конфигурации.
Для работы механизма требуется активация как [CMS Sentinel](../../concepts/glossary.md#cms), так и [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration).

1. Получить текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

2. Изменить конфигурационный файл `config.yaml`, поменяв значение параметра `state_storage_self_heal_config.enable` на `true` или на `false`:

    ```yaml
    config:
        self_management_config:
            enabled: true # Включение распределённой конфигурации
        cms_config:
            sentinel_config:
                enable: true # Включение Sentinel
                state_storage_self_heal_config:
                    enable: true # Включение self heal state storage
    ```

    Для работы механизма требуется активация как [CMS Sentinel](../../concepts/glossary.md#cms), так и [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration). Убедитесь, что они включены.
    Подробнее о [миграции на конфигурацию V2 и включении распределённой конфигурации](../../devops/configuration-management/migration/migration-to-v2.md).
    При значении `true` у параметра `state_storage_self_heal_config.enable` включается механизм сохранения работоспособности и отказоустойчивости [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board).

3. Обновить конфигурацию кластера с учетом выполненных изменений с помощью [ydb admin cluster config replace](../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

## Управление автоматическим изменением конфигурации {#automatic-management}

Помимо общего включения/выключения Self Heal State Storage (параметр `state_storage_self_heal_config.enable`, см. [выше](#on-off)), в секции `self_management_config` конфигурационного файла `config.yaml` можно по отдельности управлять автоматическим изменением конфигурации каждой из подсистем распространения метаданных, а также ограничивать множество узлов, на которые Self Heal может переносить реплики.

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
| `automatic_state_storage_management` | `true` | Разрешает Self Heal автоматически изменять конфигурацию [State Storage](../../concepts/glossary.md#state-storage). При значении `false` Self Heal не должен изменять текущую конфигурацию State Storage. |
| `automatic_state_storage_board_management` | `true` | То же самое для [Board](../../concepts/glossary.md#board): разрешает или запрещает Self Heal автоматически изменять его конфигурацию. |
| `automatic_scheme_board_management` | `true` | То же самое для [Scheme Board](../../concepts/glossary.md#scheme-board): разрешает или запрещает Self Heal автоматически изменять его конфигурацию. |
| `state_storage_self_heal_allowed_nodes` | пустой список (без ограничений) | Список идентификаторов узлов, на которые Self Heal может переносить или на которых может добавлять реплики [State Storage](../../concepts/glossary.md#state-storage). Пустой список означает, что ограничений нет и могут быть использованы любые узлы кластера. |
| `state_storage_board_self_heal_allowed_nodes` | пустой список (без ограничений) | То же самое для реплик [Board](../../concepts/glossary.md#board). |
| `scheme_board_self_heal_allowed_nodes` | пустой список (без ограничений) | То же самое для реплик [Scheme Board](../../concepts/glossary.md#scheme-board). |

## Дополнительные параметры Self Heal State Storage {#self-heal-config-parameters}

В секции `cms_config.sentinel_config.state_storage_self_heal_config` конфигурационного файла `config.yaml` можно настроить дополнительные параметры работы механизма Self Heal State Storage, влияющие на то, как быстро реагирует механизм на изменения и сколько реплик подсистем распространения метаданных создаётся.

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
| `wait_for_config_step` | `60000000` (микросекунды, 60 секунд) | Время ожидания между промежуточными шагами применения новой конфигурации подсистем распространения метаданных (добавление/удаление групп колец, снятие флага `WriteOnly`, см. [Конфигурирование State Storage](../../devops/configuration-management/configuration-v2/state-storage-reconfiguration.md#metadata-subsystems-reconfig-rules)). Значение задаётся в микросекундах. |
| `relax_time` | `600000000` (микросекунды, 600 секунд) | Минимальный интервал между двумя последовательными срабатываниями Self Heal State Storage. Пока не прошло указанное время с момента предыдущего срабатывания, повторное изменение конфигурации не запускается, даже если обнаружены неисправные узлы. Значение задаётся в микросекундах. |
| `pileup_replicas` | `false` | Разрешает размещать реплики разных подсистем (State Storage, Board, Scheme Board) на одном и том же наборе узлов. При значении `false` Self Heal старается использовать разные узлы для реплик разных подсистем там, где это возможно; при значении `true` узлы, уже занятые под одну подсистему, могут повторно использоваться для остальных. |
| `override_replicas_in_ring_count` | `0` (рассчитывается автоматически) | Принудительно задаёт количество реплик в одном кольце. Если значение `0`, количество реплик в кольце вычисляется автоматически на основе `replicas_specific_volume` и числа доступных узлов. |
| `override_rings_count` | `0` (рассчитывается автоматически) | Принудительно задаёт количество колец в конфигурации. Если значение `0`, количество колец вычисляется автоматически на основе числа доступных узлов и топологии кластера. |
| `replicas_specific_volume` | `200` | Определяет, сколько узлов кластера должно приходиться на одну дополнительную реплику в кольце: одна дополнительная реплика добавляется на каждые `replicas_specific_volume` узлов в кластере. Используется при автоматическом расчёте количества реплик, если `override_replicas_in_ring_count` не задан (равен `0`). |

## Проверка результата {#verify-result}

Проверить, что изменения применились, можно  в разделе `CMS` в [Embedded UI](../../reference/embedded-ui/index.md) кластера (доступен на порту 8765): перейдите на вкладку `Sentinel` для просмотра статуса Sentinel и Self Heal State Storage.
