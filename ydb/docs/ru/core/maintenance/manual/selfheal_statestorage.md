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

## Проверка результата {#verify-result}

Проверить, что изменения применились, можно  в разделе `CMS` в [Embedded UI](../../reference/embedded-ui/index.md) кластера (доступен на порту 8765): перейдите на вкладку `Sentinel` для просмотра статуса Sentinel и Self Heal State Storage.
