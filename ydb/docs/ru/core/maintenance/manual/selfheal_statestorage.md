# Работа с Self Heal State Storage

В процессе работы кластеров могут выходить из строя узлы целиком, на которых работает {{ ydb-short-name }}.

Self Heal State Storage используется для сохранения работоспособности и отказоустойчивости подсистеи [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board) кластера, если невозможно быстро восстановить вышедшие из строя узлы, и автоматически увеличивать количество реплик этих подсистем при добавлении новых узлов в кластер.

Self Heal State Storage позволяет:

* обнаружить неисправные узлы кластера {{ ydb-short-name }};
* перенести реплики [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board) на другие узлы или добавить новые реплики.

Self Heal State Storage  включен по умолчанию.

Компонент {{ ydb-short-name }}, отвечающий за Self Heal State Storage, называется [CMS Sentinel](../../concepts/glossary.md#cms).

## Включение и выключение Self Heal State Storage {#on-off}

Вы можете включать и выключать Self Heal State Storage с помощью изменения конфигурации.
Для работы механизма требуется активация как [CMS Sentinel](../../concepts/glossary.md#cms), так и [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration).

1. Получить текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

2. Изменить конфигурационный файл `config.yaml`, поменяв значение параметра `state_storage_self_heal_config.enable` на `true` или на `false`:

    ```yaml
    config:
        cms_config:
            sentinel_config:
                state_storage_self_heal_config:
                    enable: true # Включение self heal state storage
    ```

    {% cut "Подробнее" %}
    Для работы механизма требуется активация как [CMS Sentinel](../../concepts/glossary.md#cms), так и [распределённой конфигурации](../../concepts/glossary.md#distributed-configuration). Убедитесь что они включены.

    Опция `state_storage_self_heal_config` отвечает за управление механизмом сохранения работоспособности и отказоустойчивости [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board)
    {% endcut %}

3. Обновить конфигурацию кластера с учетом выполненных изменений с помощью [ydb admin cluster config replace](../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config replace -f config.yaml
    ```
