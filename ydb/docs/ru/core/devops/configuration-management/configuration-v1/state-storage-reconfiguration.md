# Переконфигурирование State Storage

Применяется если нужно изменить конфигурацию [домена кластера State Storage](../../../reference/configuration/index.md#domains-state) состоящую из [StateStorage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), [SchemeBoard](../../../concepts/glossary.md#scheme-board), на кластере {{ ydb-short-name }}.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

## Изменение конфигурации StateStorage

1. Получить текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

2. Изменить конфигурационный файл `config.yaml`, поменяв значение параметра `domains_config`:

    Конфигурация может быть задана единая для всех компонент StateStorage, Board, SchemeBoard:

    ```yaml
    config:
        domains_config:
            state_storage:
            - ring:
                nto_select: 5
                node: [1,2,3,4,5,6,7,8]
            ssid: 1
    ```

    Либо раздельно для каждого типа:

    ```yaml
    config:
        domains_config:
            explicit_state_storage_config:
                ring_groups:
                - ring:
                    nto_select: 5
                    node: [1,2,3,4,5,6,7,8]
            explicit_state_storage_board_config:
                ring_groups:
                - ring:
                    nto_select: 5
                    node: [10,20,30,40,50,60,70,80]
            explicit_scheme_board_config:
                ring_groups:
                - ring:
                    nto_select: 5
                    node: [11,12,13,14,15,16,17,18]
    ```

3. Загрузить обновлённый конфигурационный файл в кластер с помощью [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config replace -f config.yaml
    ```

## Правила конфигурирования StateStorage
    Неправильная конфигурация [домена кластера State Storage](../../../reference/configuration/index.md#domains-state) может привести к отказу кластера.
    Данные правила применимы как для параметра `state_storage` так и для раздельной конфигурации `explicit_state_storage_config`, `explicit_state_storage_board_config`, `explicit_scheme_board_config`

    1. Чтобы изменить конфигурацию StateStorage без отказов кластера необходимо производить это путем добавления и удаления групп колец.
    1. Добавлять и удалять можно только группы колец с параметром `WriteOnly: true`.
    1. В новой конфигурации всегда должна присутствовать хотябы одна группа колец из предыдущей конфигурации без параметра `WriteOnly`. Такая группа колец должна идти первой в списке.
    1. Если в разных группах колец используются одни и те же узлы кластера, добавьте параметр `ring_group_actor_id_offset:42` к группе колец. Значение должно быть униальным среди групп колец.
    1. Переход к новой конфигурации должен происходить в 4 шага через промежуточные конфигурации. Между шагами необходимо делать паузу `1 минута`.
    1.1. Добавляем новую группу колец с параметром `WriteOnly: true` соответствующую целевой конфигурации.
    1.1. Снимаем флаг `WriteOnly`.
    1.1. Выставляем флаг `WriteOnly: true` на группу колец соответствующую старой конфигурации, новую группу колец переносим в начало списка групп колец.
    1.1. Удаляем старую группу колец

## Пример

    Рассмотрим на примере текущей конфигурации `explicit_scheme_board_config: { nto_select: 5, node: [1,2,3,4,5,6,7,8] }` и целевой конфигурации `explicit_scheme_board_config: { nto_select: 5, node: [10,20,30,40,5,6,7,8] }`. Мы хотим перенести часть реплик на другие узлы кластера.

**Шаг 0**
Текущая конфигурация `explicit_scheme_board_config`

```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring:
        nto_select: 5
        node: [1,2,3,4,5,6,7,8]
```

**Шаг 1**
На первом шаге первая группа колец должна соответствовать текущей конфигурации. Добавляем новую группу колец, которая соответствует целевой конфигурации и помечаем ее WriteOnly: true. Поскольку в новой конфигурации задействованы те же узлы кластера что и в старой необходимо добавить параметр `ring_group_actor_id_offset: 1`

```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
      - ring:
        nto_select: 5
        node: [1,2,3,4,5,6,7,8]
      - ring:
        nto_select: 5
        node: [10,20,30,40,5,6,7,8]
        write_only: true
        ring_group_actor_id_offset: 1
```

**Шаг 2**
Снимаем флаг `WriteOnly`.

```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
      - ring:
        nto_select: 5
        node: [1,2,3,4,5,6,7,8]
      - ring:
        nto_select: 5
        node: [10,20,30,40,5,6,7,8]
        ring_group_actor_id_offset: 1
```

**Шаг 3**
Делаем новую группу колец первой в списке. На старую конфигурацию выставляем флаг `WriteOnly: true`

```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
      - ring:
        nto_select: 5
        node: [10,20,30,40,5,6,7,8]
        ring_group_actor_id_offset: 1
      - ring:
        nto_select: 5
        node: [1,2,3,4,5,6,7,8]
        write_only: true
```

**Шаг 4**
Применяем на кластере целевую конфигурацию:

```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
      - ring:
        nto_select: 5
        node: [10,20,30,40,5,6,7,8]
        ring_group_actor_id_offset: 1
```
