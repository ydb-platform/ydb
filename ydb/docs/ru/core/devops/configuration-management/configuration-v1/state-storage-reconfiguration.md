# Конфигурирование State Storage

Применяется если нужно изменить [конфигурацию State Storage](../../../reference/configuration/domains_config.md) состоящую из [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), [Scheme Board](../../../concepts/glossary.md#scheme-board), на кластере {{ ydb-short-name }}.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

Для изменения конфигурации State Storage в кластере {{ ydb-short-name }} необходимо выполнить следующие шаги.

1. Получить текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

  ```bash
  ydb [global options...] admin cluster config fetch > config.yaml
  ```

  В результате выполнения данной команды текущая конфигурация будет сохранена в файле `config.yaml`

2. Внести требуемые изменения в секции `domains_config` конфигурационного файла `config.yaml`:

  Единая конфигурацию для компонент StateStorage, Board, SchemeBoard задается следующим образом:

  ```yaml
  config:
      domains_config:
          state_storage:
          - ring:
              nto_select: 5
              node: [1,2,3,4,5,6,7,8]
          ssid: 1
  ```

  Конфигурация для каждого типа отдельно задается следующим образом:

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

3. Применить новую конфигурацию кластера с помощью [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

  ```bash
  ydb [global options...] admin cluster config replace -f config.yaml
  ```

## Правила конфигурирования StateStorage

  Неправильная [конфигурация State Storage](../../../reference/configuration/domains_config.md) может привести к отказу кластера.
  Перечисленные ниже правила применяются как для параметра `state_storage` так и для раздельной конфигурации `explicit_state_storage_config`, `explicit_state_storage_board_config`, `explicit_scheme_board_config`

  1. Чтобы изменить конфигурацию State Storage без отказов кластера необходимо производить это путем добавления и удаления групп колец.
  1. Добавлять и удалять можно только группы колец с параметром `WriteOnly: true`.
  1. В новой конфигурации всегда должна присутствовать хотя бы одна группа колец из предыдущей конфигурации без параметра `WriteOnly`. Такая группа колец должна идти первой в списке.
  1. Если в разных группах колец используются одни и те же узлы кластера, добавьте параметр `ring_group_actor_id_offset:1` к группе колец. Значение должно быть уникальным среди групп колец. Этот параметр сделает уникальными идентификаторы реплик в данной группе колец, они не будут совпадать с идентификаторами из других групп, и это позволит разместить на одном узле кластера несколько реплик одного типа.
  1. Переход к новой конфигурации выполняется за 4 последовательных шага. На каждом шаге подготавливается новая конфигурация и применяется к кластеру. Только что созданные или готовые к удалению группы колец помечаются флагом `WriteOnly: true`. Это необходимо чтобы запросы на чтение обрабатывались уже развернутой группой колец, пока новая конфигурация распространится на необходимое количество узлов, создадутся новые реплики или удалятся старые. Поэтому, между шагами необходимо делать паузу как минимум в `1 минуту`.
    1. Добавляем новую группу колец с параметром `WriteOnly: true` соответствующую целевой конфигурации.
    1. Снимаем флаг `WriteOnly`.
    1. Выставляем флаг `WriteOnly: true` на группу колец соответствующую старой конфигурации, новую группу колец переносим в начало списка групп колец.
    1. Удаляем старую группу колец

## Пример

  Рассмотрим на примере текущей конфигурации:

  ```yaml
  config:
    domains_config:
      explicit_scheme_board_config:
        ring:
          nto_select: 5
          node: [1,2,3,4,5,6,7,8]
  ```

  и целевой конфигурации:

  ```yaml
  config:
    domains_config:
      explicit_scheme_board_config:
        ring:
          nto_select: 5
          node: [10,20,30,40,5,6,7,8]
  ```

  Мы хотим перенести часть реплик на другие узлы кластера.

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
