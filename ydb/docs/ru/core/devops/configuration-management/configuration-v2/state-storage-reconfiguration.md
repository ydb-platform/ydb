# Конфигурирование подсистем распространения метаданных State Storage, Board, Scheme Board

Применяется если нужно изменить [конфигурацию подсистем распространения метаданных](../../../reference/configuration/domains_config.md#domains-state) состоящую из [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), [Scheme Board](../../../concepts/glossary.md#scheme-board), на кластере {{ ydb-short-name }}.

При использовании конфигурации V2 подсистемы распространения метаданных частично поддерживаются автоматически за счёт механизма Self Heal — см. [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md) (перенос и добавление реплик при изменениях топологии). Чтобы отключить это поведение, установите `state_storage_self_heal_config.enable` в `false`, как описано в том же разделе. Отключение не обязательно для нижеприведённых шагов: конфигурация после `ydb admin cluster config replace` применится до очередного срабатывания автоматики.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

Для ручного изменения конфигурации State Storage в кластере {{ ydb-short-name }} необходимо выполнить следующие шаги.

1. Получить текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

  ```bash
    ydb [global options...] admin cluster config fetch --v2-internal-state > config.yaml
  ```

  В результате выполнения данной команды текущая конфигурация будет сохранена в файле `config.yaml`

2. Внести требуемые изменения в секции `domains_config` конфигурационного файла `config.yaml`:
    Правила изменения секции `domains_config` см. в разделе [Правила конфигурирования State Storage](#metadata-subsystems-reconfig-rules).

3. Применить новую конфигурацию кластера с помощью [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

  ```bash
  ydb [global options...] admin cluster config replace -f config.yaml
  ```

## Правила конфигурирования State Storage {#metadata-subsystems-reconfig-rules}

  Перечисленные ниже правила применяются к [`state_storage`](../../../reference/configuration/domains_config.md#domains-state) и к раздельным полям `explicit_state_storage_config`, `explicit_state_storage_board_config`, `explicit_scheme_board_config` в секции [`domains_config`](../../../reference/configuration/domains_config.md) файла `config.yaml` (см. [Конфигурация State Storage](../../../reference/configuration/domains_config.md#domains-state)). Ключи `explicit_*` соответствуют по отдельности [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board) и [Scheme Board](../../../concepts/glossary.md#scheme-board).

  Под кольцом в конфигурации понимается блок `ring` внутри элемента списка `ring_groups` (см. Конфигурация State Storage).

  Изменение конфигурации производится в несколько шагов. Сначала добавляется новая группа колец, состоящая из правильно выбранных узлов (в соответствии с моделью отказа), а затем старая группа колец удаляется.

  Чтобы избежать недоступности кластера, удаление и добавление групп колец выполняйте строго в последовательности шагов, описанной ниже.

  1. Чтобы изменить конфигурацию подсистем распространения метаданных без недоступности кластера необходимо производить это путем добавления и удаления групп колец.
  1. Добавлять и удалять можно только группы колец с параметром `WriteOnly: true`.
  1. В новой конфигурации всегда должна присутствовать хотя бы одна группа колец из предыдущей конфигурации без параметра `WriteOnly`. Такая группа колец должна идти первой в списке.
  1. Если в разных группах колец используются одни и те же узлы кластера, добавьте параметр `ring_group_actor_id_offset:1` к группе колец. Значение должно быть уникальным среди групп колец.

     Этот параметр сделает уникальными идентификаторы реплик в данной группе колец, они не будут совпадать с идентификаторами из других групп, и это позволит разместить на одном узле кластера несколько реплик одного типа.
  1. Переход к новой конфигурации выполняется за 4 последовательных шага. На каждом шаге подготавливается новая конфигурация и применяется к кластеру.

     Только что созданные или готовые к удалению группы колец помечаются флагом `WriteOnly: true`. Это необходимо, чтобы запросы на чтение обрабатывались уже развернутой группой колец, пока новая конфигурация распространится на необходимое количество узлов, создадутся новые реплики или удалятся старые.

     Поэтому между шагами необходимо делать паузу как минимум в `1 минуту`.

     - Добавляем новую группу колец с параметром `WriteOnly: true` соответствующую целевой конфигурации.
     - Снимаем флаг `WriteOnly`.
     - Выставляем флаг `WriteOnly: true` на группу колец соответствующую старой конфигурации, новую группу колец переносим в начало списка групп колец.
     - Удаляем старую группу колец.

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
На первом шаге подготовьте [`ring_groups`](../../../reference/configuration/domains_config.md#domains-state), следуя [правилам конфигурирования](#metadata-subsystems-reconfig-rules): первая группа колец совпадает с **текущей** конфигурацией из листингов выше, вторая — с **целевой** и помечена `WriteOnly: true`. Параметр `ring_group_actor_id_offset` указывайте так, как описано в тех же правилах, если наборы узлов у групп совпадают.

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

## Проверка результата {#verify-result}

Проверить, что изменения применились, можно в разделе `CMS` в [Embedded UI](../../../reference/embedded-ui/index.md) кластера (доступен на порту 8765): перейдите на вкладку `Tablets` и убедитесь по репликам таблеток подсистем метаданных, что конфигурация подхватилась.
