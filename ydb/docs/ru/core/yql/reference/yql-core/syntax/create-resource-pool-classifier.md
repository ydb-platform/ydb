# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` создаёт [пул классификаторов ресурсов](../../../../concepts/gloassary#resource-pool-classifier.md).

## Синтаксис

```yql
CREATE RESOURCE POOL CLASSIFIER <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

- `name` — имя создаваемого классификатора пула ресурсов. Должно быть уникальным. Имя не должно содержать символы, запрещённые для схемных объектов.
- `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — позволяет задавать значения параметров, определяющих поведение классификатора пула ресурсов.

### Параметры

{% include [x](_includes/resource_pool_classifier_parameters.md) %}

## Замечания {#remarks}

Если в DDL для создания классификатора пула ресурсов не указан `RANK`, то по умолчанию ему будет присвоено значение $RANK = MAX(existing\_ranks) + 1000$. Все значения `RANK` должны быть уникальными, чтобы обеспечить строго детерминированный порядок выбора пула ресурсов в случае конфликтующих условий. Такое поведение выбрано для возможности добавлять новые классификаторы пулов ресурсов между уже существующими.

Также возможно наличие классификатора, который ссылается на несуществующий пул ресурсов или к которому у пользователя нет доступа. В таком случае такие классификаторы будут пропускаться.

С ограничениями на число классификаторов можно ознакомиться на странице [ограничений](../../../../concepts/limits-ydb#resource_pool).

## Разрешения

Требуется [разрешение](../yql/reference/syntax/grant#permissions-list) `ALL` на базу данных

Пример выдачи такого разрешения:

```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Примеры {#examples}

```yql
CREATE RESOURCE POOL CLASSIFIER olap_classifier WITH (
    RANK=1000,
    RESOURCE_POOL="olap",
    MEMBER_NAME="user1@domain"
)
```

В примере выше создаётся классификатор пула ресурсов с именем `olap_classifier`, который направляет запросы от пользователя `user1@domain` в пул ресурсов с именем `olap`. Запросы от всех остальных пользователей будут отправляться в пул ресурсов `default`, при условии, что других классификаторов пулов ресурсов не существует.

## См. также

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)
