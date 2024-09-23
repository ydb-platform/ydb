# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` создаёт [пул ресурсов classifier](../../../../concepts/gloassary#resource-pool-classifier.md).

## Синтаксис

```yql
CREATE RESOURCE POOL CLASSIFIER <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

### Параметры
* `name` - имя создаваемого классификатора пула ресурсов. Должно быть уникально. * `имя` - не должно содержать в себе запрещенные символы для схемных объектов.
* `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` позволяет задавать значения параметров, определяющих поведение классификатора пула ресурсов. Поддерживаются следующие опции:
{% include [x](_includes/resource_pool_classifier_parameters.md) %}

## Замечания {#remarks}

Если в DDL для создания классификатора пула ресурсов не указан `RANK`, то по умолчанию будет проставлен `RANK` = (максимальный `RANK` из существующих классификаторов пула ресурсов) + 1000. Все `RANK` должны быть уникальны, чтобы иметь строго детерминированный порядок выбора пула ресурсов в случае конфликтующих условий. Такое поведение выбрано для возможности добавлять между уже созданными классификаторами пулов ресурсов другие классификаторы пулов ресурсов.

С ограничениями на число классификаторов можно ознакомиться на странице с [ограничениями базы данных](../../../../concepts/limits-ydb#resource_pool)

Также возможно иметь классификатор пулов ресурсов, который ссылаются на не существующий пул ресурсов или же у пользователя может не быть доступа к нему, в этом случае они пропускаются.

## Примеры {#examples}

```
CREATE RESOURCE POOL CLASSIFIER olap_classifier WITH (
    RANK=1000,
    RESOURCE_POOL="olap",
    MEMBERNAME="user1@domain"
)
```

В примере выше создается классификатор пула ресурсов с именем `olap_classifier`, который описывает перенаправление запросов от пользователя `user1@domain` в пул ресурсов c именем `olap`, а запросы всех остальных пользователей будут отправлены в пул ресурсов `default` при условии, что других классификаторов пулов ресурсов не существуют

## Разрешения

Требуется [разрешение](../yql/reference/syntax/grant#permissions-list) `ALL` на базу данных

Пример выдачи такого разрешения:
```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## См. также

* [Управление потреблением ресурсов](../../../dev/resource-pools-and-classifiers.md)
* [ALTER RESOURCE POOL CLASSIFIER](alter-resource-pool-classifier.md)
* [DROP RESOURCE POOL CLASSIFER](drop-resource-pool-classifier.md)