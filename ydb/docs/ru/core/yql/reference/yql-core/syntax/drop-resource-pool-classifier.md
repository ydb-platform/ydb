# DROP RESOURCE POOL CLASSIFIER

`DROP RESOURCE POOL CLASSIFIER` удаляет [resource pool classifier](../../../../concepts/gloassary#resource-pool-classifier).

## Синтаксис

```yql
DROP RESOURCE POOL CLASSIFIER <name>
```

### Параметры

* `name` - имя resource pool classifier, подлежащего удалению.

## Замечания {#remarks}

* `name` - не должно содержать в себе запрещенные символы для схемных объектов, но оно не привязано к иерархии схемы

## Разрешения

Требуется [разрешение](../yql/reference/syntax/grant#permissions-list) `ALL` на базу данных

Пример выдачи такого разрешения:
```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Примеры

Следующая команда удалит resource pool classifier с именем olap_classifier:

```yql
DROP RESOURCE POOL CLASSIFIER olap_classifier;
```

## См. также

* [Управление потреблением ресурсов](../../../dev/resource-pools-and-classifiers.md)
* [CREATE RESOURCE POOL CLASSIFIER](create-resource-pool-classifier.md)
* [ALTER RESOURCE POOL CLASSIFIER](alter-resource-pool-classifier.md)