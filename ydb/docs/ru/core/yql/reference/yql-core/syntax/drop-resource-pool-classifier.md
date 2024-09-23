# DROP RESOURCE POOL CLASSIFIER

`DROP RESOURCE POOL CLASSIFIER` удаляет [классификатор пула ресурсов](../../../../concepts/gloassary#resource-pool-classifier).

## Синтаксис

```yql
DROP RESOURCE POOL CLASSIFIER <name>
```

### Параметры

* `name` - имя классификатора пула ресурсов, подлежащего удалению.

## Разрешения

Требуется [разрешение](../yql/reference/syntax/grant#permissions-list) `ALL` на базу данных

Пример выдачи такого разрешения:
```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Примеры

Следующая команда удалит классификатор пула ресурсов с именем olap_classifier:

```yql
DROP RESOURCE POOL CLASSIFIER olap_classifier;
```

## См. также

* [Управление потреблением ресурсов](../../../dev/resource-pools-and-classifiers.md)
* [CREATE RESOURCE POOL CLASSIFIER](create-resource-pool-classifier.md)
* [ALTER RESOURCE POOL CLASSIFIER](alter-resource-pool-classifier.md)