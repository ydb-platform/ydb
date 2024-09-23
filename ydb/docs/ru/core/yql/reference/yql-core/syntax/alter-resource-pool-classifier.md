# ALTER RESOURCE POOL CLASSIFIER

`ALTER RESOURCE POOL CLASSIFIER` изменяет определение [классификатора пула ресурсов](../../../../concepts/gloassary#resource-pool-classifier.md).

## Параметры
{% include [x](_includes/resource_pool_classifier_parameters.md) %}

## Изменение параметров

Синтаксис для изменения любого параметра классификатора пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> SET (<key> = <value>);
```

`<key>` — имя параметра, `<value>` — его новое значение.

Например, такая команда изменит пользователя для которого применяется правило:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier SET (MEMBER_NAME = "user2@domain");
```

## Сброс параметров

Команда для сброса параметра классификатора пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> RESET (<key>);
```

`<key>` — имя параметра.

Например, такая команда сбросит настройки `MEMBER_NAME`:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier RESET (MEMBER_NAME);
```

## Разрешения

Требуется [разрешение](../yql/reference/syntax/grant#permissions-list) `ALL` на базу данных

Пример выдачи такого разрешения:
```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## См. также

* [Управление потреблением ресурсов](../../../dev/resource-pools-and-classifiers.md)
* [CREATE RESOURCE POOL CLASSIFIER](create-resource-pool-classifier.md)
* [DROP RESOURCE POOL CLASSIFIER](drop-resource-pool-classifier.md)