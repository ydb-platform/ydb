# ALTER RESOURCE POOL CLASSIFIER

`ALTER RESOURCE POOL CLASSIFIER` изменяет определение [классификатора пула ресурсов](../../../concepts/glossary.md#resource-pool-classifier.md).

## Синтаксис

### Изменение параметров

Синтаксис для изменения любого параметра классификатора пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> SET (<key> = <value>);
```

`<key>` — имя параметра, `<value>` — его новое значение.

Например, такая команда изменит пользователя, для которого применяется правило:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier SET (MEMBER_NAME = "user2@domain");
```

### Сброс параметров

Команда для сброса параметра классификатора пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> RESET (<key>);
```

`<key>` — имя параметра.

Например, такая команда сбросит настройку `MEMBER_NAME`:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier RESET (MEMBER_NAME);
```

## Разрешения

Требуется [разрешение](grant.md#permissions-list) `ALL` на базу данных, пример выдачи такого разрешения:

```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Параметры

{% include [x](_includes/resource_pool_classifier_parameters.md) %}

## См. также

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)