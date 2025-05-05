# DROP RESOURCE POOL

`DROP RESOURCE POOL` удаляет [пул ресурсов](../../../../concepts/gloassary#resource-pool).

## Синтаксис

```yql
DROP RESOURCE POOL <name>
```

### Параметры

* `name` - имя пула ресурсов, подлежащего удалению.

## Разрешения

Требуется [разрешение](../yql/reference/syntax/grant#permissions-list) `REMOVE SCHEMA` до пула в директории `.metadata/workload_manager/pools`, пример выдачи такого разрешения:

```yql
GRANT 'REMOVE SCHEMA`' ON `.metadata/workload_manager/pools` TO `user1@domain`;
```

## Примеры

Следующая команда удалит пул ресурсов с именем "olap":

```yql
DROP RESOURCE POOL olap;
```

## См. также

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool.md)
* [{#T}](alter-resource-pool.md)