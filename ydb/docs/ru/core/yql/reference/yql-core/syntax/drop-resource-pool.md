# DROP RESOURCE POOL

`DROP RESOURCE POOL` удаляет [resource pool](../../../../concepts/gloassary#resource-pool).

## Синтаксис

```sql
DROP RESOURCE POOL <имя>
```

### Параметры

* `имя` - имя resource pool, подлежащего удалению.

## Замечания {#замечания}

* `имя` - не должно содержать в себе символ `/`, оно не привязано к иерархии схемы

## Разрешения

Требуется разрешение `REMOVE SCHEMA` на директорию `.metadata/workload_manager/pools`

## Примеры

Следующая команда удалит resource pool olap:

```sql
DROP RESOURCE POOL olap;
```

## См. также

* [CREATE RESOURCE POOL](create-resource-pool.md)
* [ALTER RESOURCE POOL](alter-resource-pool.md)