# ALTER RESOURCE POOL

`ALTER RESOURCE POOL` изменяет определение [пула ресурсов](../../../concepts/glossary.md#resource-pool.md).

## Синтаксис

### Изменение параметров

Синтаксис для изменения любого параметра пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL <name> SET (<key> = <value>);
```

`<key>` — имя параметра, `<value>` — его новое значение.

Например, такая команда включит ограничение на число параллельных запросов, равное 100:

```yql
ALTER RESOURCE POOL olap SET (CONCURRENT_QUERY_LIMIT = "100");
```

### Сброс параметров

Команда для сброса параметра пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL <name> RESET (<key>);
```

```<key>``` — имя параметра.

Например, такая команда сбросит настройки `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` для пула ресурсов:

```yql
ALTER RESOURCE POOL olap RESET (TOTAL_CPU_LIMIT_PERCENT_PER_NODE);
```

## Разрешения

Требуется [разрешение](grant.md#permissions-list) `ALTER SCHEMA` на пул ресурсов в директории `.metadata/workload_manager/pools`, пример выдачи такого разрешения:

```yql
GRANT 'ALTER SCHEMA' ON `.metadata/workload_manager/pools/olap_pool` TO `user1@domain`;
```

## Параметры

{% include [x](_includes/resource_pool_parameters.md) %}

## См. также

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool.md)
* [{#T}](drop-resource-pool.md)