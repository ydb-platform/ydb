# CREATE RESOURCE POOL

`CREATE RESOURCE POOL` создаёт [пул ресурсов](../../../concepts/glossary.md#resource-pool.md).

## Синтаксис

```yql
CREATE RESOURCE POOL <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

* `name` - имя создаваемого пула ресурсов. Должно быть уникально. Не допускается запись в виде пути (т.е. не должно содержать `/`).
* `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` позволяет задать значения параметров, определяющих поведение пула ресурсов.

### Параметры {#parameters}

{% include [x](_includes/resource_pool_parameters.md) %}

## Замечания {#remark}

Запросы всегда выполняются в каком-либо пуле ресурсов. По умолчанию все запросы отправляются в пул ресурсов `default`, который создаётся автоматически и не может быть удалён — он всегда присутствует в системе.

Если для параметра `CONCURRENT_QUERY_LIMIT` установить значение 0, то все запросы, отправленные в этот пул, будут немедленно завершены со статусом `PRECONDITION_FAILED`.

## Разрешения

Требуется [разрешение](../yql/reference/syntax/grant#permissions-list) `CREATE TABLE` на директорию `.metadata/workload_manager/pools`, пример выдачи такого разрешения:

```yql
GRANT 'CREATE TABLE' ON `.metadata/workload_manager/pools` TO `user1@domain`;
```

## Примеры {#examples}

```yql
CREATE RESOURCE POOL olap WITH (
    CONCURRENT_QUERY_LIMIT=20,
    QUEUE_SIZE=1000,
    DATABASE_LOAD_CPU_THRESHOLD=80,
    RESOURCE_WEIGHT=100,
    QUERY_MEMORY_LIMIT_PERCENT_PER_NODE=80,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
)
```

В примере выше создаётся пул ресурсов со следующими ограничениями:

- Максимальное число параллельных запросов — 20.
- Максимальный размер очереди ожидания — 1000.
- При достижении загрузки базы данных в 80%, запросы перестают запускаться параллельно.
- Каждый запрос в пуле может потребить не более 80% доступной памяти на узле. Если запрос превысит этот лимит, он будет завершён со статусом `OVERLOADED`.
- Общее ограничение на доступный CPU для всех запросов в пуле на узле составляет 70%.
- Пул ресурсов имеет вес 100, который начинает работать только в случае переподписки.

## См. также

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool.md)
* [{#T}](drop-resource-pool.md)