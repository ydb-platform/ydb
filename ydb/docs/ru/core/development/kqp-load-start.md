# KQP нагрузка

## Структура {#proto}

```proto
message TKqpLoadStart {
    message TStockWorkload {
        optional uint64 ProductCount = 1 [default = 100];
        optional uint64 Quantity = 2 [default = 1000];
        optional uint64 OrderCount = 3 [default = 100];
        optional uint64 Limit = 4 [default = 10];
        optional bool PartitionsByLoad = 5 [default = true];
    }
    message TKvWorkload {
        optional uint64 InitRowCount = 1 [default = 1000];
        optional bool PartitionsByLoad = 2 [default = true];
        optional uint64 MaxFirstKey = 3 [default = 18446744073709551615];
        optional uint64 StringLen = 4 [default = 8];
        optional uint64 ColumnsCnt = 5 [default = 2];
        optional uint64 RowsCnt = 6 [default = 1];
    }
    optional uint64 Tag = 1;
    optional uint32 DurationSeconds = 2;
    optional uint32 WindowDuration = 3;
    optional string WorkingDir = 4;
    optional uint32 NumOfSessions = 5;
    optional bool DeleteTableOnFinish = 6;
    optional uint32 UniformPartitionsCount = 7;
    optional uint32 WorkloadType = 8;
    oneof Workload {
        TStockWorkload Stock = 9;
        TKvWorkload Kv = 10;
    }
}
```

## Параметры {#options}

### TKqpLoadStart

Параметр | Описание
--- | ---
`TStockWorkload[]` | [link](#stock-workload).
`TKvWorkload[]` | [link](#kv-workload).
`Tag` | Тег нагрузки. Если не задан, то тег будет присвоен автоматически.<br>Тип: `uint64`.<br>Необязательный.
`DurationSeconds` | Длительность теста в секундах.<br>Тип: `uint32`.<br>Обязательный.
`WindowDuration` | Размер окна для агрегации статистики.<br>Тип: `uint32`.<br>Обязательный.
`WorkingDir` | Путь директории, в которой будут созданы тестовые таблицы.<br>Тип: `string`.<br>Обязательный.
`NumOfSessions` | Количество параллельных потоков, подающих нагрузку. Каждый поток пишет в свою сессию.<br>Тип: `uint32`.<br>Обязательный.
`DeleteTableOnFinish` | обязательный. Удалять ли таблицы после завершения работы нагрузки. Неудаление полезно в сценарии долгого предварительного создания большой таблицы и последующими (читающими) тестами.<br>Тип: `bool`.<br>Обязательный.
`UniformPartitionsCount` | Количество партиций, создаваемых в тестовых таблицах.<br>Тип: `uint32`.<br>Обязательный.
`WorkloadType` | Позволяет выбрать тип нагрузки.<br>Key-Value:<ul><li>`0` — UpsertRandom;</li><li>`1` — InsertRandom;</li><li>`2` — SelectRandom.</li></ul>Stok:<ul><li>`0` — InsertRandomOrder;</li><li>`1` — SubmitRandomOrder;</li><li>`2` — SubmitSameOrder;</li><li>`3` — GetRandomCustomerHistory;</li><li>`4` — GetCustomerHistory.</li></ul>Тип: `uint32`.<br>Обязательный.
`Workload` | Одно из значений:<ul><li>`TStockWorkload Stock` — ;</li><li>`TKvWorkload Kv` — .</li></ul>

### TStockWorkload {#stock-workload}

<!-- 
Параметр | Описание
--- | ---
optional uint64 ProductCount = 1 [default = 100];
optional uint64 Quantity = 2 [default = 1000];
optional uint64 OrderCount = 3 [default = 100];
optional uint64 Limit = 4 [default = 10];
optional bool PartitionsByLoad = 5 [default = true];
 -->

### TKvWorkload {#kv-workload}

Параметр | Описание
--- | ---
`InitRowCount` | До начала нагрузки нагружающий актор запишет в таблицу указанное количество строк.<br>Тип: `uint64`.<br>Значение по умолчанию: — `1000`.<br>Необязательный.
`PartitionsByLoad` | Добавлять ли к тестовой таблице аттрибут `AUTO_PARTITIONING_BY_LOAD`.<br>Тип: `bool`.<br>Значение по умолчанию: — `true`.<br>Необязательный.
`MaxFirstKey` | Ограничивает диапазон значений ключа. Создан для того, чтобы при читающей нагрузке случайные ключи попадал в диапазон ключей таблицы.<br>Тип: `uint64`.<br>Значение по умолчанию: — `18446744073709551615`.<br>Необязательный.
`StringLen` | Длина строки `value`.<br>Тип: `uint64`.<br>Значение по умолчанию: — `8`.<br>Необязательный.
`ColumnsCnt` | Сколько столбцов использовать в таблице.<br>Тип: `uint64`.<br>Значение по умолчанию: — `2` (столбцы для key и value).<br>Необязательный.
`RowsCnt` | Сколько строк вставлять/читать в одном SQL запросе<br>Тип: `uint64`.<br>Значение по умолчанию: — `1`.<br>Необязательный.

## Примеры {#example}

Нагрузка базы `/slice/db` типа STOCK простыми UPSERT-запросами в 64 потока длительностью 30 секунд. До начала работы создает необходимые для работы таблицы, после нагрузки удаляет их.

```proto
KqpLoadStart: {
    DurationSeconds: 30
    WindowDuration: 1
    WorkingDir: "/slice/db"
    NumOfSessions: 64
    UniformPartitionsCount: 1000
    DeleteTableOnFinish: 1
    WorkloadType: 0
    Stock: {
        ProductCount: 100
        Quantity: 1000
        OrderCount: 100
        Limit: 10
    }
}
```

{% include notitle [addition](../_includes/addition.md) %}
