# KQP нагружалка

## Описание
```proto
    message TKqpLoadStart {
        message TStockWorkload { // ссылка на описание StockWorkload? Параметры as-is скопированны оттуда
            optional uint64 ProductCount = 1 [default = 100];
            optional uint64 Quantity = 2 [default = 1000];
            optional uint64 OrderCount = 3 [default = 100];
            optional uint64 Limit = 4 [default = 10];
            optional bool PartitionsByLoad = 5 [default = true];
        }
        message TKvWorkload {
            optional uint64 InitRowCount = 1 [default = 1000]; // необязательный. До начала нагрузки нагружающий актор запишет
                                                               // InitRowCount строк в таблицу
            optional bool PartitionsByLoad = 2 [default = true];  // необязательный. Добавлять ли к тестовой таблице аттрибут AUTO_PARTITIONING_BY_LOAD
            optional uint64 MaxFirstKey = 3 [default = 18446744073709551615]; // необязательный. Ограничивает диапазон значений ключа.
                                                // Создан для того, чтобы при читающей нагрузке случайные ключи попадал в диапазон ключей таблицы
            optional uint64 StringLen = 4 [default = 8]; // необязательный. Длина строки value
            optional uint64 ColumnsCnt = 5 [default = 2]; // необязательный. Сколько столбцов использовать в таблице.
                                                          // По умолчанию 2 - один для key, второй для value.
            optional uint64 RowsCnt = 6 [default = 1]; // необязательный. Сколько строк вставлять/читать в одном SQL запросе
        }
        optional uint64 Tag = 1; // необязательный. Если не задан, то тег будет присвоен автоматически
        optional uint32 DurationSeconds = 2; // обязательный. Длительность теста
        optional uint32 WindowDuration = 3; // обязательный. Размер окна для агрегации статистики
        optional string WorkingDir = 4; // обязательный. Путь до директории, где будут созданы тестовые таблицы
        optional uint32 NumOfSessions = 5; // обязательный. Количество параллельных потоков, подающих нагрузку. Каждый поток пишет в свою сессию
        optional bool DeleteTableOnFinish = 6; // обязательный. Удалять ли таблицы после завершения работы нагрузки. Неудаление полезно в сценарии
                                            // долгого предварительного создания большой таблицы и последующими (читающими) тестами.
        optional uint32 UniformPartitionsCount = 7; // обязательный. Количество партиций, создаваемых в тестовых таблицах
        optional uint32 WorkloadType = 8; // обязательный. Позволяет выбрать тип нагрузки.
                                    //  В случае KV:
                                    //      0 - UpsertRandom,
                                    //      1 - InsertRandom,
                                    //      2 - SelectRandom
                                    //  В случае Stock:
                                    //      0 - InsertRandomOrder,
                                    //      1 - SubmitRandomOrder,
                                    //      2 - SubmitSameOrder,
                                    //      3 - GetRandomCustomerHistory,
                                    //      4 - GetCustomerHistory
        oneof Workload {
            TStockWorkload Stock = 9;
            TKvWorkload Kv = 10;
        }
    }
```

## Примеры

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