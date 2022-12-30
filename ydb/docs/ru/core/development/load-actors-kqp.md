# KqpLoad

Подает нагрузку на слой Query Processor и нагружает все компоненты кластера {{ ydb-short-name }}. Нагрузка, которую генерирует актор, аналогична нагрузке от подкоманды [workload](../reference/ydb-cli/commands/workload/index.md) {{ ydb-short-name }} CLI, но запускается изнутри кластера. Позволяет запускать два типа нагрузки:

* **Stock** — симулирует работу склада интернет-магазина: создает заказы из нескольких товаров, получает список заказов по клиенту.
* **Key-value** — использует БД как key-value хранилище.

Результатом теста является количество успешных транзакций в секунду, количество повторных попыток исполнения транзакций и количество ошибок.

{% include notitle [addition](../_includes/addition.md) %}

## Спецификация актора {#proto}

```proto
message TKqpLoad {
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
    optional bool IncreaseSessions = 11;
    optional bool DeleteTableOnFinish = 6;
    optional uint32 UniformPartitionsCount = 7;
    optional uint32 WorkloadType = 8;
    oneof Workload {
        TStockWorkload Stock = 9;
        TKvWorkload Kv = 10;
    }
}
```

## Параметры актора {#options}

Параметр | Описание
--- | ---
`TStockWorkload[]` | [Параметры нагрузки stock](#stock-workload).
`TKvWorkload[]` | [Параметры нагрузки key-value](#kv-workload).
`Tag` | Тег нагрузки. Если не задан, то тег будет присвоен автоматически.<br>Тип: `uint64`.<br>Необязательный.
`DurationSeconds` | Длительность теста в секундах.<br>Тип: `uint32`.<br>Обязательный.
`WindowDuration` | Размер окна для агрегации статистики.<br>Тип: `uint32`.<br>Обязательный.
`WorkingDir` | Путь директории, в которой будут созданы тестовые таблицы.<br>Тип: `string`.<br>Обязательный.
`NumOfSessions` | Количество параллельных потоков, подающих нагрузку. Каждый поток пишет в свою сессию.<br>Тип: `uint32`.<br>Обязательный.
`DeleteTableOnFinish` | Удалять ли таблицы после завершения работы нагрузки. Например, Оставлять таблицы может быть полезно в сценарии долгого предварительного создания большой таблицы и последующими (читающими) тестами.<br>Тип: `bool`.<br>Обязательный.
`UniformPartitionsCount` | Количество партиций, создаваемых в тестовых таблицах.<br>Тип: `uint32`.<br>Обязательный.
`WorkloadType` | Позволяет выбрать тип нагрузки.<br>Key-Value:<ul><li>`0` — UpsertRandom;</li><li>`1` — InsertRandom;</li><li>`2` — SelectRandom.</li></ul>Stok:<ul><li>`0` — InsertRandomOrder;</li><li>`1` — SubmitRandomOrder;</li><li>`2` — SubmitSameOrder;</li><li>`3` — GetRandomCustomerHistory;</li><li>`4` — GetCustomerHistory.</li></ul>Тип: `uint32`.<br>Обязательный.
`Workload` | Одно из значений:<ul><li>`TStockWorkload Stock` — тип нагрузки Stock;</li><li>`TKvWorkload Kv` — тип нагрузки key-value.</li></ul>

### TStockWorkload {#stock-workload}

Параметр | Описание
--- | ---
`ProductCount` | Количество видов товаров.<br>Тип: `uint64`.<br>Значение по умолчанию: `100`.<br>Необязательный.
`Quantity` | Количество товаров каждого вида на складе.<br>Тип: `uint64`.<br>Значение по умолчанию: `1000`.<br>Необязательный.
`OrderCount` | Первоначальное количество заказов в БД.<br>Тип: `uint64`.<br>Значение по умолчанию: `100`.<br>Необязательный.
`Limit` | Минимальное количество шардов для таблиц.<br>Тип: `uint64`.<br>Значение по умолчанию: `10`.<br>Необязательный.
`PartitionsByLoad` | Включение/выключение автошардирования.<br>Тип: `bool`.<br>Значение по умолчанию: `true`.<br>Необязательный.

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

Ниже приведена спецификация актора, который запускает stock-нагрузку БД `/slice/db`, выполняя простые UPSERT-запросы в 64 потока в течение 30 секунд. Перед началом работы создаются необходимые таблицы, после завершения они удаляются.

{% list tabs %}

- Embedded UI

  ```proto
  KqpLoad: {
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

{% endlist %}
