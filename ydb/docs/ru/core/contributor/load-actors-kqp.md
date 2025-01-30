# KqpLoad

Тестирует производительности кластера {{ ydb-short-name }} в целом, нагружая все компоненты через слой Query Processor. Нагрузка, аналогична нагрузке от подкоманды [workload](../reference/ydb-cli/commands/workload/index.md) {{ ydb-short-name }} CLI, но запускается изнутри кластера.

Вы можете запустить два вида нагрузки:

* **Stock** — симулирует работу склада интернет-магазина: создает заказы из нескольких товаров, получает список заказов по клиенту.
* **Key-value** — использует БД как key-value хранилище.

Перед началом работы создаются необходимые таблицы, после завершения они удаляются.

## Параметры актора {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

Параметр | Описание
--- | ---
`DurationSeconds` | Продолжительность нагрузки в секундах.
`WindowDuration` | Размер окна для агрегации статистики.
`WorkingDir` | Путь директории, в которой будут созданы тестовые таблицы.
`NumOfSessions` | Количество параллельных потоков, подающих нагрузку. Каждый поток пишет в свою сессию.
`DeleteTableOnFinish` | Если  `False`, то созданные таблицы не удаляются после завершения работы нагрузки. Может быть полезно в случае, когда при первом запуске актора создается большая таблица, а при последующих выполняются запросы к ней.
`UniformPartitionsCount` | Количество партиций, создаваемых в тестовых таблицах.
`WorkloadType` | Тип нагрузки.<br/>В случае Stoсk:<ul><li>`0` — InsertRandomOrder;</li><li>`1` — SubmitRandomOrder;</li><li>`2` — SubmitSameOrder;</li><li>`3` — GetRandomCustomerHistory;</li><li>`4` — GetCustomerHistory.</li></ul>В случае Key-Value:<ul><li>`0` — UpsertRandom;</li><li>`1` — InsertRandom;</li><li>`2` — SelectRandom.</li></ul>
`Workload` | Вид нагрузки.<br/>`Stock`:<ul><li>`ProductCount` — количество видов товаров.</li><li>`Quantity` — количество товаров каждого вида на складе.</li><li>`OrderCount` — первоначальное количество заказов в БД.</li><li>`Limit` — минимальное количество шардов для таблиц.</li></ul>`Kv`:<ul><li>`InitRowCount` — до начала нагрузки нагружающий актор запишет в таблицу указанное количество строк.</li><li>`StringLen` — длина строки `value`.</li><li>`ColumnsCnt` — сколько столбцов использовать в таблице.</li><li>`RowsCnt` — сколько строк вставлять или читать в одном SQL запросе.</li></ul>

## Примеры {#example}

Следующий актор запускает stock-нагрузку БД `/slice/db`, выполняя простые UPSERT-запросы в `64` потока в течение `30` секунд.

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

Результатом теста является количество успешных транзакций в секунду, количество повторных попыток исполнения транзакций и количество ошибок.
