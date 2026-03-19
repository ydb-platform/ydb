# Обработка стримминговых запросов

{% note info %}

Ниже речь о **потоковой выдаче результата** запроса по gRPC (стрим строк/чанков), в том числе при использовании `StreamExecuteScanQuery` и аналогов. Отдельно в продукте существуют SQL-[потоковые запросы](../../concepts/streaming-query.md) (stream processing) — см. [{#T}](../../concepts/streaming-query.md). О лимитах результата и пагинации: [{#T}](../../concepts/limits-ydb.md#query), [{#T}](../../dev/paging.md). Ретраи при чтении стрима — [{#T}](../../recipes/ydb-sdk/retry.md).

{% endnote %}

## Проблема

Стриминговые запросы в {{ ydb-short-name }} позволяют обрабатывать большие объемы данных, получая результаты частями через gRPC-стрим. Однако при работе с такими запросами возникают специфические проблемы:

1. **Дорогой ретрай всего запроса** - при ошибке перезапуск всего стрима означает потерю уже обработанных данных и повторную обработку с начала
2. **Техническая сложность ретрая с места ошибки** - отсутствие встроенного механизма для продолжения обработки с последнего успешного чанка
3. **Потеря данных при разрыве соединения** - при сетевых сбоях или рестарте нод клиент теряет уже полученные, но не обработанные данные
4. **Высокие накладные расходы** - постоянное поддержание соединения и обработка чанков требует дополнительных ресурсов

## Решение

### Стратегия 1: Использование встроенных механизмов {{ ydb-short-name }} SDK

Все официальные SDK {{ ydb-short-name }} предоставляют встроенные средства для работы со стримминговыми запросами с автоматической обработкой ошибок.

### Стратегия 2: Чекпоинты для длительных запросов

Для длительных стриминговых операций используйте механизм чекпоинтов, который позволяет сохранять состояние обработки и продолжать с места остановки.

### Стратегия 3: Пагинация по первичному ключу

Для больших выборок данных используйте пагинацию с OFFSET/LIMIT вместо полного стрима, что позволяет делать ретраи разумного размера.


{% list tabs %}

- Go

  {% cut "Плохой пример" %}
  ```go
// ПЛОХО: Прямое выполнение стриммингового запроса без обработки ошибок
func processStreamBad(db *sql.DB) error {
    rows, err := db.Query("SELECT * FROM large_table")
    if err != nil {
        return err
    }
    defer rows.Close()
    
    for rows.Next() {
        var id int
        var data string
        if err := rows.Scan(&id, &data); err != nil {
            // При ошибке сканирования теряем весь прогресс
            return err
        }
        processData(id, data)
    }
    return rows.Err()
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
// ХОРОШО: Использование встроенного ретрая и чекпоинтов
func processStreamGood(db *sql.DB) error {
    return retry.DoTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
        // Используем пагинацию для контроля размера ретрая
        offset := 0
        batchSize := 1000
        
        for {
            query := fmt.Sprintf("SELECT id, data FROM large_table ORDER BY id LIMIT %d OFFSET %d",
                batchSize, offset)
            
            rows, err := tx.QueryContext(ctx, query)
            if err != nil {
                return err
            }
            
            var processed int
            for rows.Next() {
                var id int
                var data string
                if err := rows.Scan(&id, &data); err != nil {
                    rows.Close()
                    return err
                }
                processData(id, data)
                processed++
            }
            
            if err := rows.Close(); err != nil {
                return err
            }
            
            if processed < batchSize {
                break // Все данные обработаны
            }
            offset += batchSize
        }
        return nil
    }, retry.WithIdempotent(true))
}
  ```
  {% endcut %}

- C++

  {% cut "Плохой пример" %}
  ```cpp
// ПЛОХО: Прямая работа со стримом без обработки ошибок
void ProcessStreamBad(NYdb::NTable::TTableClient client) {
    auto query = "SELECT * FROM large_table";
    
    auto result = client.StreamExecuteScanQuery(query).GetValueSync();
    if (!result.IsSuccess()) {
        throw std::runtime_error("Query failed");
    }
    
    auto stream = result.GetResultSetParser();
    while (stream.TryNextRow()) {
        auto id = stream.ColumnParser("id").GetInt64();
        auto data = stream.ColumnParser("data").GetString();
        ProcessData(id, data);
        
        // Нет обработки ошибок внутри стрима
    }
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```cpp
// ХОРОШО: Использование RetryQuerySync с обработкой чекпоинтов
void ProcessStreamGood(NYdb::NQuery::TQueryClient client) {
    auto result = client.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
        // Загружаем последний чекпоинт
        auto lastId = LoadLastCheckpoint();
        auto query = TStringBuilder() <<
            "SELECT id, data FROM large_table " <<
            "WHERE id > " << lastId << " " <<
            "ORDER BY id LIMIT 1000";
        
        auto result = session.ExecuteQuery(
            query,
            NYdb::NQuery::TTxControl::BeginTx(
                NYdb::NQuery::TTxSettings::SerializableRW()
            ).CommitTx()
        ).GetValueSync();
        
        if (!result.IsSuccess()) {
            return result;
        }
        
        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        
        int64_t maxProcessedId = lastId;
        while (parser.TryNextRow()) {
            auto id = parser.ColumnParser("id").GetInt64();
            auto data = parser.ColumnParser("data").GetOptionalUtf8();
            
            ProcessData(id, data);
            maxProcessedId = std::max(maxProcessedId, id);
        }
        
        // Сохраняем прогресс
        SaveCheckpoint(maxProcessedId);
        
        return result;
    });
    
    if (!result.IsSuccess()) {
        std::cerr << "Query failed: " << result.GetIssues().ToString() << std::endl;
    }
}
  ```
  {% endcut %}

{% endlist %}


## Рекомендации

1. **Используйте встроенные механизмы SDK** - все официальные SDK {{ ydb-short-name }} имеют оптимизированную логику ретраев
2. **Реализуйте чекпоинты** - сохраняйте состояние обработки для возможности продолжения с места остановки
3. **Разбивайте на пачки** - для больших данных используйте пагинацию вместо полного стрима
4. **Настройте таймауты** - устанавливайте разумные таймауты для избежания вечных ожиданий
5. **Мониторьте метрики** - отслеживайте производительность и ошибки стриминговых запросов

## Особенности {{ ydb-short-name }} Streaming

{{ ydb-short-name }} Streaming предоставляет дополнительные возможности для потоковой обработки:
- **Чекпоинты** - автоматическое сохранение состояния обработки
- **Восстановление из чекпоинтов** - продолжение обработки после сбоев
- **Поддержка оконных функций** - агрегация данных в скользящих окнах
- **Интеграция с YDS** - работа с потоками данных Яндекс Data Streams

Для сложных сценариев потоковой обработки рекомендуется использовать специализированный {{ ydb-short-name }} Streaming вместо обычных стриминговых запросов.
