# Параллельное выполнение запросов на одной сессии и ошибка `SESSION_BUSY`

## Проблема

Сессия в {{ ydb-short-name }} — это однопоточный актор, который не поддерживает параллельное выполнение запросов. При попытке выполнить два или более запросов одновременно на одной сессии возникает ошибка **SESSION_BUSY** ([код 400190](../../reference/ydb-sdk/ydb-status-codes.md#session-busy) в [{#T}](../../reference/ydb-sdk/ydb-status-codes.md)).

### Причины возникновения ошибки:

1. **Параллельные запросы** — явная попытка выполнить несколько запросов одновременно на одной сессии
2. **Неполное чтение результатов** — если запрос выполнен, но результаты не прочитаны до конца, сессия остается "занятой"

### Последствия

Поскольку ошибка `SESSION_BUSY` является **повторяемой** с короткой задержкой, то при частом возникновении может привести к инвалидации сессии в пуле, из-за чего
снизится производительность, поскольку SDK будет постоянно ходить по сети к {{ ydb-short-name }}, чтобы пересоздать сессию.

## Решение

- Не пытайтесь выполнять параллельные запросы на одной сессии
- Всегда читайте результаты запросов до конца перед выполнением следующих операций

## Примеры кода


{% list tabs %}

- C++

  {% cut "Плохой пример" %}
  ```cpp
// НЕПРАВИЛЬНО: Запуск параллельных запросов на одной сессии
NYdb::NQuery::TQueryClient client(driver);

auto status = client.RetryQuerySync([userId](NYdb::NQuery::TSession session) -> NYdb::TStatus {
    // thread 1
    auto future1 = session.ExecuteQuery(...); // async, возвращает TFuture

    // thread 2
    auto future2 = session.ExecuteQuery(...); // SESSION_BUSY!!!

    auto result1 = future1.GetValueSync();
    auto result2 = future2.GetValueSync();

    return result2.GetStatus();
});
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```cpp
// ПРАВИЛЬНО: Запуск параллельных запросов на разных сессиях
NYdb::NQuery::TQueryClient client(driver);

// thread 1
auto status1 = client.RetryQuerySync([userId](NYdb::NQuery::TSession session) -> NYdb::TStatus {
    auto result1 = session.ExecuteQuery(...).GetValueSync();
    return result1;
});

// thread 2
auto status2 = client.RetryQuerySync([userId](NYdb::NQuery::TSession session) -> NYdb::TStatus {
    auto result2 = session.ExecuteQuery(...).GetValueSync();
    return result2.GetStatus();
});
  ```
  {% endcut %}

- JavaScript

  {% cut "Плохой пример" %}
  ```javascript
// НЕПРАВИЛЬНО: Неполное чтение результатов
const session = await driver.tableClient.withSession(async (session) => {
    const result = await session.executeQuery("SELECT * FROM table");
    // Не читаем результаты до конца
    return result; // Сессия остается занятой и ее может успеть взять другой поток для исполнения запроса!
});
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```javascript
// ПРАВИЛЬНО: Прочитать все результаты
const session = await driver.tableClient.withSession(async (session) => {
    const result = await session.executeQuery("SELECT * FROM table");

    const rows = [];
    for await (const row of result) {
        rows.push(row);
    }
    return rows;
});
  ```
  {% endcut %}

{% endlist %}
