# Объединение нескольких операций в один запрос

## Проблема

При последовательном выполнении нескольких запросов к {{ ydb-short-name }} общая задержка транзакции складывается из времени выполнения каждого запроса и сетевых задержек между ними. Основные проблемы:

- **Накопление сетевых задержек** — каждый запрос добавляет как минимум один round-trip между клиентом и сервером
- **Увеличение общего времени выполнения** — для N запросов общая latency = сумма (latency каждого запроса + network RTT)
- **Снижение пропускной способности** — каждое обращение к базе несёт накладные расходы на установку соединения и передачу метаданных

Это особенно критично для интерактивных транзакций, где между запросами выполняется код на стороне клиента.

## Решение

{{ ydb-short-name }} позволяет выполнить несколько YQL-операций в рамках одного запроса. Если транзакция не требует интерактивности (выполнения кода между запросами), рекомендуется объединить все операции в один YQL-запрос. Это исключает дополнительные сетевые задержки и улучшает производительность.

### Преимущества объединения операций

1. **Минимальная сетевая задержка** — все операции выполняются за один round-trip к серверу
2. **Атомарность** — все операции выполняются в рамках одной транзакции с ACID-гарантиями
3. **Упрощение кода** — меньше кода для управления транзакциями и обработки ошибок
4. **Эффективное использование ресурсов** — сервер может оптимизировать выполнение всех операций вместе

### Пример на YQL

```sql
-- Объединение нескольких операций в одном запросе
-- Параметры $user_id, $product_id, $quantity передаются через SDK

-- 1. Получаем информацию о пользователе
SELECT name, email FROM users WHERE user_id = $user_id;

-- 2. Получаем информацию о товаре
SELECT title, price FROM products WHERE product_id = $product_id;

-- 3. Создаём заказ
INSERT INTO orders (user_id, product_id, quantity, created_at)
VALUES ($user_id, $product_id, $quantity, CurrentUtcTimestamp())
RETURNING *;
```

Все эти операции выполняются за один round-trip к серверу в одной транзакции. Если любая из операций завершится с ошибкой, все изменения будут отменены. Клиент получит все result sets по порядку.

## Когда использовать объединение операций

**Рекомендуется использовать:**
- Когда операции логически связаны и должны выполняться атомарно
- Для паттерна чтение-модификация-запись (read-modify-write)
- Когда между операциями не требуется выполнение кода на стороне клиента
- Для транзакций с несколькими DML-операциями (INSERT, UPDATE, DELETE, SELECT)

**Не рекомендуется использовать:**
- Когда требуется интерактивная транзакция с логикой между запросами на клиенте
- Для массовых операций (используйте специальные batch-методы SDK)
- Когда операции логически независимы и не требуют общей транзакции
- Для DDL-операций (CREATE TABLE, ALTER TABLE) — {{ ydb-short-name }} не поддерживает объединение DDL с DML

## Ограничения

- **Размер запроса:** текст запроса не должен превышать лимиты {{ ydb-short-name }} (обычно несколько МБ)
- **Транзакционные гарантии:** все операции выполняются в одной транзакции — при ошибке откатываются все изменения
- **Оптимистичные блокировки:** {{ ydb-short-name }} использует Optimistic Concurrency Control, при конфликтах требуется повторное выполнение транзакции

## Итоги

Объединение нескольких операций в один запрос — это эффективный способ снижения latency при работе с {{ ydb-short-name }}. Используйте этот подход для неинтерактивных транзакций, где все операции известны заранее и не требуют выполнения кода между ними на стороне клиента.


{% list tabs %}

- Python

  {% cut "Плохой пример" %}
  ```python
# Плохой пример: три отдельных запроса = три round-trip
# Общая latency = latency1 + RTT + latency2 + RTT + latency3 + RTT

def create_order_bad(pool, user_id, product_id, quantity):
    # Запрос 1
    pool.execute_with_retries(
        "SELECT name, email FROM users WHERE user_id = $userId",
        {"$userId": user_id}
    )
    
    # Запрос 2
    pool.execute_with_retries(
        "SELECT title, price FROM products WHERE product_id = $productId",
        {"$productId": product_id}
    )
    
    # Запрос 3
    pool.execute_with_retries(
        "INSERT INTO orders (user_id, product_id, quantity) VALUES ($userId, $productId, $quantity) RETURNING *",
        {"$userId": user_id, "$productId": product_id, "$quantity": quantity}
    )
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```python
# Хороший пример: все операции в одном запросе = один round-trip
# Общая latency = latency_combined + RTT

def create_order_good(pool, user_id, product_id, quantity):
    pool.execute_with_retries(
        """
        SELECT name, email FROM users WHERE user_id = $userId;
        SELECT title, price FROM products WHERE product_id = $productId;
        INSERT INTO orders (user_id, product_id, quantity) VALUES ($userId, $productId, $quantity) RETURNING *;
        """,
        {"$userId": user_id, "$productId": product_id, "$quantity": quantity}
    )
  ```
  {% endcut %}

- Go

  {% cut "Плохой пример" %}
  ```go
// Плохой пример: три отдельных запроса = три round-trip

func CreateOrderBad(ctx context.Context, db *ydb.Driver, userID, productID uint64, quantity int32) {
    // Запрос 1
    db.Query().Query(ctx,
        `SELECT name, email FROM users WHERE user_id = $userId`,
        query.WithParameters(ydb.ParamsBuilder().Param("$userId").Uint64(userID).Build()),
    )
    
    // Запрос 2
    db.Query().Query(ctx,
        `SELECT title, price FROM products WHERE product_id = $productId`,
        query.WithParameters(ydb.ParamsBuilder().Param("$productId").Uint64(productID).Build()),
    )
    
    // Запрос 3
    db.Query().Query(ctx,
        `INSERT INTO orders (user_id, product_id, quantity) VALUES ($userId, $productId, $quantity) RETURNING *`,
        query.WithParameters(
            ydb.ParamsBuilder().
                Param("$userId").Uint64(userID).
                Param("$productId").Uint64(productID).
                Param("$quantity").Int32(quantity).
                Build(),
        ),
    )
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
// Хороший пример: все операции в одном запросе = один round-trip

func CreateOrderGood(ctx context.Context, db *ydb.Driver, userID, productID uint64, quantity int32) {
    db.Query().Query(ctx,
        `SELECT name, email FROM users WHERE user_id = $userId;
         SELECT title, price FROM products WHERE product_id = $productId;
         INSERT INTO orders (user_id, product_id, quantity) VALUES ($userId, $productId, $quantity) RETURNING *;`,
        query.WithParameters(
            ydb.ParamsBuilder().
                Param("$userId").Uint64(userID).
                Param("$productId").Uint64(productID).
                Param("$quantity").Int32(quantity).
                Build(),
        ),
    )
}
  ```
  {% endcut %}

- C#

  {% cut "Плохой пример" %}
  ```csharp
// Плохой пример: три отдельных запроса = три round-trip

public async Task CreateOrderBad(QueryClient client, ulong userId, ulong productId, int quantity)
{
    // Запрос 1
    await client.Exec(
        "SELECT name, email FROM users WHERE user_id = $userId",
        new Dictionary<string, YdbValue> { { "$userId", YdbValue.MakeUint64(userId) } }
    );
    
    // Запрос 2
    await client.Exec(
        "SELECT title, price FROM products WHERE product_id = $productId",
        new Dictionary<string, YdbValue> { { "$productId", YdbValue.MakeUint64(productId) } }
    );
    
    // Запрос 3
    await client.Exec(
        "INSERT INTO orders (user_id, product_id, quantity) VALUES ($userId, $productId, $quantity) RETURNING *",
        new Dictionary<string, YdbValue> {
            { "$userId", YdbValue.MakeUint64(userId) },
            { "$productId", YdbValue.MakeUint64(productId) },
            { "$quantity", YdbValue.MakeInt32(quantity) }
        }
    );
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```csharp
// Хороший пример: все операции в одном запросе = один round-trip

public async Task CreateOrderGood(QueryClient client, ulong userId, ulong productId, int quantity)
{
    await client.Exec(@"
        SELECT name, email FROM users WHERE user_id = $userId;
        SELECT title, price FROM products WHERE product_id = $productId;
        INSERT INTO orders (user_id, product_id, quantity) VALUES ($userId, $productId, $quantity) RETURNING *;
    ",
    new Dictionary<string, YdbValue> {
        { "$userId", YdbValue.MakeUint64(userId) },
        { "$productId", YdbValue.MakeUint64(productId) },
        { "$quantity", YdbValue.MakeInt32(quantity) }
    });
}
  ```
  {% endcut %}

- JavaScript

  {% cut "Плохой пример" %}
  ```javascript
// Плохой пример: три отдельных запроса = три round-trip

async function createOrderBad(userId, productId, quantity) {
    // Запрос 1
    await sql`SELECT name, email FROM users WHERE user_id = ${new Uint64(userId)}`;
    
    // Запрос 2
    await sql`SELECT title, price FROM products WHERE product_id = ${new Uint64(productId)}`;
    
    // Запрос 3
    await sql`INSERT INTO orders (user_id, product_id, quantity) VALUES (${new Uint64(userId)}, ${new Uint64(productId)}, ${new Int32(quantity)}) RETURNING *`;
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```javascript
// Хороший пример: все операции в одном запросе = один round-trip

async function createOrderGood(userId, productId, quantity) {
    await sql`
        SELECT name, email FROM users WHERE user_id = ${new Uint64(userId)};
        SELECT title, price FROM products WHERE product_id = ${new Uint64(productId)};
        INSERT INTO orders (user_id, product_id, quantity) VALUES (${new Uint64(userId)}, ${new Uint64(productId)}, ${new Int32(quantity)}) RETURNING *;
    `;
}
  ```
  {% endcut %}

{% endlist %}
