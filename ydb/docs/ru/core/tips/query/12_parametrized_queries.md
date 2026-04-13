# Параметризованные запросы для производительности

## Проблема

Встраивание значений напрямую в текст запроса (через конкатенацию или форматирование строк) приводит к **избыточной компиляции запросов**.

Каждый запрос с уникальным набором значений создаёт новый текст запроса, который {{ ydb-short-name }} должна скомпилировать заново:

```sql
SELECT * FROM users WHERE id = 123 AND status = 'active';
SELECT * FROM users WHERE id = 456 AND status = 'inactive';
SELECT * FROM users WHERE id = 789 AND status = 'active';
```

Все три запроса имеют одинаковую структуру, но из-за разных значений {{ ydb-short-name }} видит их как три разных запроса и компилирует каждый отдельно. Это создаёт высокую нагрузку на CPU сервера и увеличивает latency каждого запроса.

## Решение

**Параметризованные запросы** отделяют структуру запроса от данных. Значения передаются как параметры:

```sql
SELECT * FROM users WHERE id = $userId AND status = $status;
```

### Как это работает

1. **Первый запрос:** {{ ydb-short-name }} компилирует структуру запроса и кэширует скомпилированный план
2. **Последующие запросы:** {{ ydb-short-name }} использует закэшированный план, подставляя новые значения параметров
3. **Результат:** Компиляция выполняется один раз, независимо от количества выполнений

### Преимущества

- **Снижение нагрузки на CPU** — компиляция выполняется один раз вместо N раз
- **Уменьшение latency** — пропускается фаза компиляции для повторных запросов
- **Увеличение пропускной способности** — сервер обрабатывает больше запросов при той же мощности
- **Дополнительно:** защита от SQL-инъекций (подробнее в статье "SQL-инъекции")

## Проверка запросов через {{ ydb-short-name }} CLI

Вы можете проверить корректность параметризованного запроса через {{ ydb-short-name }} CLI:

```bash
# Выполнение параметризованного запроса
ydb -e grpcs://localhost:2135 -d /local sql \
  -p '$userId=123' \
  -p '$status="active"' \
  -s 'SELECT * FROM users WHERE id = $userId AND status = $status'
```


{% list tabs %}

- Python

  {% cut "Плохой пример" %}
  ```python
# Плохой пример: конкатенация строк
# Каждый запрос компилируется заново + уязвимость к SQL-инъекциям

def get_user_bad(pool, user_id, status):
    # f-string встраивает значения напрямую в текст запроса
    query = f"SELECT * FROM users WHERE id = {user_id} AND status = '{status}'"
    pool.execute_with_retries(query)
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```python
# Хороший пример: параметризованный запрос
# План запроса кэшируется + защита от SQL-инъекций

def get_user_good(pool, user_id, status):
    pool.execute_with_retries(
        "SELECT * FROM users WHERE id = $userId AND status = $status",
        {"$userId": user_id, "$status": status}
    )
  ```
  {% endcut %}

- Go

  {% cut "Плохой пример" %}
  ```go
// Плохой пример: конкатенация строк
// Каждый запрос компилируется заново + риск SQL-инъекций

func GetUserBad(ctx context.Context, db *ydb.Driver, userID uint64, status string) {
    // fmt.Sprintf создаёт уникальную строку запроса
    query := fmt.Sprintf("SELECT * FROM users WHERE id = %d AND status = '%s'", userID, status)
    
    db.Query().Query(ctx, query)
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
// Хороший пример: параметризованный запрос
// План запроса кэшируется + безопасность

func GetUserGood(ctx context.Context, db *ydb.Driver, userID uint64, status string) {
    db.Query().Query(ctx,
        `SELECT * FROM users WHERE id = $userId AND status = $status`,
        query.WithParameters(
            ydb.ParamsBuilder().
                Param("$userId").Uint64(userID).
                Param("$status").Text(status).
                Build(),
        ),
    )
}
  ```
  {% endcut %}

- C#

  {% cut "Плохой пример" %}
  ```csharp
// Плохой пример: интерполяция строк
// Каждый запрос компилируется заново + уязвимость

public async Task GetUserBad(QueryClient client, ulong userId, string status)
{
    // Интерполяция создаёт уникальный текст запроса
    var query = $"SELECT * FROM users WHERE id = {userId} AND status = '{status}'";
    await client.Exec(query);
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```csharp
// Хороший пример: параметризованный запрос
// Кэширование плана + защита от инъекций

public async Task GetUserGood(QueryClient client, ulong userId, string status)
{
    await client.Exec(
        "SELECT * FROM users WHERE id = $userId AND status = $status",
    new Dictionary<string, YdbValue> {
        { "$userId", YdbValue.MakeUint64(userId) },
        { "$status", YdbValue.MakeUtf8(status) }
    });
}
  ```
  {% endcut %}

- JavaScript

  {% cut "Хороший пример" %}
  ```javascript
// Хороший пример: автоматическая параметризация через типизацию
// SDK автоматически создаёт параметры при использовании YDB типов

async function getUserGood(userId, status) {
    // Tagged template literal с YDB типами автоматически параметризует запрос
    await sql`
        SELECT * FROM users 
        WHERE id = ${new Uint64(userId)} AND status = ${new Utf8(status)}
    `;
}
  ```
  {% endcut %}

{% endlist %}
