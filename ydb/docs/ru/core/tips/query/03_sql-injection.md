# SQL-инъекции

## Проблема

Встраивание пользовательских данных напрямую в текст SQL-запроса создаёт критическую уязвимость — SQL-инъекцию. Злоумышленник может изменить логику запроса, получить несанкционированный доступ к данным, модифицировать или удалить их.

### Пример атаки

Приложение проверяет логин пользователя:
```sql
SELECT * FROM users WHERE login = 'admin'
```

Если логин пользователя встраивается через конкатенацию строк, злоумышленник может передать:
```
admin' OR '1'='1
```

Результирующий запрос:
```sql
SELECT * FROM users WHERE login = 'admin' OR '1'='1'
```

Условие `'1'='1'` всегда истинно — запрос вернёт всех пользователей.

Более опасный пример:
```
admin'; DROP TABLE users; --
```

Результирующий запрос выполнит удаление таблицы:
```sql
SELECT * FROM users WHERE login = 'admin'; DROP TABLE users; --'
```

## Решение

**Всегда используйте параметризованные запросы** — единственный надёжный способ защиты от SQL-инъекций.

Параметризованные запросы передают пользовательские данные отдельно от текста запроса. {{ ydb-short-name }} SDK автоматически обрабатывает параметры безопасным образом:
- Экранирует специальные символы
- Проверяет соответствие типов
- Гарантирует, что данные не могут изменить структуру запроса


{% list tabs %}

- Python

  {% cut "Плохой пример" %}
  ```python
# ОПАСНО! Уязвимо для SQL-инъекций
user_login = "admin' OR '1'='1"  # Атака
query = f"SELECT * FROM users WHERE login = '{user_login}'"
pool.execute_with_retries(query)
# Выполнится: SELECT * FROM users WHERE login = 'admin' OR '1'='1'
# Вернёт всех пользователей!
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```python
# Безопасно: параметризованный запрос
user_login = "admin' OR '1'='1"  # Попытка атаки
pool.execute_with_retries(
    "SELECT * FROM users WHERE login = $login",
    {"$login": user_login}
)
# SDK экранирует значение, атака не сработает
# Ищется пользователь с логином "admin' OR '1'='1" (буквально)
  ```
  {% endcut %}

- Go

  {% cut "Плохой пример" %}
  ```go
// ОПАСНО! Уязвимо для SQL-инъекций
userLogin := "admin' OR '1'='1"
query := fmt.Sprintf("SELECT * FROM users WHERE login = '%s'", userLogin)
db.Query().Query(ctx, query)
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
// Безопасно: параметризованный запрос
userLogin := "admin' OR '1'='1"
db.Query().Query(ctx,
    `SELECT * FROM users WHERE login = $login`,
    query.WithParameters(
        ydb.ParamsBuilder().Param("$login").Text(userLogin).Build(),
    ),
)
  ```
  {% endcut %}

- C#

  {% cut "Плохой пример" %}
  ```csharp
// ОПАСНО! Уязвимо для SQL-инъекций
var userLogin = "admin' OR '1'='1";
var query = $"SELECT * FROM users WHERE login = '{userLogin}'";
await client.Exec(query);
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```csharp
// Безопасно: параметризованный запрос
var userLogin = "admin' OR '1'='1";
await client.Exec(
    "SELECT * FROM users WHERE login = $login",
    new Dictionary<string, YdbValue> { { "$login", YdbValue.MakeUtf8(userLogin) } }
);
  ```
  {% endcut %}

- JavaScript

  {% cut "Хороший пример" %}
  ```javascript
// Безопасно: параметризованный запрос
const userLogin = "admin' OR '1'='1";
await sql`SELECT * FROM users WHERE login = ${new Utf8(userLogin)}`;
// SDK автоматически параметризует при использовании типов YDB
  ```
  {% endcut %}

{% endlist %}
