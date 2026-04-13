# Использование deprecated Scripting Service

## Проблема

Использование устаревшего Scripting Service вместо Query Service приводит к следующим проблемам:

- **Отсутствие развития**: Scripting Service не получает новых возможностей и улучшений; развитие сосредоточено на Query Service.
- **Семантические различия**: Несовместимость с современными подходами к выполнению DDL-операций (например, CREATE TABLE IF NOT EXISTS)
- **Ограниченная диагностика**: Меньше возможностей для сбора статистики и анализа производительности запросов

## Решение

Переходите на Query Service:

- **В CLI**: `ydb sql` вместо `ydb scripting yql` или `ydb yql`
- **В SDK**: использовать Query Service клиенты:
  - C++: `TQueryClient` вместо `TScriptingClient`
  - Go: `db.Query()` вместо `db.Scripting()`

Query Service предоставляет:

- Отсутствие лимита на количество строк в ответе;
- Стриминг данных;
- Управление транзакциями;
- Улучшенную семантику DDL-операций (например, CREATE TABLE для существующей таблицы возвращает успех вместо ошибки).

## Примечание

Scripting Service считается устаревшим. Для новых проектов используйте Query Service. Query Service рекомендуется для production-сценариев начиная с версии {{ ydb-short-name }} 24.3.

## Использование в CLI

```bash
# ❌ Устаревший подход
ydb scripting yql -s "SELECT * FROM large_table"
ydb yql -s "SELECT * FROM large_table"

# ✅ Современный подход
ydb sql -s "SELECT * FROM large_table"
```

## Использование в SDK


{% list tabs %}

- C++

  {% cut "Плохой пример" %}
  ```cpp
// Плохой пример: использование устаревшего Scripting Service
#include <ydb-cpp-sdk/client/draft/ydb_scripting.h>

void ExecuteWithScripting(NYdb::TDriver& driver) {
    NYdb::NScripting::TScriptingClient client(driver);

    auto result = client.ExecuteYqlScript(
        "SELECT * FROM series"
    ).GetValueSync();
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```cpp
#include <ydb-cpp-sdk/client/query/client.h>

void ExecuteWithQueryService(NYdb::NQuery::TQueryClient& client) {
    auto result = client.ExecuteQuery(
        "SELECT * FROM series",
        NYdb::NQuery::TTxControl::NoTx()
    ).GetValueSync();
}
  ```
  {% endcut %}

- Go

  {% cut "Плохой пример" %}
  ```go
// Плохой пример: использование устаревшего Scripting Service
import "github.com/ydb-platform/ydb-go-sdk/v3/scripting"

res, err := db.Scripting().Execute(ctx, "SELECT * FROM series", table.NewQueryParameters())
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
import "github.com/ydb-platform/ydb-go-sdk/v3/query"

result, err := db.Query().Query(ctx, "SELECT * FROM series")
  ```
  {% endcut %}

{% endlist %}
