# Очистка результатов между попытками выполнения

## Проблема

При использовании механизма повторных попыток в {{ ydb-short-name }} SDK (см. [{#T}](../../recipes/ydb-sdk/retry.md)) лямбда-функция может выполняться несколько раз. Если внутри лямбды используются изменяемые структуры данных (списки, слайсы, словари), которые накапливают результаты между вызовами, это может привести к дублированию данных и некорректным результатам.

Особенно критично это для:
- Списков/слайсов, в которые складываются результаты запроса
- Счетчиков и аккумуляторов
- Временных буферов данных

**Пример плохого кода (Python):**

```python
import ydb

results = []

def some_op(session: ydb.QuerySession):
    for result_set in session.execute("SELECT * FROM table"):
        results.append(result_set)

pool.retry_operation_sync(some_op)
```

**Пример плохого кода (Go):**

```go
package main

import (
    "context"
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// ПЛОХО: глобальная переменная
var results []string

func ExecuteQueryWithRetryBad(db *ydb.Driver) error {
    return db.Query().Do(context.Background(), func(ctx context.Context, s query.Session) error {
        var titles []string
        err := s.Execute(ctx, `
            SELECT title FROM series LIMIT 10;
        `, query.WithTxControl(query.TxControl(query.BeginTx(query.WithSerializableReadWrite()), query.CommitTx())),
        ).Scan(ctx, &titles)
        if err != nil {
            return err
        }

        // ПЛОХО: данные накапливаются при каждой попытке
        results = append(results, titles...)
        return nil
    })
}
```

## Решение

Необходимо гарантировать, что при каждой новой попытке выполнения лямбды все временные структуры данных инициализируются заново. Это можно достичь несколькими способами:

### 1. Локальные переменные внутри лямбды

**Хороший код (Python):**

```python
import ydb

def some_op(session: ydb.QuerySession):
    results = []
    for result_set in session.execute("SELECT * FROM table"):
        results.append(result_set)
    return results

pool.retry_operation_sync(some_op)
```

**Хороший код (Go):**

```go
package main

import (
    "context"
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func ExecuteQueryWithRetryGood(db *ydb.Driver) ([]string, error) {
    var finalResults []string

    err := db.Query().Do(context.Background(), func(ctx context.Context, s query.Session) error {
        // ХОРОШО: локальная переменная для каждой попытки
        var attemptResults []string

        err := s.Execute(ctx, `
            SELECT title FROM series LIMIT 10;
        `, query.WithTxControl(query.TxControl(query.BeginTx(query.WithSerializableReadWrite()), query.CommitTx())),
        ).Scan(ctx, &attemptResults)
        if err != nil {
            return err
        }

        // ХОРОШО: переносим результаты только при успешной попытке
        finalResults = attemptResults
        return nil
    })

    return finalResults, err
}
```

### 2. Функциональный подход с возвратом результатов

**Хороший код (функциональный стиль):**

```cpp
#include <ydb-cpp-sdk/client/query/client.h>
#include <vector>
#include <string>
#include <optional>

std::optional<std::vector<std::string>> ExecuteQueryWithRetryFunctional(
    NYdb::NQuery::TQueryClient client) {

    std::optional<std::vector<std::string>> finalResult;

    auto status = client.RetryQuerySync([&finalResult](NYdb::NQuery::TSession session) -> NYdb::TStatus {
        std::vector<std::string> attemptResult;

        auto query = R"(
            SELECT title FROM series LIMIT 10;
        )";

        auto result = session.ExecuteQuery(
            query,
            NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync();

        if (!result.IsSuccess()) {
            return result;
        }

        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            attemptResult.push_back(parser.ColumnParser("title").GetOptionalUtf8().value());
        }

        // ХОРОШО: результат упаковывается только при успешном выполнении
        finalResult = std::move(attemptResult);
        return result;
    });

    if (!status.IsSuccess()) {
        return std::nullopt;
    }

    return finalResult;
}
```

## Рекомендации

1. **Избегайте глобальных переменных** в лямбда-функциях для повторных попыток
2. **Используйте локальные переменные** внутри лямбды для хранения промежуточных результатов
3. **Очищайте структуры данных** перед каждой новой попыткой, если используете внешние переменные
4. **Возвращайте результаты через параметры** или возвращаемые значения, а не через побочные эффекты
5. **Тестируйте сценарии с ошибками** чтобы убедиться, что повторные попытки не приводят к дублированию данных

Соблюдение этих правил гарантирует корректную работу приложения при использовании механизма повторных попыток {{ ydb-short-name }} SDK.
