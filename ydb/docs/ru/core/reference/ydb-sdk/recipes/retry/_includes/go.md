В {{ ydb-short-name }} Go SDK корректная обработка ошибок закреплена в нескольких программных интерфейсах:

* Основная логика обработки ошибок реализуется функцией-помощником `retry.Retry`.
  Подробности выполнения повторных запросов максимально скрыты.
  Пользователь может влиять на логику работы функции `retry.Retry` двумя способами:
   * через контекст (можно устанавливать deadline и cancel);
   * через флаг идемпотентности операции `retry.WithIdempotent()`. По умолчанию операция считается неидемпотентной.

  Пользователь передает свою функцию в `retry.Retry`, которая по своей сигнатуре должна возвращать ошибку.
  В случае, если из пользовательской функции вернулся `nil`, то повторные запросы прекращаются.
  В случае, если из пользовательской функции вернулась ошибка, {{ ydb-short-name }} Go SDK пытается идентифицировать эту ошибку и в зависимости от нее выполняет повторные попытки.

  {% cut "Пример кода, использующего функцию `retry.Retry`:" %}

    ```go
    package main

    import (
        "context"
        "time"

        "github.com/ydb-platform/ydb-go-sdk/v3"
        "github.com/ydb-platform/ydb-go-sdk/v3/retry"
    )

    func main() {
        db, err := ydb.Open(ctx,
            os.Getenv("YDB_CONNECTION_STRING"),
        )
        if err != nil {
            panic(err)
        }
        defer func() {
            _ = db.Close(ctx)
        }()
        var cancel context.CancelFunc
        // fix deadline for retries
        ctx, cancel := context.WithTimeout(ctx, time.Second)
        err = retry.Retry(
            ctx,
            func(ctx context.Context) error {
                whoAmI, err := db.Discovery().WhoAmI(ctx)
                if err != nil {
                    return err
                }
                fmt.Println(whoAmI)
            },
            retry.WithIdempotent(true),
        )
        if err != nil {
            panic(err)
        }
    }
    ```

  {% endcut %}

* Сервис для работы с табличными запросами `db.Table()` сразу предоставляет программный интерфейс `table.Client`, который использует пакет `retry`, а также следит за временем жизни сессий {{ ydb-short-name }}.
  Пользователю доступны две публичных функции: `db.Table().Do(ctx, op)` (`op` предоставляет сессию) и `db.Table().DoTx(ctx, op)` (`op` предоставляет транзакцию).
  Как и в предыдущем случае пользователь может влиять на логику выполнения повторных запросов через контекст и признак идемпотентности, а {{ ydb-short-name }} Go SDK интерпретирует возвращаемые из `op` ошибки.
* Запросы к остальным сервисам {{ ydb-short-name }} (`db.Scripting()`, `db.Scheme()`, `db.Coordination()`, `db.Ratelimiter()`, `db.Discovery()`) также используют внутри себя функцию `retry.Retry` для выполнения повторных запросов.
