# ent

[ent](https://entgo.io/) — это фреймворк для работы с сущностями на языке Go, упрощающий создание и поддержку приложений с большими моделями данных. Ent предоставляет генерацию кода на основе описания схем, типобезопасные запросы и автоматические миграции.

{{ ydb-short-name }} поддерживает интеграцию с Ent через форк [ydb-platform/ent](https://github.com/ydb-platform/ent), добавляющий диалект `ydb` и поддержку генерации YQL-запросов.

В статье описано подключение Go-приложения на базе Ent к {{ ydb-short-name }} с помощью этого форка и совместимого инструментария миграций [ydb-platform/ariga-atlas](https://github.com/ydb-platform/ariga-atlas).

{% note warning %}

Поддержка {{ ydb-short-name }} в ent находится в стадии **preview** и требует использования [Atlas migration engine](https://entgo.io/docs/migrate#atlas-integration) для управления миграциями.

{% endnote %}

## Установка

1) Установите CLI Ent (например, в современной версии Go)

    ```bash
    go get entgo.io/ent/cmd/ent@latest
    ```

2) В файле `go.mod` замените Ent на форк с поддержкой {{ ydb-short-name }}:

    ```text
    replace entgo.io/ent => github.com/ydb-platform/ent v0.0.1
    ```

3) Замените **Atlas** (движок миграций, который использует Ent) на форк с поддержкой {{ ydb-short-name }}:

    ```text
    replace ariga.io/atlas => github.com/ydb-platform/ariga-atlas v0.0.1
    ```

4) Сгенерируйте проект, как описано в [кратком введении](https://entgo.io/docs/getting-started/). Например:

    ```bash
    go run -mod=mod entgo.io/ent/cmd/ent new User
    ```

5) Выполните `go mod tidy`, чтобы обновить `go.mod` и `go.sum`:

    ```bash
    go mod tidy
    ```

## Подключение

Для подключения к {{ ydb-short-name }} используйте стандартную функцию `ent.Open()` с диалектом `"ydb"`:

```go
package main

import (
    "context"
    "log"

    "entdemo/ent"
)

func main() {
    client, err := ent.Open("ydb", "grpc://localhost:2136/local")
    if err != nil {
        log.Fatalf("failed opening connection to ydb: %v", err)
    }
    defer client.Close()

    ctx := context.Background()

    if err := client.Schema.Create(ctx); err != nil {
        log.Fatalf("failed creating schema resources: %v", err)
    }
}
```

## Транзакции {#transactions}

При использовании высокоуровневых CRUD-билдеров операции записи выполняются внутри короткоживущих транзакций через [`retry.DoTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#DoTx) в драйвере, а типичные операции чтения — через [`retry.Do`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#Do) без интерактивной транзакции `database/sql`. `Client.BeginTx()` следует модели явной транзакции `database/sql`. Поэтому с {{ ydb-short-name }} через Ent доступны два различных способа работы с транзакциями.

### Неинтерактивные транзакции {#non-interactive-transactions}

При использовании стандартных CRUD-билдеров (`.Create()`, `.Query()`, `.Update()`, `.Delete()`) Ent выполняет операции через механизм повторных попыток ydb-go-sdk:

- **Операции записи** (Create, Update, Delete) выполняются через [`retry.DoTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#DoTx) — SDK открывает транзакцию, выполняет операцию как callback, коммитит, а при временной ошибке откатывает и повторяет callback заново.
- **Операции чтения** (Query) выполняются через [`retry.Do`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#Do) — SDK получает соединение, выполняет callback чтения и повторяет при временных ошибках. Явная транзакция не создаётся; чтение выполняется с неявным snapshot.

Это рекомендуемый способ работы с {{ ydb-short-name }} через Ent. Автоматические повторные попытки и управление сессиями выполняются прозрачно.

### Интерактивные транзакции {#interactive-transactions}

При вызове `Client.BeginTx()` Ent открывает транзакцию через стандартный `database/sql` API и возвращает объект `Tx`. Вы выполняете операции и вручную вызываете `Commit()` или `Rollback()`. В этом режиме:

- Нет callback-функции, которую SDK мог бы повторить, поэтому **автоматические повторные попытки невозможны**.
- Время жизни сессии и транзакции управляется вашим кодом.

Используйте интерактивные транзакции только когда нужен явный контроль над границами commit/rollback, который невозможно выразить через стандартные билдеры.

## Автоматический механизм повторных попыток {#retry-mechanism}

Поскольку {{ ydb-short-name }} — распределённая база данных, временные ошибки (сетевые проблемы, кратковременная недоступность и т.п.) требуют явной обработки. Драйвер Ent для {{ ydb-short-name }} интегрируется с [пакетом retry из ydb-go-sdk](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry) и по возможности обрабатывает такие сценарии автоматически.

{% note info %}

Автоматические повторные попытки не используются при создании интерактивной транзакции через `Client.BeginTx()`. Подробнее см. в разделе [{#T}](#interactive-transactions).

{% endnote %}

### Использование WithRetryOptions

Все CRUD-операции поддерживают метод `WithRetryOptions()` для настройки поведения повторных попыток:

```go
import "github.com/ydb-platform/ydb-go-sdk/v3/retry"

// Create
user, err := client.User.Create().
    SetName("John").
    SetAge(30).
    WithRetryOptions(retry.WithIdempotent(true)).
    Save(ctx)

// Query
users, err := client.User.Query().
    Where(user.AgeGT(18)).
    WithRetryOptions(retry.WithIdempotent(true)).
    All(ctx)

// Update
affected, err := client.User.Update().
    Where(user.NameEQ("John")).
    SetAge(31).
    WithRetryOptions(retry.WithIdempotent(true)).
    Save(ctx)

// Delete
affected, err := client.User.Delete().
    Where(user.NameEQ("John")).
    WithRetryOptions(retry.WithIdempotent(true)).
    Exec(ctx)
```

### Параметры повторных попыток

Часто используемые параметры из `ydb-go-sdk`:

| Параметр | Описание |
| -------- | -------- |
| `retry.WithIdempotent(true)` | Пометить операцию как идемпотентную, что позволяет повторять при большем количестве типов ошибок |
| `retry.WithLabel(string)` | Добавить метку для отладки/трассировки |
| `retry.WithTrace(trace.Retry)` | Включить трассировку повторных попыток |

## Известные ограничения {#limitations}

### Нет автоматических повторных попыток при использовании Client.BeginTx {#no-retry-begintx}

`Client.BeginTx()` возвращает объект транзакции вместо того, чтобы принимать callback, поэтому механизм повторных попыток из ydb-go-sdk не может быть применён. Подробнее см. в разделе [{#T}](#interactive-transactions).

При необходимости можно написать обёртку для повторных попыток вручную, как показано в [примерах ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/basic/database/sql/series.go).

### Нет вложенных транзакций {#no-nested-transactions}

{{ ydb-short-name }} использует плоские транзакции и не поддерживает вложенные. Драйвер Ent для {{ ydb-short-name }} при запросе вложенной транзакции возвращает no-op (пустой) объект транзакции.

### Нет коррелированных подзапросов {#no-correlated-subqueries}

{{ ydb-short-name }} не поддерживает коррелированные подзапросы с `EXISTS` или `NOT EXISTS`. Интеграция с Ent автоматически переписывает такие запросы, используя `IN` с подзапросами.

### Ограничение индексов на Float/Double {#float-index-restriction}

{{ ydb-short-name }} не позволяет использовать типы `Float` или `Double` в качестве ключей индексов. Если индекс определён на поле с плавающей точкой, он будет пропущен при миграции.

### Нет нативных типов Enum {#no-native-enums}

{{ ydb-short-name }} не поддерживает типы enum в DDL. Интеграция с Ent отображает enum-поля на тип `Utf8` (строка), а валидация выполняется на уровне приложения.

### Требования к первичному ключу {#primary-key-requirements}

{{ ydb-short-name }} требует явного первичного ключа для всех таблиц. Определите соответствующие поля ID в схемах Ent.

## Полезные ссылки

- [Репозиторий ydb-platform/ent на GitHub](https://github.com/ydb-platform/ent)
- [Пример проекта](https://github.com/ydb-platform/ent/tree/ydb-develop/examples/ydb)
- [Документация Ent](https://entgo.io/docs/getting-started)
