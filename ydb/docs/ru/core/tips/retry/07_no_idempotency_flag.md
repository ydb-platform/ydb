# Флаг идемпотентности

## Проблема

Флаг идемпотентности в {{ ydb-short-name }} SDK позволяет определить, можно ли безопасно повторять операцию при временных ошибках. Неправильное использование этого флага приводит к двум основным проблемам:

1. **Отсутствие флага для идемпотентных операций** - ретраер не будет повторять операции при временных ошибках ([`UNAVAILABLE`](../../reference/ydb-sdk/grpc-status-codes.md#unavailable), [`DEADLINE_EXCEEDED`](../../reference/ydb-sdk/grpc-status-codes.md#deadline-exceeded)), что приводит к ненужным сбоям
2. **Неправильное использование флага для неидемпотентных операций** - может привести к дублированию данных и неконсистентности

## Решение

См. также общую схему статусов в [{#T}](../../reference/ydb-sdk/ydb-status-codes.md) и [{#T}](../../reference/ydb-sdk/grpc-status-codes.md). Практика ретраев — [{#T}](../../recipes/ydb-sdk/retry.md).

Всегда проставляйте флаг идемпотентности для операций, которые являются идемпотентными. Идемпотентные операции - это операции, которые можно безопасно повторять без изменения результата.

### Примеры идемпотентных операций:
- SELECT запросы (чтение данных)
- UPSERT (если правильно спроектированы)
- DELETE по первичному ключу
- Операции с семафорами

### Примеры неидемпотентных операций:
- INSERT без проверки существования
- Инкрементные UPDATE
- Операции с побочными эффектами

## Примеры кода

### Плохой пример: отсутствие флага для идемпотентной операции

```go
import (
    "context"
    "log"
    "github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func main() {
    err := retry.Do(
        context.Background(),
        func(ctx context.Context) (err error) {
            // SELECT запрос - идемпотентная операция
            return executeSelectQuery(ctx)
        },
        // Нет указания флага idempotent - по умолчанию false
    )
    if err != nil {
        log.Printf("Query failed: %v\n", err)
    }
}
```

**Проблема**: При временной ошибке UNAVAILABLE операция не будет повторена, хотя это безопасно.

### Хороший пример: правильное использование флага

```go
import (
    "context"
    "log"
    "github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func main() {
    err := retry.Do(
        context.Background(),
        func(ctx context.Context) (err error) {
            // SELECT запрос - идемпотентная операция
            return executeSelectQuery(ctx)
        },
        retry.WithIdempotent(true), // Указываем, что операция идемпотентна
    )
    if err != nil {
        log.Printf("Query failed after retries: %v\n", err)
    }
}
```

**Преимущество**: SDK будет повторять операцию при временных ошибках, повышая надежность.

### Опасный пример: флаг для неидемпотентной операции

```go
import (
    "context"
    "log"
    "github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func main() {
    err := retry.Do(
        context.Background(),
        func(ctx context.Context) (err error) {
            // INSERT без проверки - неидемпотентная операция
            return executeInsertQuery(ctx)
        },
        retry.WithIdempotent(true), // ОПАСНО: может привести к дублированию
    )
    if err != nil {
        log.Printf("Insert failed: %v\n", err)
    }
}
```

**Риск**: При временной ошибке операция может быть выполнена дважды, создав дублирующиеся записи.

## Рекомендации

1. **Для чтения данных** - всегда используйте `idempotent: true`
2. **Для модификации данных** - анализируйте, является ли операция идемпотентной
3. **Используйте UPSERT вместо INSERT** для создания идемпотентных операций записи
4. **Проверяйте документацию** для конкретных типов операций в {{ ydb-short-name }}

## Дополнительная информация

### Ошибки, которые ретраятся для идемпотентных операций

При установленном флаге `idempotent: true` SDK будет повторять операции для следующих типов ошибок:

- **UNAVAILABLE** - сервис временно недоступен
- **DEADLINE_EXCEEDED** - превышено время выполнения
- **TransactionLocksInvalidated (TLi)** - оптимистичные блокировки в {{ ydb-short-name }} могут приводить к таким ошибкам
- **GENERIC_ERROR** с определенными кодами (например, "tx state unknown")

### Реальные кейсы из практики

Из инцидентов в Яндексе известно, что неправильное использование флага идемпотентности приводило к:

1. **Высокому фону ошибок** - когда идемпотентные операции не ретраились при временных сбоях
2. **Неконсистентности данных** - когда неидемпотентные операции ретраировались и выполнялись дважды

### Особенности работы с топиками

Для операций с топиками (LogBroker) также важно правильно использовать флаг идемпотентности. Чтение из топиков обычно является идемпотентной операцией, если правильно обрабатывается коммит офсетов.

### Java SDK пример

```java
// Хороший пример для Java
SessionRetryContext retryCtx = SessionRetryContext.create(sessionPool)
    .withMaxRetries(5)
    .build();

retryCtx.supplyResult(session -> {
    // Идемпотентная операция чтения
    return session.executeDataQuery("SELECT * FROM table", TxControl.serializableRw());
}, RetrySettings.newBuilder()
    .setIdempotent(true)  // Указываем идемпотентность
    .build());
```

> **Важно**: Обработка ошибок в ретраерах SDK написана "кровью" - доверяйте встроенным механизмам, а не пишите собственные ретраеры без глубокого понимания всех нюансов.

### Ссылки на документацию

- [Выполнение повторных запросов - {{ ydb-short-name }} SDK](../../recipes/ydb-sdk/retry)
- [Обработка временных сбоев](../../reference/ydb-sdk/error_handling#handling-retryable-errors)
