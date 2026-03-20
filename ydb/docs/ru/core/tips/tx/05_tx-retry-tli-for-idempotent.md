# Обработка TLI для идемпотентных операций

## Проблема

TLI (Transaction Locks Invalidated) возникает в {{ ydb-short-name }} при использовании [оптимистических блокировок](../../concepts/glossary.md#optimistic-locking), когда параллельные транзакции изменяют одни и те же данные. Базовые сведения о транзакциях — [{#T}](../../concepts/transactions.md), про ретраи — [{#T}](../../recipes/ydb-sdk/retry.md). Конкретно, ошибка возникает когда:

- Одна транзакция читает диапазон ключей
- Другая транзакция пишет в этот же диапазон и успешно коммитится
- Первая транзакция пытается закоммититься, но её блокировки уже инвалидированы

Для идемпотентных операций это нормальная ситуация, которую нужно обрабатывать через ретрай.

## Решение

### Основные принципы

1. **Идемпотентность операции** - гарантия того, что повторное выполнение операции не изменит результат
2. **Экспоненциальная задержка** - стратегия ретраев с постепенно увеличивающимися интервалами
3. **Ограничение числа попыток** - предотвращение бесконечных циклов

### Стратегии минимизации TLI

- Уменьшение времени жизни транзакции
- Чтение только необходимых данных
- Избегание интерактивных транзакций
- Использование `SELECT FOR UPDATE` для явных блокировок
- Разбиение крупных транзакций на более мелкие


## Примеры кода

### ❌ Плохой пример - отсутствие обработки TLI

```python
# Проблема: транзакция может упасть с TLI и данные не сохранятся
def update_user_balance(user_id: int, amount: int):
    with db.transaction() as tx:
        # Читаем текущий баланс
        result = tx.execute(
            "SELECT balance FROM users WHERE id = $userId",
            {"$userId": user_id},
        )
        current_balance = result[0].rows[0]["balance"]

        # Обновляем баланс
        new_balance = current_balance + amount
        tx.execute(
            "UPDATE users SET balance = $newBalance WHERE id = $userId",
            {"$userId": user_id, "$newBalance": new_balance},
        )

        # Если здесь произойдет TLI - транзакция упадет без ретрая
        tx.commit()
```

### ✅ Хороший пример - правильная обработка TLI с ретраями

```python
import time
from ydb import RetryableError

def update_user_balance_with_retry(user_id: int, amount: int, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            with db.transaction() as tx:
                # Читаем текущий баланс
                result = tx.execute(
                    "SELECT balance FROM users WHERE id = $userId",
                    {"$userId": user_id},
                )
                current_balance = result[0].rows[0]["balance"]

                # Обновляем баланс
                new_balance = current_balance + amount
                tx.execute(
                    "UPDATE users SET balance = $newBalance WHERE id = $userId",
                    {"$userId": user_id, "$newBalance": new_balance},
                )

                tx.commit()
                return new_balance

        except RetryableError as e:
            if "Transaction locks invalidated" in str(e) and attempt < max_retries - 1:
                # Экспоненциальная задержка
                time.sleep(2 ** attempt)
                continue
            raise

    raise Exception(f"Failed after {max_retries} attempts")

# Альтернативный вариант с использованием встроенного ретрая YDB SDK
def update_user_balance_ydb_retry(user_id: int, amount: int):
    def operation(tx):
        result = tx.execute(
            "SELECT balance FROM users WHERE id = $userId",
            {"$userId": user_id},
        )
        current_balance = result[0].rows[0]["balance"]
        new_balance = current_balance + amount
        tx.execute(
            "UPDATE users SET balance = $newBalance WHERE id = $userId",
            {"$userId": user_id, "$newBalance": new_balance},
        )

    # Используем встроенный механизм ретраев YDB SDK
    return db.retry_operation(operation, idempotent=True)
```

### Пример на Go с использованием YDB Go SDK

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func updateUserBalanceWithRetry(ctx context.Context, db *ydb.Driver, userID int64, amount int64) error {
    maxRetries := 3

    for i := 0; i < maxRetries; i++ {
        err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
            // Читаем текущий баланс
            var currentBalance int64
            row, err := tx.QueryRow(ctx,
                "SELECT balance FROM users WHERE id = $userId",
                query.WithParameters(
                    ydb.ParamsBuilder().
                        Param("$userId").Int64(userID).
                        Build(),
                ),
            )
            if err != nil {
                return err
            }
            if err = row.Scan(&currentBalance); err != nil {
                return err
            }

            // Обновляем баланс
            newBalance := currentBalance + amount
            _, err = tx.Exec(ctx,
                "UPDATE users SET balance = $newBalance WHERE id = $userId",
                query.WithParameters(
                    ydb.ParamsBuilder().
                        Param("$userId").Int64(userID).
                        Param("$newBalance").Int64(newBalance).
                        Build(),
                ),
            )
            return err
        }, query.WithIdempotent())
        if err != nil {
            // Проверяем, является ли ошибка TLI
            if isTLIError(err) && i < maxRetries-1 {
                time.Sleep(time.Duration(1<<i) * time.Second) // Экспоненциальная задержка
                continue
            }
            return err
        }

        return nil
    }

    return fmt.Errorf("failed after %d attempts", maxRetries)
}

func isTLIError(err error) bool {
    return ydb.IsOperationError(err, Ydb.StatusIds_ABORTED)
}
```

### Пример на Java с {{ ydb-short-name }} Native SDK

```java
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

public class TLIHandler {

    public static void updateUserBalanceWithRetry(TableClient client, long userId, int amount) {
        RetrySettings retrySettings = RetrySettings.newBuilder()
            .maxRetries(3)
            .idempotent(true)
            .build();

        Retry.retryWithSettings(retrySettings, () -> {
            try (Session session = client.createSession().join()) {
                // Читаем текущий баланс
                ResultSetReader reader = session.executeDataQuery(
                    "SELECT balance FROM users WHERE id = $userId",
                    TxControl.serializableRw().setCommitTx(false),
                    Params.of(
                        "$userId", PrimitiveValue.newInt64(userId)
                    )
                ).join().getResultSet(0);

                if (reader.next()) {
                    int currentBalance = reader.getColumn("balance").getInt32();
                    int newBalance = currentBalance + amount;

                    session.executeDataQuery(
                        "UPDATE users SET balance = $newBalance WHERE id = $userId",
                        TxControl.serializableRw().setCommitTx(true),
                        Params.of(
                            "$userId", PrimitiveValue.newInt64(userId),
                            "$newBalance", PrimitiveValue.newInt32(newBalance)
                        )
                    ).join();
                }
            }
            return null;
        });
    }
}
```


## Рекомендации

1. **Всегда проверяйте идемпотентность** операции перед добавлением ретраев
2. **Используйте встроенные механизмы ретраев** {{ ydb-short-name }} SDK когда возможно
3. **Мониторьте процент TLI ошибок** в мониторинге для понимания уровня конкуренции
4. **Оптимизируйте схему доступа** к данным для уменьшения конфликтов
