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
        current_balance = tx.execute("SELECT balance FROM users WHERE id = ?", user_id)
        
        # Обновляем баланс
        new_balance = current_balance + amount
        tx.execute("UPDATE users SET balance = ? WHERE id = ?", new_balance, user_id)
        
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
                current_balance = tx.execute("SELECT balance FROM users WHERE id = ?", user_id)
                
                # Обновляем баланс
                new_balance = current_balance + amount
                tx.execute("UPDATE users SET balance = ? WHERE id = ?", new_balance, user_id)
                
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
        current_balance = tx.execute("SELECT balance FROM users WHERE id = ?", user_id)
        new_balance = current_balance + amount
        tx.execute("UPDATE users SET balance = ? WHERE id = ?", new_balance, user_id)
    
    # Используем встроенный механизм ретраев YDB SDK
    return db.retry_operation(operation, idempotent=True)
```

### Пример на Go с использованием database/sql

Для `database/sql` рекомендуется использовать `retry.DoTx` из пакета `github.com/ydb-platform/ydb-go-sdk/v3/retry`, который автоматически обрабатывает ретраи, включая TLI-ошибки:

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"

    "github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func updateUserBalance(ctx context.Context, db *sql.DB, userID int, amount int) error {
    return retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
        var currentBalance int
        err := tx.QueryRowContext(ctx,
            "SELECT balance FROM users WHERE id = $1", userID,
        ).Scan(&currentBalance)
        if err != nil {
            return fmt.Errorf("select balance: %w", err)
        }

        newBalance := currentBalance + amount
        _, err = tx.ExecContext(ctx,
            "UPDATE users SET balance = $1 WHERE id = $2", newBalance, userID,
        )
        if err != nil {
            return fmt.Errorf("update balance: %w", err)
        }

        return nil
    }, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
}

func main() {
    // ...
    if err := updateUserBalance(ctx, db, 42, 100); err != nil {
        log.Printf("update failed: %v\n", err)
    }
}
```

### Пример на Java с {{ ydb-short-name }} Native SDK

```java
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.transaction.TxControl;
import com.yandex.ydb.table.transaction.Transaction;

public class TLIHandler {
    
    public static void updateUserBalanceWithRetry(TableClient client, String userId, int amount) {
        RetrySettings retrySettings = RetrySettings.newBuilder()
            .maxRetries(3)
            .idempotent(true)
            .build();
            
        Retry.retryWithSettings(retrySettings, () -> {
            try (Session session = client.createSession().join()) {
                Transaction tx = session.beginTransaction().join();
                
                // Выполняем операции в транзакции
                ResultSetReader reader = session.executeDataQuery(
                    "SELECT balance FROM users WHERE id = ?",
                    TxControl.tx(tx),
                    Params.of("id", Value.ofUtf8(userId))
                ).join().getResultSet(0);
                
                if (reader.next()) {
                    int currentBalance = reader.getColumn("balance").getInt32();
                    int newBalance = currentBalance + amount;
                    
                    session.executeDataQuery(
                        "UPDATE users SET balance = ? WHERE id = ?",
                        TxControl.tx(tx),
                        Params.of(
                            "balance", Value.ofInt32(newBalance),
                            "id", Value.ofUtf8(userId)
                        )
                    ).join();
                }
                
                tx.commit().join();
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
