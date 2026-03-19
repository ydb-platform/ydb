# Совмещение последнего запроса в транзакции с коммитом

## Проблема

Коммит транзакции явным вызовом `Commit` создает дополнительный сетевой запрос (хоп) к серверу {{ ydb-short-name }}, что увеличивает латентность всей транзакции. Каждый дополнительный сетевой запрос добавляет задержку, особенно заметную при работе с распределенными системами.

В {{ ydb-short-name }} все изменения, производимые в рамках транзакции, накапливаются в памяти сервера базы данных и применяются в момент завершения транзакции. Явный вызов `Commit` после последнего запроса создает лишний сетевой round-trip, который можно избежать.

## Решение

Выполнение последнего запроса с коммитом транзакции вместо явного `Commit` позволяет избежать лишнего сетевого запроса и снизить latency. Используйте флаг `WithCommit` (или аналогичный в вашем SDK) в последнем запросе вместо явного `Commit`, чтобы избежать лишнего хопа и снизить латентность всей транзакции.

**Рекомендации:**
- Комбинируйте последний запрос в транзакции с коммитом
- Используйте соответствующие флаги в SDK (например, `WithCommit`, `commit_tx=True`)
- Это особенно важно для транзакций с высокой частотой выполнения
- Помните, что при ошибке в последнем запросе транзакция все равно будет откачена


{% list tabs %}

- Go

  {% cut "Плохой пример" %}
  ```go
// Плохой пример: явный коммит после запроса
err := db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
    tx, err := s.Begin(ctx, query.TxSettings(query.WithDefaultTxMode()))
    if err != nil {
        return fmt.Errorf("failed start transaction: %w", err)
    }
    defer tx.Rollback(ctx)

    // Выполнение запросов
    _, err = tx.Exec(ctx, "UPDATE users SET status = 'active' WHERE id = 1")
    if err != nil {
        tx.Rollback(ctx)
        return err
    }

    _, err = tx.Exec(ctx, 
        "INSERT INTO logs (user_id, action) VALUES (1, 'activated')",
        query.WithCommit(), // Коммит вместе с запросом
    )

    // Явный коммит создает лишний сетевой запрос
    return tx.CommitTx(ctx)
})
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
// Хороший пример: коммит вместе с последним запросом
err := db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
    tx, err := s.Begin(ctx, query.TxSettings(query.WithDefaultTxMode()))
    if err != nil {
        return fmt.Errorf("failed start transaction: %w", err)
    }

    // Первые запросы без коммита
    _, err = tx.Exec(ctx, "UPDATE users SET status = 'active' WHERE id = 1")
    if err != nil {
        tx.Rollback(ctx)
        return err
    }

    // Последний запрос с коммитом - избегаем лишнего хопа
    _, err = tx.Exec(ctx, 
        "INSERT INTO logs (user_id, action) VALUES (1, 'activated')",
        query.WithCommit(), // Коммит вместе с запросом
    )

    return err
})
  ```
  {% endcut %}

- Python

  {% cut "Плохой пример" %}
  ```python
# Плохой пример: явный коммит
tx = session.transaction()
try:
    tx.execute("UPDATE users SET status = 'active' WHERE id = $id", {"$id": user_id})
    tx.execute("INSERT INTO logs (user_id, action) VALUES ($id, 'activated')", {"$id": user_id})
    tx.commit()  # Лишний сетевой запрос
except Exception as e:
    tx.rollback()
    raise
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```python
# Хороший пример: коммит с последним запросом
tx = session.transaction()
try:
    tx.execute("UPDATE users SET status = 'active' WHERE id = $id", {"$id": user_id})
    # Последний запрос с коммитом
    tx.execute(
        "INSERT INTO logs (user_id, action) VALUES ($id, 'activated')",
        {"$id": user_id},
        commit_tx=True  # Коммит вместе с запросом
    )
except Exception as e:
    tx.rollback()
    raise
  ```
  {% endcut %}

{% endlist %}
