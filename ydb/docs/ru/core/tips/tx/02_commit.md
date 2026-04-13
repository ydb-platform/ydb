# Совмещение последнего запроса в транзакции с коммитом

## Проблема

Коммит транзакции отдельным вызовом `Commit` создает дополнительный сетевой запрос (хоп) к серверу {{ ydb-short-name }} и увеличивает общую задержку выполнения транзакции. Каждый дополнительный сетевой запрос добавляет задержку, особенно заметную при работе с распределенными системами.

В {{ ydb-short-name }} изменения, сделанные внутри транзакции, накапливаются на стороне сервера и применяются при её завершении. Поэтому отдельный вызов `Commit` после последнего запроса часто означает просто ещё один лишний round-trip.

## Решение

Если совместить последний запрос с коммитом, можно убрать один лишний сетевой запрос. Используйте флаг `WithCommit` или его аналог в вашем SDK в последнем запросе вместо отдельного вызова `Commit`.

**Рекомендации:**
- Комбинируйте последний запрос в транзакции с коммитом.
- Используйте соответствующие флаги в SDK, например `WithCommit` или `commit_tx=True`.
- Это особенно важно для транзакций с высокой частотой выполнения.
- Помните, что при ошибке в последнем запросе транзакция все равно будет отменена.


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
    )
    if err != nil {
        tx.Rollback(ctx)
        return err
    }

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
