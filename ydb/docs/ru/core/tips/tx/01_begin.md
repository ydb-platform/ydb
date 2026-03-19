# Совмещение начала транзакции с первым запросом

## Проблема

Открытие транзакции явным вызовом `BeginTx` создает дополнительный сетевой запрос (хоп) к серверу {{ ydb-short-name }}, что увеличивает латентность всей транзакции. Каждый дополнительный сетевой запрос добавляет задержку, особенно заметную при работе с распределенными системами, где время отклика критично для производительности.

В {{ ydb-short-name }} транзакции по умолчанию выполняются в режиме **Serializable**, который предоставляет самый строгий уровень изоляции.

## Решение

Выполнение первого запроса с открытием транзакции вместо явного `BeginTx` позволяет избежать лишнего сетевого запроса и снизить latency. {{ ydb-short-name }} поддерживает неявные транзакции, когда режим определяется автоматически на основе типа запроса. 

**Рекомендации:**
- Делайте первый запрос с открытием транзакции вместо явного `BeginTx`
- Используйте `LazyTx` (если доступно в вашем SDK) для автоматической оптимизации
- Формируйте транзакции таким образом, чтобы в первой части транзакции выполнялись только чтения, а во второй части транзакции только модификации


{% list tabs %}

- Go

  {% cut "Плохой пример" %}
  ```go
// Плохой пример: явное открытие транзакции
tx, err := session.BeginTransaction(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

result, err := tx.Execute(ctx, `
    SELECT * FROM users WHERE id = $id
`, table.NewQueryParameters(
    table.ValueParam("$id", types.Int64Value(123)),
))
if err != nil {
    return err
}
// ... обработка результата
return tx.Commit(ctx)
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
// Хороший пример: открытие транзакции с первым запросом
result, err := session.Execute(ctx, `
    SELECT * FROM users WHERE id = $id
`, table.NewQueryParameters(
    table.ValueParam("$id", types.Int64Value(123)),
), table.WithTxControl(table.BeginTx(table.WithSerializableReadWrite())))
if err != nil {
    return err
}
// ... обработка результата
// Транзакция автоматически открыта и может быть продолжена
  ```
  {% endcut %}

- Python

  {% cut "Плохой пример" %}
  ```python
# Плохой пример: явное открытие транзакции
tx = session.transaction()
try:
    tx.begin()
    result = tx.execute("SELECT * FROM users WHERE id = $id", {"$id": 123})
    # ... обработка результата
    tx.commit()
except Exception as e:
    tx.rollback()
    raise
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```python
# Хороший пример: открытие транзакции с первым запросом (без явного begin())
tx = session.transaction()
result = tx.execute(
    "SELECT * FROM users WHERE id = $id",
    {"$id": 123},
    commit_tx=False  # Транзакция открывается автоматически, без явного begin()
)
# ... обработка результата
tx.commit()
  ```
  {% endcut %}

{% endlist %}
