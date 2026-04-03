# Вставка данных в таблицы с синхронными индексами

## Проблема

Синхронные вторичные индексы в {{ ydb-short-name }} обеспечивают консистентность данных, но усложняют массовую вставку. Основные последствия:

1. **Невозможность использования `BulkUpsert`** - метод `BulkUpsert` в настоящее время не поддерживается для таблиц с синхронными индексами
2. **Снижение производительности** - каждая вставка требует обновления как основной таблицы, так и индексных таблиц
3. **Увеличенная вероятность ошибок TLI (`TransactionLocksInvalidated`)** - чем дольше выполняется транзакция, тем выше вероятность конфликтов блокировок.

## Решение

### Стратегия 1: Используйте `UPSERT` в транзакционных запросах

Для таблиц с синхронными индексами вместо `BulkUpsert` используйте `UPSERT` в обычных транзакционных запросах.

#### ❌ Плохой пример: попытка использовать `BulkUpsert`

```python
# Этот код вызовет ошибку для таблиц с синхронными индексами
try:
    session.bulk_upsert(table_path, data_rows)
except Exception as e:
    # Получим ошибку: "BulkUpsert не поддерживается для таблиц с синхронными индексами"
    print(f"Ошибка: {e}")
```

#### ✅ Хороший пример: транзакционный `UPSERT`

```sql
-- Один запрос с несколькими строками
DECLARE $records AS List<Struct<
    id: Uint64,
    name: Utf8,
    email: Utf8,
    created_at: Timestamp
>>;

UPSERT INTO users (id, name, email, created_at)
SELECT id, name, email, created_at
FROM AS_TABLE($records);
```

```python
# Python SDK пример
def insert_users_batch(session, users_data):
    query = """
    DECLARE $users AS List<Struct<
        id: Uint64,
        name: Utf8,
        email: Utf8,
        created_at: Timestamp
    >>;
    
    UPSERT INTO users (id, name, email, created_at)
    SELECT id, name, email, created_at FROM AS_TABLE($users);
    """
    
    return session.transaction().execute(
        query,
        commit_tx=True,
        parameters={'$users': users_data}
    )
```

### Стратегия 2: Подберите размер батча

Не отправляйте слишком большой объём данных в одной транзакции. Разбивайте загрузку на батчи разумного размера:

#### ❌ Плохой пример: слишком большие транзакции

```python
# Слишком большая транзакция повышает риск TLI
def insert_all_users(users):
    # 10,000 записей в одной транзакции - риск TLI
    return insert_users_batch(session, users[:10000])
```

#### ✅ Хороший пример: умеренный размер батча

```python
def insert_users_optimized(users, batch_size=500):
    """Вставка с оптимальным размером батча"""
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        try:
            insert_users_batch(session, batch)
        except ydb.issues.TransactionLocksInvalidated:
            # Ретрай при TLI ошибках
            time.sleep(0.1)
            insert_users_batch(session, batch)
```

### Стратегия 3: Выбирайте между `UPSERT` и `INSERT` осознанно

{% note info %}

При большом количестве вторичных индексов `INSERT` может работать быстрее, чем `UPSERT`. Но, не являясь идемпотентной операцией, `INSERT` может приводить к ошибкам дублирования при повторных вызовах. Поэтому выбор между `UPSERT` и `INSERT` здесь всегда является компромиссом между производительностью и идемпотентностью.

{% endnote %}

Если операция может быть повторена, безопаснее использовать `UPSERT`:

#### ❌ Плохой пример: риск дублирования

```sql
INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');
-- При ретрае получим ошибку дублирования ключа
```

#### ✅ Хороший пример: идемпотентный `UPSERT`

```sql
UPSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');
-- Безопасен при ретраях
```

### Стратегия 4: Для первичной массовой загрузки временно убирайте индексы

Если нужно один раз загрузить большой объём данных, можно использовать такой подход:

1. Удалите синхронные индексы
2. Выполните загрузку с помощью BulkUpsert
3. Восстановите индексы

#### ✅ Пример стратегии массовой загрузки

```sql
-- Шаг 1: Удаление индекса
DROP INDEX idx_user_email ON users;

-- Шаг 2: Массовая загрузка (используя BulkUpsert или оптимальные батчи)
-- ... загрузка данных ...

-- Шаг 3: Восстановление индекса
CREATE INDEX idx_user_email ON users (email);
```

## Практические рекомендации

1. **Размер батча**: в качестве отправной точки используйте 100-1000 строк на транзакцию и подбирайте значение по нагрузке.
2. **Порядок данных**: по возможности группируйте или сортируйте записи по первичному ключу, чтобы уменьшить разброс по шардам.
3. **Ретраи**: обрабатывайте ошибки TLI и используйте экспоненциальную задержку между повторами.
4. **Мониторинг**: отслеживайте частоту TLI-ошибок и рост задержек записи.

## Пример полного решения на Go

```go
func InsertWithSyncIndex(db *ydb.Driver, users []User) error {
    const batchSize = 500
    maxRetries := 3
    
    for i := 0; i < len(users); i += batchSize {
        end := i + batchSize
        if end > len(users) {
            end = len(users)
        }
        batch := users[i:end]
        
        err := retryWithBackoff(maxRetries, func() error {
            return db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
                // Подготовка параметров
                params := table.NewQueryParameters(
                    table.ValueParam("$users", prepareUserList(batch)),
                )
                
                // Выполнение UPSERT
                _, err := tx.Execute(ctx, `
                    DECLARE $users AS List<Struct<
                        id: Uint64,
                        name: Utf8,
                        email: Utf8
                    >>;
                    
                    UPSERT INTO users SELECT * FROM AS_TABLE($users);
                `, params)
                return err
            }, table.WithIdempotent())
        })
        
        if err != nil {
            return fmt.Errorf("failed to insert batch: %w", err)
        }
    }
    return nil
}
```

Эти рекомендации помогают снизить нагрузку на таблицы с синхронными индексами, уменьшить задержки записи и реже сталкиваться с ошибками TLI.
