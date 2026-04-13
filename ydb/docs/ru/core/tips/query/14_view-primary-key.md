# Явное указание индекса через VIEW PRIMARY KEY

## Проблема

{{ ydb-short-name }} имеет автоматический механизм выбора индексов при выполнении запросов, который работает на основе rule-based алгоритма. Однако этот алгоритм может выбирать неоптимальные индексы в сложных сценариях, особенно когда:

- В таблице есть несколько вторичных индексов
- Запрос содержит JOIN с несколькими таблицами
- Условия фильтрации затрагивают разные комбинации колонок
- Оптимизатор ошибочно выбирает вторичный индекс вместо первичного ключа

В таких случаях запрос может выполняться значительно медленнее из-за неоптимального плана выполнения.

## Решение

Для принудительного указания использования первичного ключа вместо автоматического выбора индекса используется конструкция `VIEW PRIMARY KEY`. Это гарантирует, что запрос будет выполняться с использованием первичного индекса таблицы, что особенно важно в следующих сценариях:

1. Когда известно, что первичный ключ является оптимальным выбором
2. Когда нужно избежать неожиданного выбора вторичного индекса
3. В сложных JOIN-запросах, где оптимизатор может ошибиться

## Примеры

### Плохой подход (без явного указания индекса)

```sql
-- Запрос может выбрать неоптимальный вторичный индекс
SELECT
    ow.id AS id,
    ow.type AS method,
    pw.withdrawal_type AS type,
    ow.status AS status,
    ow.data AS data,
    ow.started_at AS started_at,
    NULL AS finished_at,
    pw.participation_id AS participation_id
FROM
    (SELECT
        withdrawal_id,
        participation_id,
        withdrawal_type
    FROM
        participation_withdrawals) pw
JOIN
    ongoing_withdrawals ow
ON
    (ow.id == pw.withdrawal_id);
```

### Хороший подход (с явным указанием первичного ключа)

```sql
-- Гарантированное использование первичного ключа
SELECT
    ow.id AS id,
    ow.type AS method,
    pw.withdrawal_type AS type,
    ow.status AS status,
    ow.data AS data,
    ow.started_at AS started_at,
    NULL AS finished_at,
    pw.participation_id AS participation_id
FROM
    (SELECT
        withdrawal_id,
        participation_id,
        withdrawal_type
    FROM
        participation_withdrawals VIEW PRIMARY KEY) pw
JOIN
    ongoing_withdrawals ow
ON
    (ow.id == pw.withdrawal_id);
```

### Дополнительные примеры

```sql
-- Простой SELECT с явным указанием первичного ключа
SELECT * FROM `Table` VIEW PRIMARY KEY WHERE key_column = $value;

-- Сравнение с использованием вторичного индекса
SELECT * FROM `Table` VIEW secondary_index WHERE indexed_column = $value;

-- Без указания индекса (автоматический выбор)
SELECT * FROM `Table` WHERE some_column = $value;
```

## Когда использовать

Используйте `VIEW PRIMARY KEY` когда:

1. **Известно, что первичный ключ оптимален** - например, при точечных запросах по полному первичному ключу
2. **Нужна предсказуемость** - когда важно гарантировать определенный план выполнения
3. **Проблемы с производительностью** - если автоматический выбор индекса приводит к деградации
4. **Сложные JOIN-запросы** - где оптимизатор может ошибиться в выборе индекса

## Примечания

- Конструкция `VIEW PRIMARY KEY` имеет приоритет над автоматическим выбором индекса
- Используется только для чтения данных (SELECT запросы)
- Не влияет на операции модификации данных (INSERT, UPDATE, DELETE)
- Рекомендуется использовать после анализа плана выполнения запроса
