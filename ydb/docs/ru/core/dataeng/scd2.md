# Реализация SCD2 в YDB

В этой статье описывается реализация паттерна Slowly Changing Dimensions Type 2 (SCD2) в YDB с использованием процесса подмерживания изменений.

## Что такое SCD2 и когда его использовать

SCD2 (Type 2) — это подход к хранению данных, при котором при изменении атрибута измерения создается новая запись, а старая сохраняется, таким образом, формируется полная история изменений. Этот подход используется, когда:

- Требуется отслеживать историю изменений данных
- Необходимо выполнять анализ данных с учетом временных периодов
- Требуется возможность восстановления состояния данных на определенный момент времени

## Особенности реализации SCD2 в YDB

В {{ ydb-short-name }} нет встроенной команды `MERGE` для подмерживания изменений в таблицы хранения данных. Поэтому для реализации SCD2 необходимо написать SQL-запрос, переносящий данные из таблицы изменений в целевую таблицу хранения.

## Описание процесса

### Структура таблиц

Для реализации подхода используются две таблицы:


1. Таблица изменений (`dimension_scd_changes`) — для приёма всех изменений;
2. Финальная SCD2-таблица (`dimension_scd2_final`) — для хранения данных в SCD2-формате.

**Таблица изменений:**

```sql
CREATE TABLE dimension_scd_changes (
    id Utf8 NOT NULL,
    attribute1 Utf8,
    attribute2 Utf8,
    change_time Timestamp NOT NULL,
    operation Utf8,
    row_operation_at Timestamp,
    PRIMARY KEY (change_time, id)
)
```

**Финальная SCD2-таблица:**

```sql
CREATE TABLE dimension_scd2_final (
    id Utf8 NOT NULL,
    attribute1 Utf8,
    attribute2 Utf8,
    valid_from Timestamp NOT NULL,
    valid_to Timestamp,
    is_current Uint8,
    is_deleted Uint8,
    PRIMARY KEY (valid_from, id)
)
PARTITION BY HASH(valid_from, id)
WITH(STORE=COLUMN)
```

Ключевые поля финальной таблицы:
- `id` — бизнес-ключ записи;
- `attribute1`, `attribute2` — атрибуты измерения;
- `valid_from` — момент времени, с которого запись стала актуальной;
- `valid_to` — момент времени, до которого запись была актуальной (NULL для текущих записей);
- `is_current` — флаг, указывающий, является ли запись текущей (1) или исторической (0);
- `is_deleted` — флаг, указывающий, была ли запись удалена (1) или нет (0).

### Загрузка данных в таблицу изменений

Для загрузки данных в таблицу изменений можно использовать любой способ загрузки данных и автоматическую поставку изменений с помощью механизма [TRANSFER](transfer/index.md). Пример трансфера, поставляющего данные в формате, подходящем для данного примера, описан в разделе [{#T}](transfer/scd2.md#scd2).

### Скрипт поддержания данных (мерджинг изменений)

Для преобразования данных из таблицы изменений в формат SCD2 и загрузки в финальную таблицу используется следующий скрипт:

```sql
-- Шаг 1: Читаем все новые события из стейджинг-таблицы (`dimension_scd_changes`).
-- Этот CTE ($changes) является исходным набором данных для всей последующей обработки в рамках этого запуска.
$changes = (
    SELECT
        id,
        attribute1,
        attribute2,
        change_time,
        String::AsciiToUpper(operation) AS op
    FROM dimension_scd_changes
    -- При необходимости можно ограничить обрабатываемое окно по времени:
    -- WHERE change_time > $from_ts AND change_time <= $to_ts
);

-- Шаг 2: Фильтруем события, оставляя только те, которых еще нет в целевой таблице.
-- Цель этого шага - обеспечить идемпотентность на уровне чтения, чтобы не обрабатывать
-- уже загруженные данные в случае сбоя и перезапуска скрипта.
$unprocessed_data = (
    SELECT
        chg.id AS id,
        chg.attribute1 AS attribute1,
        chg.attribute2 AS attribute2,
        chg.change_time AS change_time,
        chg.op AS op
    FROM $changes AS chg
    LEFT OUTER JOIN dimension_scd2_final AS scd
        ON chg.id = scd.id AND chg.change_time = scd.valid_from
    WHERE scd.id IS NULL
);

-- Шаг 3: Находим в целевой таблице активные записи (`is_current=1`), для которых пришли обновления.
-- Формируем для них "закрывающие" версии, устанавливая `valid_to` равным времени
-- самого первого изменения из новой пачки ($unprocessed_data).
$close_open_intervals = (
    SELECT
        target.id AS id,
        target.attribute1 as attribute1,
        target.attribute2 as attribute2,
        target.valid_from as valid_from,
        0ut AS is_current, -- Закрываемая запись больше не является текущей
        d.change_time AS valid_to,
        target.is_deleted as is_deleted
    FROM dimension_scd2_final AS target
    INNER JOIN (
        SELECT
            id,
            MIN(change_time) AS change_time
        FROM $unprocessed_data
        GROUP BY id
    ) AS d
    ON target.id = d.id
    WHERE target.is_current = 1ut
);

-- Шаг 4: Преобразуем поток необработанных событий в версионные записи (строки для вставки).
-- Здесь вычисляются все необходимые атрибуты для новых версий: `valid_to`, `is_current`, `is_deleted`.
$updated_data = (
    SELECT
        t.id AS id,
        t.attribute1 AS attribute1,
        t.attribute2 AS attribute2,
        t.is_deleted AS is_deleted,
        -- Логика флага `is_current`: он устанавливается в 1 только для последней
        -- записи в цепочке (`next_change_time IS NULL`), и только если это не
        -- операция удаления (`is_deleted == 0`).
        IF(t.next_change_time IS NOT NULL, 0ut, IF(t.is_deleted == 1ut, 0ut, 1ut)) AS is_current,
        t.change_time AS valid_from,
        t.next_change_time AS valid_to
    FROM (
        -- Внутренний запрос вычисляет для каждой строки флаг удаления (`is_deleted`)
        -- и временную метку следующего события (`next_change_time`) с помощью оконной функции LEAD.
        SELECT
            d.id AS id,
            d.attribute1 AS attribute1,
            d.attribute2 AS attribute2,
            d.op AS op,
            d.change_time AS change_time,
            IF(d.op = "DELETE", 1ut, 0ut) AS is_deleted,
            LEAD(d.change_time) OVER (PARTITION BY id ORDER BY d.change_time) AS next_change_time
        FROM $unprocessed_data AS d
    ) AS t
);

-- Шаг 5: Атомарно применяем все рассчитанные изменения к целевой таблице.
-- UPSERT обновит существующие записи (из $close_open_intervals) и вставит новые (из $updated_data).
UPSERT INTO dimension_scd2_final (id, attribute1, attribute2, is_current, is_deleted, valid_from, valid_to)
SELECT * FROM (
SELECT
    id,
    attribute1,
    attribute2,
    is_current,
    is_deleted,
    valid_from,
    valid_to
FROM $close_open_intervals
UNION ALL
SELECT
    id,
    attribute1,
    attribute2,
    is_current,
    is_deleted,
    valid_from,
    valid_to
FROM $updated_data);

-- Шаг 6: Очищает стейджинг-таблицу от обработанных записей.
-- ВАЖНО: для надежной работы этот шаг должен быть идемпотентным и удалять
-- только те записи, которые были прочитаны в CTE `$changes`.
DELETE FROM dimension_scd_changes ON
SELECT id, change_time FROM $changes;
```

Пояснения к скрипту поддержания данных:

1. **Удалённые записи не считаются текущими**: при удалении записи `is_current` устанавливается в `0`. Это делается, чтобы упростить логику чтения данных.

1. **Чтение новых событий**: Скрипт начинается с чтения всех новых событий из таблицы изменений.

1. **Фильтрация уже обработанных событий**: Затем отфильтровываются события, которые уже были обработаны и существуют в финальной таблице.

1. **Закрытие открытых интервалов**: Для активных записей, для которых пришли обновления, создаются «закрывающие» версии, устанавливая `valid_to` равным времени самого первого изменения из новой пачки и `is_current = 0`.

1. **Преобразование событий в версионные записи**: События преобразуются в версионные записи с вычислением всех необходимых атрибутов: `valid_to`, `is_current`, `is_deleted`.

1. **Применение изменений**: Все рассчитанные изменения атомарно применяются к финальной таблице с помощью UPSERT.

1. **Очистка таблицы изменений**: После успешного применения изменений, обработанные записи удаляются из таблицы изменений.

## Пример ленты версий

В примере `attribute1` — полное имя, `attribute2` — город.

* `t0 = 2025-08-22 17:00`
* `t1 = 2025-08-22 19:00`
* `t2 = 2025-08-22 21:00`

### События (staging: `dimension_scd_changes`)

| id             | attribute1 | attribute2    | change\_time     | operation |
| -------------- | ---------- | ------------- | ---------------- | --------- |
| CUSTOMER\_1001 | John Doe   | Los Angeles   | 2025-08-22 17:00 | CREATE    |
| CUSTOMER\_1002 | Judy Doe   | New York      | 2025-08-22 17:00 | CREATE    |
| CUSTOMER\_1001 | John Doe   | San Francisco | 2025-08-22 19:00 | UPDATE    |
| CUSTOMER\_1002 | Judy Doe   | New York      | 2025-08-22 21:00 | DELETE    |

### Итоговые версии (final: `dimension_scd2_final`)


| id             | attribute1 | attribute2    | valid\_from      | valid\_to        | is\_current | is\_deleted |
| -------------- | ---------- | ------------- | ---------------- | ---------------- | ----------- | ----------- |
| CUSTOMER\_1001 | John Doe   | Los Angeles   | 2025-08-22 17:00 | 2025-08-22 19:00 | 0           | 0           |
| CUSTOMER\_1001 | John Doe   | San Francisco | 2025-08-22 19:00 | NULL             | 1           | 0           |
| CUSTOMER\_1002 | Judy Doe   | New York      | 2025-08-22 17:00 | 2025-08-22 21:00 | 0           | 0           |
| CUSTOMER\_1002 | Judy Doe   | New York      | 2025-08-22 21:00 | NULL             | 0           | 1           |

Визуальное представление:

```text
John/LA   [2025-08-22 17:00)──────────┐
John/SF                      [2025-08-22 19:00)── … (текущее)
```

```text
Judy/NY  [2025-08-22 17:00)─────────┐
(DELETE)                   [2025-08-22 21:00)── … (удаление)
```

## Получение данных из SCD2 таблицы

### Получение актуальных данных

```sql
SELECT
    id,
    attribute1,
    attribute2,
    valid_from,
    valid_to
FROM dimension_scd2_final
WHERE is_current = 1ut;
```

### Получение данных на определенный момент времени

```sql
DECLARE $as_of AS Timestamp;
$as_of = Datetime::MakeDatetime(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2025-08-22 19:11:30"));

SELECT
    id,
    attribute1,
    attribute2,
    valid_from,
    valid_to
FROM dimension_scd2_final
WHERE valid_from <= $as_of
    AND (valid_to IS NULL OR valid_to > $as_of)
    AND (is_deleted = 0ut OR (is_deleted = 1ut AND valid_from <= $as_of AND valid_to > $as_of));
```

### Получение истории изменений для конкретной записи

```sql
SELECT
    id,
    attribute1,
    attribute2,
    valid_from,
    valid_to,
    is_current,
    is_deleted
FROM dimension_scd2_final
WHERE id = 'CUSTOMER_1001'
ORDER BY valid_from;
```
