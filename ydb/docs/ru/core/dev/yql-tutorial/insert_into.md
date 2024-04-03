# Вставка данных с помощью INSERT

Добавьте данные в таблицу с помощью конструкции [INSERT INTO](../../yql/reference/syntax/insert_into.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
INSERT INTO episodes
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    21,
    "Test 21",
    CAST(Date("2018-08-27") AS Uint64)
),                                        -- Строки разделяются запятыми.
(
    2,
    5,
    22,
    "Test 22",
    CAST(Date("2018-08-27") AS Uint64)
)
;

COMMIT;

-- Посмотреть результат:
SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5;

COMMIT;
```
