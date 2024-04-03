# Обновление данных с помощью UPDATE

Обновите данные в таблице с помощью оператора [UPDATE](../../yql/reference/syntax/update.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
UPDATE episodes
SET title="test Episode 2"
WHERE
    series_id = 2
    AND season_id = 5
    AND episode_id = 12
;

COMMIT;

-- Посмотреть результат:
SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5;

-- YDB не знает об изменениях, имевших место в начале транзакции,
-- поэтому сначала выполняет чтение. Невозможно обновить или удалить таблицу,
-- которая уже была изменена в рамках текущей транзакции. UPDATE ON и
-- DELETE ON позволяют читать, обновлять и удалять несколько строк в таблице
-- в рамках одной транзакции.

$to_update = (
    SELECT series_id,
           season_id,
           episode_id,
           Utf8("Yesterday's Jam UPDATED") AS title
    FROM episodes
    WHERE series_id = 1 AND season_id = 1 AND episode_id = 1
);

SELECT * FROM episodes WHERE series_id = 1 AND season_id = 1;

UPDATE episodes ON
SELECT * FROM $to_update;

COMMIT;

-- Посмотреть результат:
SELECT * FROM episodes WHERE series_id = 1 AND season_id = 1;

COMMIT;
```
