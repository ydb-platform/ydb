# Вставка и модификация данных с помощью REPLACE

{% note warning %}

{% include [column-and-row-tables-in-read-only-tx](../../concepts/_includes/limitation-column-row-in-read-only-tx.md) %}

{% endnote %}

Добавьте новые данные в таблицы с одновременным обновлением уже существующих данных с помощью конструкции [REPLACE INTO](../../yql/reference/syntax/replace_into.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```yql
REPLACE INTO episodes
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
    12,
    "Test Episode !!!",
    CAST(Date("2018-08-27") AS Uint64)
)
;

-- Вызов COMMIT используется, чтобы следующей операции SELECT
-- были видны изменения, сделанные в рамках предыдущей транзакции.
COMMIT;

-- Посмотреть результат:
SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5;
```
