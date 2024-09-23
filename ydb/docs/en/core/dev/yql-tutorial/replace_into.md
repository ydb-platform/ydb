# Inserting and updating data with REPLACE

Add data to the table using [REPLACE INTO](../../yql/reference/syntax/replace_into.md):

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
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

-- COMMIT is called so that the next SELECT operation
-- can see the changes made by the previous transaction.
COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5;

COMMIT;
```

