# Inserting and updating data with UPSERT

Add data to the table using [UPSERT INTO](../../yql/reference/syntax/upsert_into.md):

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
UPSERT INTO episodes
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
    13,
    "Test Episode",
    CAST(Date("2018-08-27") AS Uint64)
)
;

COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5;

COMMIT;
```

