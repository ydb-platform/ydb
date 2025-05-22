# Вставка и модификация данных с помощью UPSERT

{% note warning %}

{% include [not_allow_for_olap](../../_includes/not_allow_for_olap_text.md) %}

{% include [not_allow_for_olap](../../_includes/ways_add_data_to_olap.md) %}

{% endnote %}

Добавьте данные в строковою таблицу с помощью конструкции [UPSERT INTO](../../yql/reference/syntax/upsert_into.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```yql
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

-- Посмотреть результат:
SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5;
```
