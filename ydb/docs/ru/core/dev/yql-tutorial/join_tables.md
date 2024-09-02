# Объединение таблиц с помощью JOIN

Объедините колонки исходных таблиц `seasons` и `series` и выведите все сезоны сериала IT Crowd в результирующей таблице с помощью оператора [JOIN](../../yql/reference/syntax/join.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
SELECT
    sa.title AS season_title,    -- sa и sr — это «связующие названия»,
    sr.title AS series_title,    -- алиасы таблиц, объявленные ниже с помощью AS.
    sr.series_id,                -- Они используются, чтобы избежать
    sa.season_id                 -- неоднозначности в именах указанных колонок.

FROM
    seasons AS sa
INNER JOIN
    series AS sr
ON sa.series_id = sr.series_id
WHERE sa.series_id = 1
ORDER BY                         -- Cортировка результатов.
    sr.series_id,
    sa.season_id                 -- ORDER BY сортирует значения по одному
;                                -- или нескольким столбцам.
                                 -- Столбцы перечисляются через запятую.

COMMIT;
```
