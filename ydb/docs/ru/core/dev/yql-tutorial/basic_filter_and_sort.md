# Сортировка и фильтрация

Выберите первые три эпизода из всех сезонов IT Crowd за исключением первого.

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
SELECT
   series_id,
   season_id,
   episode_id,
   CAST(air_date AS Date) AS air_date,
   title

FROM episodes
WHERE
   series_id = 1      -- Список условий для формирования результата.
   AND season_id > 1  -- Логическое И (AND) используется для написания сложных условий.

ORDER BY              -- Сортировка результатов.
   series_id,         -- ORDER BY сортирует значения по одному или нескольким
   season_id,         -- столбцам. Столбцы перечисляются через запятую.
   episode_id

LIMIT 3               -- LIMIT N после ORDER BY означает
                      -- «получить первые N» или «последние N» результатов
;                     -- в зависимости от порядка сортировки.

COMMIT;
```