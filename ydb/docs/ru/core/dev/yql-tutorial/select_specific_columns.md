# Выборка данных из определенных колонок

Выберите данные из колонок `series_id`, `release_date` и `title`. При этом переименуйте `title` в `series_title` и преобразуйте тип `release_date` из `Uint32` в `Date`.

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
SELECT
    series_id,             -- Имена колонок (series_id, release_date, title)
                           -- перечисляются через запятую.

    title AS series_title, -- С помощью AS можно переименовать столбцы
                           -- или дать имя произвольному выражению

    CAST(release_date AS Date) AS release_date

FROM series;

COMMIT;
```

