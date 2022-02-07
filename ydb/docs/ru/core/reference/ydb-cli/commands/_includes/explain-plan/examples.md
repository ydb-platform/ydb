## Пример модификации запроса {#examples}

Измените запрос так, чтобы получить только первые сезоны всех сериалов:

```bash
{{ ydb-cli }} table query explain \
  -q "SELECT sa.title AS season_title, sr.title AS series_title, sr.series_id, sa.season_id 
  FROM seasons AS sa 
  INNER JOIN series AS sr ON sa.series_id = sr.series_id 
  WHERE sa.season_id = 1"
```
