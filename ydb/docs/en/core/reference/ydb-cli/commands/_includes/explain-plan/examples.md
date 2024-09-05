## Example of query modification {#examples}

Adjust the query so that you get only the first seasons of all the series:

```bash
{{ ydb-cli }} table query explain \
  -q "SELECT sa.title AS season_title, sr.title AS series_title, sr.series_id, sa.season_id
  FROM seasons AS sa
  INNER JOIN series AS sr ON sa.series_id = sr.series_id
  WHERE sa.season_id = 1"
```

