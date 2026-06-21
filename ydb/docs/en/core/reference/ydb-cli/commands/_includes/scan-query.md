# Performing scan queries

You can run a query using [Scan Queries](../../../../concepts/scan_query.md) via the {{ ydb-short-name }} CLI by adding the `-t scan` flag to the `{{ ydb-cli }} table query execute` command.

<<<<<<< HEAD
Run the data query:
=======
To run a query via [Scan Queries](../../../../concepts/query_execution/scan_query.md) using {{ ydb-short-name }} CLI, add the `-t scan` flag to the `{{ ydb-cli }} table query execute` command.

Run a query against the data:
>>>>>>> 48da0d6260a ([YDBDOCS-2232] перенести статью https://github.com/ydb-platform/ydb/pull/41475 в "Выполнение запросов" (#43439))

```bash
{{ ydb-cli }} table query execute -t scan \
  --query "SELECT season_id, episode_id, title \
  FROM episodes \
  WHERE series_id = 1 AND season_id > 1 \
  ORDER BY season_id, episode_id \
  LIMIT 3"
```

Where:

* `--query`: Query text.

Result:

```text
┌───────────┬────────────┬──────────────────────────────┐
| season_id | episode_id | title                        |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 1          | "The Work Outing"            |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 2          | "Return of the Golden Child" |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 3          | "Moss and the German"        |
└───────────┴────────────┴──────────────────────────────┘
```

