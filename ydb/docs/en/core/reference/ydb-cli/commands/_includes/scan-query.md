<<<<<<< HEAD
# Performing scan queries

{% include notitle [warning](../../_includes/deprecated_command_warning.md) %}

You can run a query using [Scan Queries](../../../../concepts/scan_query.md) via the {{ ydb-short-name }} CLI by adding the `-t scan` flag to the `{{ ydb-cli }} table query execute` command.

Run the data query:

```bash
{{ ydb-cli }} table query execute -t scan \
  --query "SELECT season_id, episode_id, title \
  FROM episodes \
  WHERE series_id = 1 AND season_id > 1 \
  ORDER BY season_id, episode_id \
  LIMIT 3"
=======
# Running scan queries

{% include notitle [warning](../../_includes/deprecated_command_warning.md) %}

To run a query via [Scan Queries](../../../../concepts/scan_query.md) using {{ ydb-short-name }} CLI, add the `-t scan` flag to the `{{ ydb-cli }} table query execute` command.

Run a query against the data:

```bash
{{ ydb-cli }} table query execute -t scan \
 --query "SELECT season_id, episode_id, title \
 FROM episodes \
 WHERE series_id = 1 AND season_id > 1 \
 ORDER BY season_id, episode_id \
 LIMIT 3"
>>>>>>> 23d71c75863 ([YDBDOCS-2043] Вернуть описание scan_queries (#38583))
```

Where:

<<<<<<< HEAD
* `--query`: Query text.
=======
* `--query` — query text.
>>>>>>> 23d71c75863 ([YDBDOCS-2043] Вернуть описание scan_queries (#38583))

Result:

```text
┌───────────┬────────────┬──────────────────────────────┐
<<<<<<< HEAD
| season_id | episode_id | title                        |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 1          | "The Work Outing"            |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 2          | "Return of the Golden Child" |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 3          | "Moss and the German"        |
└───────────┴────────────┴──────────────────────────────┘
```

=======
| season_id | episode_id | title |
├───────────┼────────────┼──────────────────────────────┤
| 2 | 1 | "The Work Outing" |
├───────────┼────────────┼──────────────────────────────┤
| 2 | 2 | "Return of the Golden Child" |
├───────────┼────────────┼──────────────────────────────┤
| 2 | 3 | "Moss and the German" |
└───────────┴────────────┴──────────────────────────────┘
```
>>>>>>> 23d71c75863 ([YDBDOCS-2043] Вернуть описание scan_queries (#38583))
