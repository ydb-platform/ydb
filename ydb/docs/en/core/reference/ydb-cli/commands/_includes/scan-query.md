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
```

Where:

* `--query` — query text.

Result:

```text
┌───────────┬────────────┬──────────────────────────────┐
| season_id | episode_id | title |
├───────────┼────────────┼──────────────────────────────┤
| 2 | 1 | "The Work Outing" |
├───────────┼────────────┼──────────────────────────────┤
| 2 | 2 | "Return of the Golden Child" |
├───────────┼────────────┼──────────────────────────────┤
| 2 | 3 | "Moss and the German" |
└───────────┴────────────┴──────────────────────────────┘
```
