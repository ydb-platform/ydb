# Streaming table reads

To read an entire table snapshot, use the `read` subcommand. Data is transferred as a stream, which enables you to read any size table.

Read data:

```bash
{{ ydb-cli }} table read episodes \
  --ordered \
  --limit 5 \
  --columns series_id,season_id,episode_id,title
```

Where:

* `--ordered`: Order read entries by key.
* `--limit`: Limit the number of entries to read.
* `--columns`: Columns whose values should be read (all by default) in CSV format.

Result:

```text
┌───────────┬───────────┬────────────┬───────────────────────────────┐
| series_id | season_id | episode_id | title                         |
├───────────┼───────────┼────────────┼───────────────────────────────┤
| 1         | 1         | 1          | "Yesterday's Jam"             |
├───────────┼───────────┼────────────┼───────────────────────────────┤
| 1         | 1         | 2          | "Calamity Jen"                |
├───────────┼───────────┼────────────┼───────────────────────────────┤
| 1         | 1         | 3          | "Fifty-Fifty"                 |
├───────────┼───────────┼────────────┼───────────────────────────────┤
| 1         | 1         | 4          | "The Red Door"                |
├───────────┼───────────┼────────────┼───────────────────────────────┤
| 1         | 1         | 5          | "The Haunting of Bill Crouse" |
└───────────┴───────────┴────────────┴───────────────────────────────┘
```

To only get the number of read entries, use the `--count-only` parameter:

```bash
{{ ydb-cli }} table read episodes \
  --columns series_id \
  --count-only
```

Result:

```text
70
```

