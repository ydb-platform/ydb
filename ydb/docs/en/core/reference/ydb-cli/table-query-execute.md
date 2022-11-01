# Running a query

You can use the `table query execute` subcommand to run single ad-hoc queries of certain types, for the purposes of testing and troubleshooting.

General format of the command:

```bash
{{ ydb-cli }} [global options...] table query execute [options...]
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).

View the description of the YQL query command:

```bash
{{ ydb-cli }} table query execute --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--timeout` | The time within which the operation should be completed on the server. |
| `-t`, `--type` | Query type.<br>Acceptable values:<ul><li>`data`: A data query.</li><li>`scheme`: A scheme query.</li><li>`scan`: A [scan query](../../concepts/scan_query.md).</li></ul>The default value is `data`. |
| `--stats` | Statistics mode.<br>Acceptable values:<ul><li>`none`: Do not collect statistics.</li><li>`basic`: Collect statistics for basic events.</li><li>`full`: Collect statistics for all events.</li></ul>Defaults to `none`. |
| `-s` | Enable statistics collection in the `basic` mode. |
| `--tx-mode` | Specify the [transaction mode](../../concepts/transactions.md#modes) (for `data` queries).<br>Acceptable values:<ul><li>`serializable-rw`: The result of parallel transactions is equivalent to their serial execution.</li><li>`online-ro`: Each of the reads in the transaction reads data that is most recent at the time of its execution.</li><li>`stale-ro`: Data reads in a transaction return results with a possible delay (fractions of a second).</li></ul>Default value: `serializable-rw`. |
| `-q`, `--query` | Text of the YQL query to be executed. |
| `-f,` `--file` | Path to the text of the YQL query to be executed. |
| `-p`, `--param` | [Query parameters](../../getting_started/yql.md#param) (for data queries and scan queries).<br>You can specify multiple parameters. To change the input format, use the `--input-format` subcommand parameter. |
| `--input-format` | Input format.<br>Acceptable values:<ul><li>`json-unicode`: [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} input with binary strings [Unicode]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %}-encoded.</li><li>`json-base64`: JSON input with binary strings [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}-encoded.</li></ul> |
| `--format` | Input format.<br>Default value: `pretty`.<br>Acceptable values:<ul><li>`pretty`: A human-readable format.</li><li>`json-unicode`: [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} output with binary strings [Unicode]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %}-encoded and each JSON string in a separate line.</li><li>`json-unicode-array`: JSON output with binary strings Unicode-encoded and the result output as an array of JSON strings with each JSON string in a separate line.</li><li>`json-base64`: JSON output with binary strings [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}-encoded and each JSON string in a separate line.</li><li>`json-base64-array`: JSON output with binary strings Base64-encoded and the result output as an array of JSON strings with each JSON string in a separate line.</li></ul> |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Run the data query:

```bash
{{ ydb-cli }} table query execute \
  --query "SELECT season_id, episode_id, title \
  FROM episodes \
  WHERE series_id = 1 AND season_id > 1 \
  ORDER BY season_id, episode_id \
  LIMIT 3"
```

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
