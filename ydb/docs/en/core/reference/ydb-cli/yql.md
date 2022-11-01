# Running a script (with streaming support)

You can use the `yql` subcommand to run a YQL script. The script can include queries of different types. Unlike `scripting yql`, the `yql` subcommand establishes a streaming connection and retrieves data through it. With the in-stream query execution, no limit is imposed on the amount of data read.

General format of the command:

```bash
{{ ydb-cli }} [global options...] yql [options...]
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).

View the description of the YQL script command:

```bash
{{ ydb-cli }} yql --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--timeout` | The time within which the operation should be completed on the server. |
| `--stats` | Statistics mode.<br>Acceptable values:<ul><li>`none`: Do not collect statistics.</li><li>`basic`: Collect statistics for basic events.</li><li>`full`: Collect statistics for all events.</li></ul>Defaults to `none`. |
| `-s`, `--script` | Text of the YQL query to be executed. |
| `-f`, `--file` | Path to the text of the YQL query to be executed. |
| `-p`, `--param` | [Query parameters](../../getting_started/yql.md#param) (for data queries and scan queries).<br>You can specify multiple parameters. To change the input format, use the `--input-format` parameter. |
| `--input-format` | Input format.<br>Acceptable values:<ul><li>`json-unicode`: [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} input with binary strings [Unicode]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %}-encoded.</li><li>`json-base64`: JSON input with binary strings [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}-encoded.</li></ul> |
| `--format` | Input format.<br>Default value: `pretty`.<br>Acceptable values:<ul><li>`pretty`: A human-readable format.</li><li>`json-unicode`: [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} output with binary strings [Unicode]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %}-encoded and each JSON string in a separate line.</li><li>`json-unicode-array`: JSON output with binary strings Unicode-encoded and the result output as an array of JSON strings with each JSON string in a separate line.</li><li>`json-base64`: JSON output with binary strings [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}-encoded and each JSON string in a separate line.</li><li>`json-base64-array`: JSON output with binary strings Base64-encoded and the result output as an array of JSON strings with each JSON string in a separate line.</li></ul> |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Running queries with a timeout {#example-timeout}

Run a query with a timeout of 500 ms:

```bash
ydb yql \
  --script \
  "CREATE TABLE series ( \
  series_id Uint64, \
  title Utf8, \
  series_info Utf8, \
  release_date Uint64, \
  PRIMARY KEY (series_id) \
  );" \
  --timeout 500
```

If the server couldn't complete the query in 500 ms, it returns an error. If the client couldn't get an error message from the server, the query is interrupted on the client side in 500+200 ms.

### Running a parameterized query {#example-param}

Run the parameterized query with statistics enabled:

```bash
ydb yql \
  --stats full \
  --script \
  "DECLARE \$myparam AS Uint64; \
  SELECT * FROM series WHERE series_id=\$myparam;" \
  --param '$myparam=1'
```

Result:

```text
┌──────────────┬───────────┬──────────────────────────────────────────────────────────────────────────────┬────────────┐
| release_date | series_id | series_info                                                                  | title      |
├──────────────┼───────────┼──────────────────────────────────────────────────────────────────────────────┼────────────┤
| 13182        | 1         | "The IT Crowd is a British sitcom produced by Channel 4, written by Graham L | "IT Crowd" |
|              |           | inehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Ka |            |
|              |           | therine Parkinson, and Matt Berry."                                          |            |
└──────────────┴───────────┴──────────────────────────────────────────────────────────────────────────────┴────────────┘

Statistics:
query_phases {
  duration_us: 14294
  table_access {
    name: "/my-db/series"
    reads {
      rows: 1
      bytes: 209
    }
    partitions_count: 1
  }
  cpu_time_us: 783
  affected_shards: 1
}
process_cpu_time_us: 5083
total_duration_us: 81373
total_cpu_time_us: 5866
```
