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

#|
|| **Name** | **Description** ||
|| `--timeout` | The time within which the operation should be completed on the server. ||
|| `--stats` | Statistics mode.
Acceptable values:
* `none` (default): Do not collect.
* `basic`: Collect statistics for basic events.
* `full`: Collect statistics for all events.
     ||
|| `-s`, `--script` | Text of the YQL query to be executed. ||
|| `-f`, `--file` | Path to the text of the YQL query to be executed. ||
|| `--format` | Result format.
Possible values:

{% include notitle [format](./_includes/result_format_common.md) %}

{% include notitle [format](./_includes/result_format_csv_tsv.md) %}

||
|#
### Working with parameterized queries {#parameterized-query}

{% include [parameterized-query](../../_includes/parameterized-query.md) %}

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

A script to create a table, populate it with data, and select data from the table:

```bash
{{ ydb-cli }} -p quickstart yql -s '
    CREATE TABLE series (series_id Uint64, title Utf8, series_info Utf8, release_date Date, PRIMARY KEY (series_id));
    COMMIT;
    UPSERT INTO series (series_id, title, series_info, release_date) values (1, "Title1", "Info1", Cast("2023-04-20" as Date));
    COMMIT;
    SELECT * from series;
  '
```

Command output:

```text
┌──────────────┬───────────┬─────────────┬──────────┐
| release_date | series_id | series_info | title    |
├──────────────┼───────────┼─────────────┼──────────┤
| "2023-04-20" | 1         | "Info1"     | "Title1" |
└──────────────┴───────────┴─────────────┴──────────┘
```

Running a script from the example above saved as the `script1.yql` file, with results output in `JSON` format:

```bash
{{ ydb-cli }} -p quickstart yql -f script1.yql --format json-unicode
```

Command output:

```text
{"release_date":"2023-04-20","series_id":1,"series_info":"Info1","title":"Title1"}
```


You can find examples of passing parameters to scripts in the [article on how to pass parameters to YQL execution commands](parameterized-queries-cli.md).

