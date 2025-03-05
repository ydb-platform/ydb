# Query execution

You can use the `{{ ydb-cli }} sql` subcommand to execute any SQL query. The query can be of any type (DDL, DML e.t.c.) and can consist of several sub-queries. The `{{ ydb-cli }} sql` subcommand establishes a streaming connection and retrieves data through it. With the in-stream query execution, no limit is imposed on the amount of data read.

General format of the command:

```bash
{{ ydb-cli }} [global options...] sql [options...]
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).

View the description of this command by calling it with `--help` option:

```bash
{{ ydb-cli }} sql --help
```

## Parameters of the subcommand {#options}

#|
|| Name | Description ||
|| `-h`, `--help` | Print general usage help ||
|| `-hh` | Print complete usage help. Prints some specific options that are not printed with `--help` ||
|| `-s`, `--script` | Script (query) text to execute. ||
|| `-f`, `--file` | Path to a file with query text to execute. Path `-` means reading query text from `stdin`. ||
|| `--stats` | Statistics mode.<br/>Available options:<br/>
<ul>
<li>`none` (default): Do not collect.</li>
<li>`basic`: Collect aggregated statistics for updates and deletes per table.</li>
<li>`full`: Add execution statistics and plan on top of `basic`.</li>
<li>`profile`: Collect detailed execution statistics including statistics for individual tasks and channels.</li>
</ul> ||
|| `--explain` | Execute explain request for the query. Shows query logical plan. The query is not actually executed, thus does not affect database data. ||
|| `--explain-ast` | Same as `--explain`, but in addition to the query logical plan, you an AST (abstract syntax tree) is printed. The AST section contains a representation in the internal miniKQL language. ||
|| `--explain-analyze` | Execute query in `EXPLAIN ANALYZE` mode. Shows query execution plan. Query results are ignored.<br/>**Important note: The query is actually executed, so any changes will be applied to the database**. ||
|| `--format` | Output format.<br/>Available options:

{% include notitle [format](./_includes/result_format_common.md) %}

{% include notitle [format](./_includes/result_format_csv_tsv.md) %}

||
|#

### Working with parameterized queries {#parameterized-query}

A brief help is provided below. For a detailed description with examples, see [{#T}](parameterized-query-execution.md).

| Name | Description |
---|---
| `-p, --param` | The value of a single parameter of the query, in the format: `name=value` or `$name=value`, where `name` is the parameter name and `value` is its value (a valid [JSON value](https://www.json.org/json-ru.html)). |
| `--input-file` | Name of a file in [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} format and in [UTF-8]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/UTF-8){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/UTF-8){% endif %} encoding that specifies values of the parameters matched against the query parameters by key names. Only one input file can be used. |
| `--input-format` | Format of parameter values, applied to all sources of parameters (command line, file, or `stdin`).<br/>Available options:<ul><li>`json` (default): [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} format.</li><li>`csv`:[CSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/CSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Comma-separated_values){% endif %} format.</li><li>`tsv`: [TSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/TSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Tab-separated_values){% endif %} format.</li><li>`raw`: Input is read as parameter value[s] with no transformation or parsing. Parameter name should be set with `--input-param-name` option.</li></ul> |
| `--input-binary-strings` | Input binary strings encoding format. Sets how binary strings in the input should be interterpreted.<br/>Available options:<ul><li>`unicode`: Every byte in binary strings that is not a printable ASCII symbol (codes 32-126) should be encoded as [UTF-8]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/UTF-8){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/UTF-8){% endif %}.</li><li>`base64`: Binary strings should be fully encoded with base64.</li></ul> |

#### More specific options for input parameters {#specific-param-options}

Following options are not described in `--help` output. To see its descriptions use the `-hh` option instead.

| Name | Description |
---|---
| `--input-framing` | Input framing format. Defines how parameter sets are delimited on the input.
<br/>Available options:<br/><br/>
<ul>
<li>`no-framing` (default): Data from the input is taken as a single set of parameters.</li>
<li>`newline-delimited`: Newline character delimits parameter sets on the input and triggers processing in accordance to `--input-batch` option.</li>
</ul> |
| `--input-param-name` | Parameter name on the input stream, required/applicable when input format implies values only (i.e. `--input-format raw`). |
| `--input-columns` | String with column names that replaces CSV/TSV header. Relevant when passing parameters in CSV/TSV format only. It is assumed that there is no header in the file. |
| `--input-skip-rows` | Number of CSV/TSV header rows to skip in the input data (not including the row of column names, if `--header` option is used). Relevant when passing parameters in CSV/TSV format only. |
| `--input-batch` | The batch mode applied to parameter sets on `stdin` or `--input-file`.<br/>Available options:<br/>
<ul>
<li>`iterative` (default): Executes the query for each parameter set (exactly one execution when `no-framing` is specified for `--input-framing`)</li>
<li>`full`: Executes the query with all parameter sets wrapped in json list every time EOF is reached on stdin</li>
<li>adaptive`: Executes the query with a json list of parameter sets every time when its number reaches `--input-batch-max-rows`, or the waiting time reaches `--input-batch-max-delay`</li>
</ul> |
| `--input-batch-max-rows` | Maximum size of list for input adaptive batching mode (default: 1000) |
| `--input-batch-max-delay` | Maximum delay to process first item in the list for `adaptive` batching mode (default: 1s) |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

A script to create a table, populate it with data, and select data from the table:

```bash
{{ ydb-cli }} -p quickstart sql -s '
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
{{ ydb-cli }} -p quickstart sql -f script1.yql --format json
```

Command output:

```text
{"release_date":"2023-04-20","series_id":1,"series_info":"Info1","title":"Title1"}
```

You can find examples of passing parameters to queries in the [article on how to pass parameters to `{{ ydb-cli }} sql`](parameterized-queries-cli.md).
