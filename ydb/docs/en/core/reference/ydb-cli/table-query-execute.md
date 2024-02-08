# Running a query

The `table query execute` subcommand is designed for reliable execution of YQL queries. With this sub-command, you can successfully execute your query when certain table partitions are unavailable for a short time (for example, due to being [split or merged](../../concepts/datamodel/table.md#partitioning)) by using built-in retry policies.

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

#|
|| **Name** | **Description** ||
|| `--timeout` | The time within which the operation should be completed on the server. ||
|| `-t`, `--type` | Query type.
Acceptable values:
* `data`: A YQL query that includes [DML]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Data_Manipulation_Language){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Data_Manipulation_Language){% endif %} operations; it can be used both to update data in the database and fetch several selections limited to 1,000 rows per selection.
* `scan`: A YQL query of the [scan](../../concepts/scan_query.md) type. It can only be used to read data from the database. It returns a single selection, but without a limit on the number of records in it. The algorithm of executing a `scan` query on the server is more sophisticated compared to a `data` query. Hence, if you don't need to return more than 1,000 rows, `data` queries are more effective.
* `scheme`: A YQL query that includes [DDL]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Data_Definition_Language){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Data_Definition_Language){% endif %} operations.
    The default value is `data`. ||
|| `--stats` | Statistics mode.
Acceptable values:
* `none`: Do not collect statistics.
* `basic`: Collect statistics for basic events.
* `full`: Collect statistics for all events.
    Defaults to `none`. ||
|| `-s` | Enable statistics collection in the `basic` mode. ||
|| `--tx-mode` | [Transaction mode](../../concepts/transactions.md#modes) (for `data` queries).
Acceptable values:<li>`serializable-rw`: The result of parallel transactions is equivalent to their serial execution.<li>`online-ro`: Each of the reads in the transaction reads data that is most recent at the time of its execution.<li>`stale-ro`: Data reads in a transaction return results with a possible delay (fractions of a second).Default value: `serializable-rw`. ||
|| `-q`, `--query` | Text of the YQL query to be executed. ||
|| `-f,` `--file` | Path to the text of the YQL query to be executed. ||
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

### Creating tables {#examples-create-tables}

```bash
{{ ydb-cli }} -p quickstart table query execute \
  --type scheme \
  -q '
  CREATE TABLE series (series_id Uint64 NOT NULL, title Utf8, series_info Utf8, release_date Date, PRIMARY KEY (series_id));
  CREATE TABLE seasons (series_id Uint64, season_id Uint64, title Utf8, first_aired Date, last_aired Date, PRIMARY KEY (series_id, season_id));
  CREATE TABLE episodes (series_id Uint64, season_id Uint64, episode_id Uint64, title Utf8, air_date Date, PRIMARY KEY (series_id, season_id, episode_id));
  '
```

### Populating the table with data {#examples-upsert}
```bash
{{ ydb-cli }} -p quickstart table query execute \
  -q '
UPSERT INTO series (series_id, title, release_date, series_info) VALUES
  (1, "IT Crowd", Date("2006-02-03"), "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'"'"'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
  (2, "Silicon Valley", Date("2014-04-06"), "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.");

UPSERT INTO seasons (series_id, season_id, title, first_aired, last_aired) VALUES
    (1, 1, "Season 1", Date("2006-02-03"), Date("2006-03-03")),
    (1, 2, "Season 2", Date("2007-08-24"), Date("2007-09-28")),
    (2, 1, "Season 1", Date("2014-04-06"), Date("2014-06-01")),
    (2, 2, "Season 2", Date("2015-04-12"), Date("2015-06-14"));

UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date) VALUES
    (1, 1, 1, "Yesterday'"'"'s Jam", Date("2006-02-03")),
    (1, 1, 2, "Calamity Jen", Date("2006-02-03")),
    (2, 1, 1, "Minimum Viable Product", Date("2014-04-06")),
    (2, 1, 2, "The Cap Table", Date("2014-04-13"));
'
```

### Simple data selection {#examples-simple-query}

```bash
{{ ydb-cli }} -p quickstart table query execute -q '
    SELECT season_id, episode_id, title
    FROM episodes
    WHERE series_id = 1
  '
```

Result:

```text
┌───────────┬────────────┬───────────────────┐
| season_id | episode_id | title             |
├───────────┼────────────┼───────────────────┤
| 1         | 1          | "Yesterday's Jam" |
├───────────┼────────────┼───────────────────┤
| 1         | 2          | "Calamity Jen"    |
└───────────┴────────────┴───────────────────┘
```

### Unlimited selection for automated processing {#examples-query-stream}

Selecting data by a query whose text is saved to a file, without a limit on the number of rows in the selection and data output in the format: [Newline-delimited JSON stream](https://en.wikipedia.org/wiki/JSON_streaming).

Let's write the query text to the `request1.yql` file.

```bash
echo 'SELECT season_id, episode_id, title FROM episodes' > request1.yql
```

Now, run the query:

```bash
{{ ydb-cli }} -p quickstart table query execute -f request1.yql --type scan --format json-unicode
```

Result:

```text
{"season_id":1,"episode_id":1,"title":"Yesterday's Jam"}
{"season_id":1,"episode_id":2,"title":"Calamity Jen"}
{"season_id":1,"episode_id":1,"title":"Minimum Viable Product"}
{"season_id":1,"episode_id":2,"title":"The Cap Table"}
```

### Passing parameters {#examples-params}

You can find examples of executing parameterized queries, including streamed processing, in the [Passing parameters to YQL execution commands](parameterized-queries-cli.md) article.

