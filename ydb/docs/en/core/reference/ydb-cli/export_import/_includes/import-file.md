# Importing data from a file to an existing table

Using the `import file` subcommand, you can import data from [CSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/CSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Comma-separated_values){% endif %} or [TSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/TSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Tab-separated_values){% endif %} files to an existing table.

Data is imported using the `BulkUpsert` method that provides high-efficient batch data inserts without atomicity guarantees. Data writes are split in multiple independent single-shard transactions executed in parallel. When succesfully completed, all data is guaranteed to be inserted.

In case the table already contains data, it will be replaced with imported data if the primary key matches.

The imported file must be encoded in [UTF-8]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/UTF-8){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/UTF-8){% endif %}. Processing a line break inside a data field is not supported.

General command format:

```bash
{{ ydb-cli }} [connection options] import file csv|tsv [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

## Subcommand parameters {#options}

### Required parameters {#required}

* `-p, --path STRING`: Path to the DB table.
* `--input-file STRING`: Path to the file in the local file system, whose data needs to be imported.

### Additional parameters {#optional}

* `--skip-rows NUM`: Number of rows from the beginning of the file to be skipped during import. Default value: `0`.
* `--header`: Specify this parameter if the first row (excluding the rows skipped when using the `--skip-rows` parameter) contains the names of data columns to be mapped to the corresponding table columns. If no header row is specified, the data will be mapped in the order of columns in the table schema.
* `--delimiter STRING`: Data column delimiter character. You can't specify a tab character as a delimiter in this parameter. To import a file with this delimiter, use the `import file tsv` subcommand. Default value: `,`.
* `--null-value STRING`: Value to be imported as `NULL`. Default value: `""`.
* `--batch-bytes VAL`: Split the uploaded file into batches of the specified size. If a row does not fit into the whole batch, it will be discarded and passed in the next batch. For any size value, a batch consists of at least one row. Default value: `1 Mib`.
* `--max-in-flight VAL`: Number of simultaneously uploaded data batches. To speed up the import of large files, you can increase the value of this parameter. Default value: `100`.

## Examples {#examples}

{% include [example_db1.md](../../_includes/example_db1.md) %}

Before running the examples, [create a table](../../../../getting_started/yql.md#create-table) `series`.

### Importing a file {#simple}

The file contains data without additional information. The `,` is used as a delimiter:

```text
1,The IT Crowd,The IT Crowd is a British sitcom.,13182
2,Silicon Valley,Silicon Valley is an American comedy television series.,16166
```

{% note info %}

The `release_date` column of the `series` table is of the [Date](../../../../yql/reference/types/primitive.md#datetime) type, so the release date in the imported file is represented as a number. To import values in [timestamp]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/ISO_8601){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/ISO_8601){% endif %} format, use string type table columns or import them to a temporary table and then convert them to the desired type.

{% endnote %}

To import this file, run the command:

```bash
ydb import file csv -p series --input-file series.csv
```

As a result, the following data is imported:

```text
┌──────────────┬───────────┬───────────────────────────────────────────────────────────┬──────────────────┐
| release_date | series_id | series_info                                               | title            |
├──────────────┼───────────┼───────────────────────────────────────────────────────────┼──────────────────┤
| "2006-02-03" | 1         | "The IT Crowd is a British sitcom."                       | "The IT Crowd"   |
├──────────────┼───────────┼───────────────────────────────────────────────────────────┼──────────────────┤
| "2014-04-06" | 2         | "Silicon Valley is an American comedy television series." | "Silicon Valley" |
└──────────────┴───────────┴───────────────────────────────────────────────────────────┴──────────────────┘
```

### Importing a file with the `|` delimiter {#custom-delimiter}

The file contains data without additional information. The `|` character is used as a delimiter:

```text
1|IT Crowd|The IT Crowd is a British sitcom.|13182
2|Silicon Valley|Silicon Valley is an American comedy television series.|16166
```

To import this file, specify the `--delimiter` parameter set to `|`:

```bash
ydb import file csv -p series --input-file series.csv --delimiter "|"
```

### Skipping rows and counting column headers {#skip}

The file contains additional information in the first and second rows and column headers in the third row. The order of data in the file rows does not correspond to the order of table columns:

```text
#The file contains data about the series.
#
series_id,title,release_date,series_info
1,The IT Crowd,13182,The IT Crowd is a British sitcom.
2,Silicon Valley,16166,Silicon Valley is an American comedy television series.
```

To skip comments in the first and second rows, specify the `--skip-rows 2` parameter. To process the third row as a header and map the file data to the appropriate table columns, specify the `--header` parameter:

```bash
ydb import file csv -p series --input-file series.csv --skip-rows 2 --header
```

### Replacing values with `NULL` {#replace-with-null}

The file contains `\N`, a designation that is frequently used for `NULL`, and an empty string.

```text
1,The IT Crowd,The IT Crowd is a British sitcom.,13182
2,Silicon Valley,"",\N
3,Lost,,\N
```

Specify the `--null-value "\N"` parameter to make sure `\N` is interpreted as `NULL`:

```bash
ydb import file csv -p series --input-file series.csv --null-value "\N"
```

As a result, the following data is imported:

```text
┌──────────────┬───────────┬─────────────────────────────────────┬──────────────────┐
| release_date | series_id | series_info                         | title            |
├──────────────┼───────────┼─────────────────────────────────────┼──────────────────┤
| "2006-02-03" | 1         | "The IT Crowd is a British sitcom." | "The IT Crowd"   |
├──────────────┼───────────┼─────────────────────────────────────┼──────────────────┤
| null         | 2         | ""                                  | "Silicon Valley" |
├──────────────┼───────────┼─────────────────────────────────────┼──────────────────┤
| null         | 3         | ""                                  | "Lost"           |
└──────────────┴───────────┴─────────────────────────────────────┴──────────────────┘
```

