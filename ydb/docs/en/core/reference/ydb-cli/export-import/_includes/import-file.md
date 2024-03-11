# Importing data from a file to an existing table

With the `import file` command, you can import data from [CSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/CSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Comma-separated_values){% endif %} or [TSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/TSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Tab-separated_values){% endif %} files to an existing table.

Data from an imported file is read in batches whose size is set in the `--batch-bytes` option. An independent query is used to write each batch to the database. The queries are executed asynchronously. When the number of executed queries reaches `--max-in-flight`, reading from the file pauses. You can import data from multiple files using a single command. In this case, data from the files will be read asynchronously.

The command implements the `BulkUpsert` method, which ensures high efficiency of multi-row bulk upserts with no atomicity guarantees. The upsert process is split into multiple independent parallel transactions, each covering a single partition. When completed successfully, it guarantees that all data is upserted.

If the table already includes data, it's replaced by imported data on primary key match.

The imported file must be in the [UTF-8]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/UTF-8){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/UTF-8){% endif %} encoding. Line feeds aren't supported in the data field.

General format of the command:

```bash
{{ ydb-cli }} [connection options] import file csv|json|parquet|tsv [options] <input files...>
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

`<input files>`: Paths to local file system files you want to import.

## Subcommand options {#options}

### Required options {#required}

* `-p, --path STRING`: A path to the table in the database.

### Additional options {#optional}

* `--timeout VAL`: Time within which the operation should be completed on the server. Default: `300s`.
* `--skip-rows NUM`: A number of rows from the beginning of the file that will be skipped at import. The default value is `0`.
* `--header`: Use this option if the first row (excluding the rows skipped by `--skip-rows`) includes names of data columns to be mapped to table columns. If the header row is missing, the data is mapped according to the order in the table schema.
* `--delimiter STRING`: The data column delimiter character. You can't use the tabulation character as a delimiter in this option. For tab-delimited import, use the `import file tsv` subcommand. Default value: `,`.
* `--null-value STRING`: The value to be imported as `NULL`. Default value: `""`.
* `--batch-bytes VAL`: Split the imported file into batches of specified sizes. If a row fails to fit into a batch completely, it's discarded and added to the next batch. Whatever the batch size is, the batch must include at least one row. Default value: `1 MiB`.
* `--max-in-flight VAL`: The number of data batches imported in parallel. You can increase this option value to import large files faster. The default value is `100`.
* `--threads VAL`: Maximum number of threads used to import data. Default value: Number of logical processors.
* `--columns`: List of data columns in the file delimited by a `comma` (for `csv` format) or by a tab character (for `tsv` format). If you use the `--header` option, the column names in it will be replaced by column names from the list. If the number of columns in the list mismatches the number of data columns, you will get an error.
* `--newline-delimited`: This flag guarantees that there will be no line breaks in records. If this flag is set, and the data is loaded from a file, then different upload streams will process different parts of the source file. This way you can distribute the workload across all partitions, ensuring the maximum performance when uploading sorted datasets to partitioned tables.

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

Before performing the examples, [create a table]({{ quickstart-path }}) named `series`.

### Import file {#simple}

The file includes data without any additional information. The `,` character is used as a delimiter.

```text
1,IT Crowd,The IT Crowd is a British sitcom.,13182
2,Silicon Valley,Silicon Valley is an American comedy television series.,16166
```

{% note info %}

The `release_date` column in the `series` table has the [Date](../../../../yql/reference/types/primitive.md#datetime) type, so the release date in the imported file has a numeric format. To import values in the [timestamp]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/ISO_8601){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/ISO_8601){% endif %} format, use string-type table columns for them. Alternatively, you can import them to a temporary table and convert them to a relevant type.

{% endnote %}

To import such a file, use the command:

```bash
ydb import file csv -p series series.csv
```

The following data will be imported:

```text
┌──────────────┬───────────┬───────────────────────────────────────────────────────────┬──────────────────┐
| release_date | series_id | series_info                                               | title            |
├──────────────┼───────────┼───────────────────────────────────────────────────────────┼──────────────────┤
| "2006-02-03" | 1         | "The IT Crowd is a British sitcom."                       | "IT Crowd"       |
├──────────────┼───────────┼───────────────────────────────────────────────────────────┼──────────────────┤
| "2014-04-06" | 2         | "Silicon Valley is an American comedy television series." | "Silicon Valley" |
└──────────────┴───────────┴───────────────────────────────────────────────────────────┴──────────────────┘
```

### Importing multiple files {#multiple-files}

The following files include CSV data without additional information:

* series1.csv:

   ```text
   1,IT Crowd,The IT Crowd is a British sitcom.,131822
   ```

* series2.csv:

   ```text
   2,Silicon Valley,Silicon Valley is an American comedy television series., 16166
   ```

To import such files, run the command:

```bash
ydb import file csv -p series series1.csv series2.csv
```

### Import file with the `|` delimiter {#custom-delimiter}

The file includes data without any additional information. The `|` character is used as a delimiter.

```text
1|IT Crowd|The IT Crowd is a British sitcom.|13182
2|Silicon Valley|Silicon Valley is an American comedy television series.|16166
```

To import such a file, use `|` in the `--delimiter` option:

```bash
ydb import file csv -p series --delimiter "|" series.csv
```

### Skip rows and read column headers {#skip}

The file includes additional information in the first and second row, as well as column headers in the third row. The order of data in the file rows mismatches the order of columns in the table:

```text
#The file contains data about the series.
#
series_id,title,release_date,series_info
1,IT Crowd,13182,The IT Crowd is a British sitcom.
2,Silicon Valley,16166,Silicon Valley is an American comedy television series.
```

To skip comments in the first and second rows, use `--skip-rows 2`. To process the third row as headers and map the file data to table columns, use the `--header` option:

```bash
ydb import file csv -p series --skip-rows 2 --header series.csv
```

### Replace values to `NULL` {#replace-with-null}

The file includes the `\N` sequence often used for `NULL`, as well as empty strings.

```text
1,IT Crowd,The IT Crowd is a British sitcom.,13182
2,Silicon Valley,"",\N
3,Lost,,\N
```

Use `--null-value "\N"` so that `\N` is interpreted as `NULL`:

```bash
ydb import file csv -p series --null-value "\N" series.csv
```

The following data will be imported:

```text
┌──────────────┬───────────┬─────────────────────────────────────┬──────────────────┐
| release_date | series_id | series_info                         | title            |
├──────────────┼───────────┼─────────────────────────────────────┼──────────────────┤
| "2006-02-03" | 1         | "The IT Crowd is a British sitcom." | "IT Crowd"       |
├──────────────┼───────────┼─────────────────────────────────────┼──────────────────┤
| null         | 2         | ""                                  | "Silicon Valley" |
├──────────────┼───────────┼─────────────────────────────────────┼──────────────────┤
| null         | 3         | ""                                  | "Lost"           |
└──────────────┴───────────┴─────────────────────────────────────┴──────────────────┘
```
