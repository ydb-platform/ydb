# YQL - Getting started

## Introduction {#intro}

YQL is a {{ ydb-short-name }} query language, a dialect of SQL. Specifics of its syntax let you use it when executing queries on clusters.

For more information about the YQL syntax, see the [YQL reference](../../yql/reference/index.md).

The examples below show how to get started with YQL and assume sequential execution of the steps described: queries in the ["Working with data"](#dml) section access data in tables created in the ["Working with a data schema"](#ddl) section. Follow the steps one by one so that the examples copied through the clipboard are executed successfully.

The {{ ydb-short-name }} YQL basic interface accepts a script that may consist of multiple commands and not a single command as input.

## YQL query execution tools {#tools}

In {{ ydb-short-name }}, you can make YQL queries to a database using:

{% include [yql/ui_prompt.md](yql/ui_prompt.md) %}

- [{{ ydb-short-name }} CLI](#cli)

- [{{ ydb-short-name }} SDK](../sdk.md)

{% include [yql/ui_execute.md](yql/ui_execute.md) %}

### {{ ydb-short-name }} CLI {#cli}

To execute scripts using the {{ ydb-short-name }} CLI, first do the following:

- [Install the CLI](../cli.md#install).
- Define and check [DB connection parameters](../cli#scheme-ls).
- [Create a `db1` profile](../cli.md#profile) configured to connect to your database.

Save the text of the scripts below to a file. Name it `script.yql` to be able to run the statements given in the examples by simply copying them through the clipboard. Next, run `{{ ydb-cli }} yql` indicating the use of the `db1` profile and reading the script from the `script.yql` file:

```bash
{{ ydb-cli }} --profile db1 yql -f script.yql
```

## Working with a data schema {#ddl}

### Creating a table {#create-table}

A table with the specified columns is created [using the YQL `CREATE TABLE`](../../yql/reference/syntax/create_table.md) statement. Make sure the primary key is defined in the table. Column data types are described in [YQL data types](../../yql/reference/types/index.md).

Currently, {{ ydb-short-name }} doesn't support the `NOT NULL` constraint, all columns allow null values, including the primary key columns. In addition, {{ ydb-short-name }} doesn't support the `FOREIGN KEY` constraint.

Create series directory tables named `series`, `seasons`, and `episodes` by running the following script:

```sql
CREATE TABLE series (
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Uint64,
    PRIMARY KEY (series_id)
);

CREATE TABLE seasons (
    series_id Uint64,
    season_id Uint64,
    title Utf8,
    first_aired Uint64,
    last_aired Uint64,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE episodes (
    series_id Uint64,
    season_id Uint64,
    episode_id Uint64,
    title Utf8,
    air_date Uint64,
    PRIMARY KEY (series_id, season_id, episode_id)
);
```

For a description of all features of working with tables, see the following sections of the YQL documentation:

- [`CREATE TABLE`](../../yql/reference/syntax/create_table.md): Creating a table and defining initial parameters.
- [`ALTER TABLE`](../../yql/reference/syntax/alter_table.md): Changing the composition of table columns and parameters.
- [`DROP TABLE`](../../yql/reference/syntax/drop_table.md): Deleting a table.

To execute the script via the {{ ydb-short-name }} CLI, follow the instructions given in the ["Executing YQL scripts in the {{ ydb-short-name }} CLI"](#cli) section in this article.

### Getting a list of existing DB tables {#scheme-ls}

Check that the tables are actually created in the database.

{% include [yql/ui_scheme_ls.md](yql/ui_scheme_ls.md) %}

To get a list of existing DB tables via the {{ ydb-short-name }} CLI, be sure to meet the prerequisites of the ["Executing YQL scripts in the {{ ydb-short-name }} CLI"](#cli) section of this article and run [the `scheme ls` statement](../cli.md#ping):

```bash
{{ ydb-cli }} --profile db1 scheme ls
```

## Operations with data {#dml}

Commands for executing YQL queries and scripts in the YDB CLI and web interface are run in **Autocommit** mode, meaning that a transaction is committed automatically  after a transaction is completed.

### UPSERT : Adding data {#upsert}

The most efficient way to add data to {{ ydb-short-name }} is through the [`UPSERT`](../../yql/reference/syntax/upsert_into.md) statement. It inserts new data by primary keys regardless of whether data by these keys previously existed in the table. As a result, unlike regular `INSERT` and `UPDATE` statements, its execution does not require a pre-fetch of data on the server to verify if a key is unique. When working with {{ ydb-short-name }}, always consider `UPSERT` as the main way to add data and only use other statements when absolutely necessary.

All statements that write data to {{ ydb-short-name }} support working with both samples and multiple entries passed directly in a query.

Let's add data to the previously created tables:

```yql
UPSERT INTO series (series_id, title, release_date, series_info)
VALUES
    (
        1,
        "IT Crowd",
        CAST(Date("2006-02-03") AS Uint64),
        "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
    (
        2,
        "Silicon Valley",
        CAST(Date("2014-04-06") AS Uint64),
        "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."
    )
    ;

UPSERT INTO seasons (series_id, season_id, title, first_aired, last_aired)
VALUES
    (1, 1, "Season 1", CAST(Date("2006-02-03") AS Uint64), CAST(Date("2006-03-03") AS Uint64)),
    (1, 2, "Season 2", CAST(Date("2007-08-24") AS Uint64), CAST(Date("2007-09-28") AS Uint64)),
    (2, 1, "Season 1", CAST(Date("2014-04-06") AS Uint64), CAST(Date("2014-06-01") AS Uint64)),
    (2, 2, "Season 2", CAST(Date("2015-04-12") AS Uint64), CAST(Date("2015-06-14") AS Uint64))
;

UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
VALUES
    (1, 1, 1, "Yesterday's Jam", CAST(Date("2006-02-03") AS Uint64)),
    (1, 1, 2, "Calamity Jen", CAST(Date("2006-02-03") AS Uint64)),
    (2, 1, 1, "Minimum Viable Product", CAST(Date("2014-04-06") AS Uint64)),
    (2, 1, 2, "The Cap Table", CAST(Date("2014-04-13") AS Uint64))
;
```

To execute the script via the {{ ydb-short-name }} CLI, follow the instructions given in the ["Executing YQL scripts in the {{ ydb-short-name }} CLI"](#cli) section in this article.

To learn more about the commands for writing data, see the YQL reference:

- [`INSERT`](../../yql/reference/syntax/insert_into.md): Adds entries.
- [`REPLACE`](../../yql/reference/syntax/replace_into.md): Adds/updates entries.
- [`UPDATE`](../../yql/reference/syntax/update.md): Updates the specified fields.
- [`UPSERT`](../../yql/reference/syntax/upsert_into.md): Adds entries/updates the specified fields.

### SELECT : Data selection {#select}

Make a select of the data added in the previous step:

```sql
SELECT 
    series_id,
    title AS series_title,
    DateTime::ToDate(DateTime::FromDays(release_date)) AS release_date
FROM series;
```

or

```sql
SELECT * FROM episodes;
```

If there are several `SELECT` statements in the YQL script, its execution will return several samples, each of which can be accessed separately. Run the above `SELECT` statements as a single script.

To execute the script via the {{ ydb-short-name }} CLI, follow the instructions given in the ["Executing YQL scripts in the {{ ydb-short-name }} CLI"](#cli) section in this article.

To learn more about the commands for selecting data, see the YQL reference:

- [`SELECT`](../../yql/reference/syntax/select.md): Selects data.
- [`SELECT ... JOIN`](../../yql/reference/syntax/join.md): Joins tables when making a select.
- [`SELECT ... GROUP BY`](../../yql/reference/syntax/group_by.md): Groups data when making a select.

### Parameterized queries {#param}

Transactional applications working with a database are characterized by the execution of multiple similar queries that only differ in parameters. Like most databases, {{ ydb-short-name }} will work more efficiently if you define updateable parameters and their types and then initiate the execution of a query by passing the parameter values separately from its text.

To define parameters in the text of a YQL query, use the [DECLARE](../../yql/reference/syntax/declare.md) statement.

Methods for executing parameterized queries in the {{ ydb-short-name }} SDK are described in the [Test case](../../reference/ydb-sdk/example/index.md) section under "Parameterized queries" for the appropriate programming language.

When debugging a parameterized query in the {{ ydb-short-name }} SDK, you can test it by calling the {{ ydb-short-name }} CLI, copying the full text of the query without any edits, and setting parameter values.

Save the script for executing the parameterized query in a text file named `script.yql`:

```sql
DECLARE $seriesId AS Uint64;
DECLARE $seasonId AS Uint64;

SELECT sa.title AS season_title, sr.title AS series_title
FROM   seasons AS sa
INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;
```

To make a parameterized select query, be sure to meet the prerequisites of the ["Executing YQL scripts in the {{ ydb-short-name }} CLI"](#cli) section of this article and run:

```bash
{{ ydb-cli }} --profile db1 yql -f script.yql -p '$seriesId=1' -p '$seasonId=1'
```

For a full description of the ways to pass parameters, see [the {{ ydb-short-name }} CLI reference](../../reference/ydb-cli/index.md).

## YQL tutorial {#tutorial}

You can learn more about YQL use cases by completing tasks from the [YQL tutorial](../../yql/tutorial/index.md).

## Learn more about YDB {#next}

Proceed to the [YDB SDK - Getting started](../sdk.md) article to learn more about YDB.

