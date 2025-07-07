# Overview

These pages describe popular datasets that you can load into {{ ydb-short-name }} to familiarize yourself with the database's functionality and test various use cases.

## Prerequisites

To load the datasets, you will need:

1. Installed [{{ ydb-short-name }} CLI](../../reference/ydb-cli/)
2. [Optional] Configured [connection profile](../../reference/ydb-cli/profile/create.md) to {{ ydb-short-name }} to avoid specifying connection parameters with every command

## General Information on Data Loading {#general-info}

{{ ydb-short-name }} supports importing data from CSV files using the [command](../../reference/ydb-cli/export-import/import-file.md) `ydb import file csv`. Example command:

```bash
ydb import file csv --header --null-value "" --path <table_path> <file>.csv
```

Where:

* `--header` indicates that the first row of the file contains the column names, and the actual data starts from the second row;
* `--null-value ""` specifies that an empty string in the CSV will be interpreted as a null value during data import into the table.

A table must already exist in {{ ydb-short-name }} for data import. The primary way to create a table is by executing the [`CREATE TABLE` YQL query](../../yql/reference/syntax/create_table/index.md). Instead of writing the query manually, you can try running the import command from a file, as shown in any example in this section, without creating the table first. In this case, the CLI will suggest a `CREATE TABLE` query, which you can use as a base, edit if necessary, and execute.

To import data into {{ ydb-short-name }}, a table must be pre-created. Typically, a table is created using the [YQL `CREATE TABLE` query](../../yql/reference/syntax/create_table/index.md). However, instead of crafting such a query manually, you can initiate the import command `ydb import file csv` as shown in the import examples in this section. If the table doesn't exist, the CLI will automatically suggest a `CREATE TABLE` query that you can use to create the table.

{% note info "Selecting a Primary Key" %}

{{ ydb-short-name }} requires a primary key for the table. It significantly speeds up data loading and processing, and it also allows for deduplication: rows with identical values in the primary key columns replace each other.

If the imported dataset doesn't have suitable columns for a primary key, we add a new column with row numbers and use it as the primary key, as each row number is unique within the file.

{% endnote %}

## Features and Limitations

When woimporting data to {{ ydb-short-name }}, consider the following points:

1. **Column Names**: Column names should not contain spaces or special characters.

2. **Data Types**:
   - Date/time strings with timezone (e.g., "2019-11-01 00:00:00 UTC") will be imported as Text type.
   - The Bool type is not supported as a column type; use Text or Int64 instead.

## Available Datasets

* [Chess Position Evaluations](chess.md) - Stockfish engine chess position evaluations
* [Video Game Sales](video-games.md) - video game sales data
* [E-Commerce Behavior Data](ecommerce.md) - user behavior data from an online store
* [COVID-19 Open Research Dataset](covid.md) - open research dataset on COVID-19
* [Netflix Movies and TV Shows](netflix.md) - data on Netflix movies and shows
* [Animal Crossing New Horizons Catalog](animal-crossing.md) - item catalog from the game