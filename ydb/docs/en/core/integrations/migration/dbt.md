# {{ dbt }} Integration With YDB

## Introduction

[{{ dbt }}](https://www.getdbt.com) is a popular tool for data transformation and management that enables you to:

- Configure reliable and consistent transformation pipelines using `SELECT` statements that reflect business logic
- Implement version control and CI/CD practices for transformation code
- Test data and detect anomalies to ensure data quality
- Generate documentation and visualize dependencies between data

The [{{ dbt-ydb }}](https://github.com/ydb-platform/dbt-ydb) connector provides seamless integration between {{ dbt }} and [{{ ydb-short-name }}](../../concepts/glossary.md), enabling data transformation, modeling, and pipeline management directly within {{ ydb-short-name }}.

{% note info %}

The {{ dbt-ydb }} connector is currently in development and has several significant limitations. These limitations will be removed in future versions.

{% endnote %}

## Features

### Models And Their Materialization

A core concept in {{ dbt }} is a [data model](https://docs.getdbt.com/docs/build/sql-models). By its nature, it is a SQL expression that can use any data sources inside your data warehouse, including other models. There are different approaches to physically creating a model (its materialization). Each approach defines how exactly the query will be executed: by creating a new table, updating an existing one, or simply forming a view.

The {{ dbt-ydb }} connector supports the following materialization strategies:

- View — materialized as a database view in YDB
- Table — persisted as a table in YDB and recreated by {{ dbt }} on every run
- [Incremental model](https://docs.getdbt.com/docs/build/incremental-models-overview) — created as a table in {{ ydb-short-name }}, but during updates, it is not recreated; instead, it is updated with changed and new rows. The connector currently supports the [`MERGE` strategy](https://docs.getdbt.com/docs/build/incremental-strategy#merge).

Another materialization type, [ephemeral model](https://docs.getdbt.com/docs/build/materializations#ephemeral), is currently not supported by the connector.

### Snapshots

The [snapshot mechanism](https://docs.getdbt.com/docs/build/snapshots) is currently not supported by {{ dbt‑ydb }}.

### Seeds

The {{ dbt‑ydb }} connector supports {{ dbt }}’s ability to define [seeds](https://docs.getdbt.com/docs/build/seeds) for loading reference and test data from CSV files into your project and using them in other models.

### Data Testing

The {{ dbt‑ydb }} connector supports standard [{{ dbt }} data tests](https://docs.getdbt.com/docs/build/data-tests#generic-data-tests), as well as [specific tests](https://docs.getdbt.com/docs/build/data-tests#singular-data-tests) within the capabilities of [YQL](../../yql/reference/index.md).

### Documentation Generation

{{ dbt‑ydb }} supports generating [documentation](https://docs.getdbt.com/docs/build/documentation) from {{ dbt }} projects for {{ ydb-short-name }}.

## Getting Started

### Prerequisites

To get started with {{ dbt }} on  {{ ydb-short-name }}, you will need:

- Python 3.10 or later.
- dbt Core (1.8 or later).
- An existing {{ ydb-short-name }} cluster; a single‑node installation from the [Quick Start guide](../../quickstart.md) will be sufficient.

{% note info %}

dbt Fusion (2.0) is currently not supported.

{% endnote %}

### Installation

To install {{ dbt‑ydb }}, run the following command:

```bash
pip install dbt-ydb
```

### Running the Example Project

A ready‑to‑use [example project](https://github.com/ydb-platform/dbt-ydb/tree/main/examples/jaffle_shop) is included with the {{ dbt‑ydb }} connector to help you quickly test or explore {{ dbt }}’s capabilities with {{ ydb-short-name }}:

1. Clone the repository

   ```bash
   git clone https://github.com/ydb-platform/dbt-ydb.git
   cd dbt-ydb/examples/jaffle_shop
   ```

2. Configure the connection profile to your {{ ydb-short-name }} in the `profiles.yml` file. Connection and authentication methods are described [here](https://github.com/ydb-platform/dbt-ydb?tab=readme-ov-file#profile-configuration). For a single‑node installation from [{#T}](../../quickstart.md), the file should look like this:

   ```yaml
   profile_name:
     target: dev
     outputs:
       dev:
         type: ydb
         host: localhost # YDB host
         port: 2136 # YDB port
         database: /local # YDB database
         schema: jaffle_shop
   ```

3. Test the connection

   ```bash
   dbt debug
   ```

4. Load test data (seeds)

   ```bash
   dbt seed
   ```

   This command loads CSV files from the `data/` directory into `raw_*` tables in {{ ydb-short-name }}.

5. Run models

   ```bash
   dbt run
   ```

   This creates tables and views based on SQL models from the `models/` directory.

6. Test data in models

   ```bash
   dbt test
   ```

   This verifies data quality according to defined rules.

7. Generate documentation

   ```bash
   dbt docs generate
   dbt docs serve
   ```

   Project documentation will be available in your browser at [http://localhost:8080](http://localhost:8080).

## Next Steps

Refer to the official [dbt documentation](https://docs.getdbt.com/docs/introduction) and the public [{{ dbt‑ydb }}](https://github.com/ydb-platform/dbt-ydb) repository for additional information.