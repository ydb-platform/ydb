# Apache Superset

Apache Superset is a modern data exploration and data visualization platform.

## Installation of needed dependencies

To work with {{ ydb-short-name }} via Superset it is required to install driver [ydb-sqlalchemy](https://pypi.org/project/ydb-sqlalchemy).

Depending on Superset installation it is possible to do it several ways, refer to [official documentation](https://superset.apache.org/docs/configuration/databases/#installing-drivers-in-docker-images) for more details.

## Adding a database connection to {{ ydb-short-name }} {#add-database-connection}

To connect to {{ ydb-short-name }} from Apache Superset, follow these steps:

1. In the Apache Superset toolbar, hover over **Settings** and select **Database Connections**.
1. Click the **+ DATABASE** button.

     The **Connect a database** wizard will appear.

1. In **Step 1** of the wizard, choose **YDB** from **Supported databases** list.
1. In **Step 2** of the wizard, enter the {{ ydb-short-name }} credentials in the corresponding fields:

    * **Display Name**. The {{ ydb-short-name }} connection name in Apache Superset.
    * **SQLAlchemy URI**. The string like `ydb://{host}:{port}/{database_name}`, where **host** and **port** are parts of [endpoint](https://ydb.tech/docs/en/concepts/connect#endpoint) of the {{ ydb-short-name }} cluster to which the connection will be made, and **database_name** - the path to the [database](../../concepts/glossary.md#database).

    ![](_assets/superset-ydb-connection-details.png =400x)

1. Click **CONNECT**.

1. To save the database connection, click **FINISH**.

For additional info about configuration of {{ ydb-short-name }} connection, please refer to [YDB section on official documentation](https://superset.apache.org/docs/configuration/databases#ydb).

## Creating a dataset {#create-dataset}

To create a dataset for a {{ ydb-short-name }} table, follow these steps:

1. In the Apache Superset toolbar, hover over the **+** button and select **SQL query**.
1. In the **DATABASE** drop-down list, select the {{ ydb-short-name }} database connection.

1. Enter the SQL query in the right section of the page. For example, `SELECT * FROM <ydb_table_name>`.

    {% note tip %}

    To create a dataset for a table located in a subdirectory of a {{ ydb-short-name }} database, specify the table path in the table name. For example:

    ```yql
    SELECT * FROM "<path/to/subdirectory/table_name>";
    ```

  {% endnote %}

1. Click **RUN** to test the SQL query.

    ![](_assets/superset-sql-query.png)

1. Click the down arrow next to the **SAVE** button, then click **Save dataset**.

    The **Save or Overwrite Dataset** dialog box appears.

1. In the **Save or Overwrite Dataset** dialog box, select **Save as new**, enter the dataset name, and click **SAVE & EXPLORE**.

After creating datasets, you can use data from {{ ydb-short-name }} to create charts in Apache Superset. For more information, refer to the [Apache Superset](https://superset.apache.org/docs/intro/) documentation.

## Creating a chart {#create-chart}

Let's create a sample chart with the dataset from the `episodes` table that is described in the [YQL tutorial](../../dev/yql-tutorial/index.md).

The table contains the following columns:

* series_id
* season_id
* episode_id
* title
* air_date

Let's say that we want to make a pie chart to show how many episodes each season contains.

To create a chart, follow these steps:

1. In the Apache Superset toolbar, hover over the **+** button and select **Chart**.
1. In the **Choose a dataset** drop-down list, select a dataset for the `episodes` table.
1. In the **Choose chart type** pane, select `Pie chart`.
1. Click **CREATE NEW CHART**.
1. In the **Query** pane, configure the chart:

    * In the **DIMENSIONS** drop-down list, select the `season_id` column.
    * In the **METRIC** field, specify the `COUNT(title)` function.
    * In the **FILTERS** field, specify the `series_id in (2)` filter.

1. Click **CREATE CHART**.

    The pie chart will appear in the preview pane on the right.

    ![](_assets/superset-sample-chart.png)

1. Click **SAVE**.

    The **Save chart** dialog box will appear.

1. In the **Save chart** dialog box, in the **CHART NAME** field, enter the chart name.
1. Click **SAVE**.
