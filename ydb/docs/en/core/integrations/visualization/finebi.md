# FineBI

FineBI is a powerful tool for big data analysis. FineBI enables organizations to analyze data and share it for informed decision-making. It allows you to visualize raw data, track key performance indicators (KPIs), identify trends, and forecast future results.

[PostgreSQL compatibility mode in {{ ydb-short-name }}](../../postgresql/intro.md) allows you to use [FineBI](https://intl.finebi.com/) to execute queries and visualize data from {{ ydb-short-name }}. In this case, FineBI works with {{ ydb-short-name }} as with PostgreSQL.

{% include [../../postgresql/_includes/alert_preview.md](../../postgresql/_includes/alert_preview.md) %}

## Prerequisites

You will need the following:

* [FineBI](https://intl.finebi.com/);
* PostgreSQL JDBC driver loaded into FineBI.

  {% note info %}

  You can download the latest PostgreSQL JDBC driver from the [download page](https://jdbc.postgresql.org/download/) of the PostgreSQL website. To load the PostgreSQL JDBC driver into FineBI, follow the [instructions](https://help.fanruan.com/finebi-en/doc-view-1540.html) in the FineBI documentation.

  {% endnote %}

## Creating a connection to {{ ydb-short-name }} {#add-database-connection}

To create a connection to {{ ydb-short-name }} from FineBI using the PostgreSQL network protocol, follow these steps:

1. Log in to your `admin` account on FineBI.
2. Go to System Management > Data Connection > Data Connection Management.
3. Click the New Data Connection button.
4. To find the PostgreSQL icon, enter `postgresql` in the Search field.
5. Click the PostgreSQL icon.
6. Enter the connection details to {{ ydb-short-name }} in the following fields:

   * Data Connection Name — the name of the connection to {{ ydb-short-name }} in FineBI.
   * Driver — the driver for connecting FineBI to {{ ydb-short-name }}.

     Select `Custom` and the installed JDBC driver `org.postgresql.Driver`.
   * Database Name — the path to the [database](../../concepts/glossary.md#database) in the {{ ydb-short-name }} cluster that will be queried.

     {% note alert %}

     Special characters in the database path must be [encoded](https://en.wikipedia.org/wiki/Percent-encoding). For example, make sure all slashes (`/`) are replaced with `%2F`.

     {% endnote %}
   * Host — the [endpoint](../../concepts/connect.md#endpoint) of the {{ ydb-short-name }} cluster to which the connection is made.
   * Port — the port of the {{ ydb-short-name }} endpoint.
   * Username — the login for connecting to the {{ ydb-short-name }} database.
   * Password — the password for connecting to the {{ ydb-short-name }} database.

   ![](_assets/finebi/finebi-database-connection.png =600x)
7. Click the Test Connection button.

   If the connection details are correct, you will see a message about the successful connection establishment.
8. To save the connection, click the Save button.

   The created connection to {{ ydb-short-name }} will appear in the Data Connection list.

## Creating a dataset (SQL dataset) {#create-dataset}

To create a dataset from a {{ ydb-short-name }} table, follow these steps:

1. In FineBI, open the Public Data tab.
2. Select the folder where you want to create the dataset.

   {% note warning %}

   You must have [Public Data Management](https://help.fanruan.com/finebi-en/doc-view-5734.html) permissions for the selected folder in FineBI.

   {% endnote %}
3. Click the Add Dataset button and select SQL Dataset from the dropdown list.
4. In the Table Name field, enter the dataset name.
5. In the Data from Data Connection dropdown list, select the connection to YDB.
6. In the SQL Statement field, enter the SQL query text to retrieve all columns from the {{ ydb-short-name }} table. For example, `SELECT * FROM <YDB_table_name>`.

   {% note tip %}

   If you want to create a dataset from a table located in a subdirectory of {{ ydb-short-name }}, you need to specify the table path in the table name itself. For example:


   ```yql
   SELECT * FROM "<path/to/subdirectory/table_name>";
   ```

   {% endnote %}
7. Click the Preview button to check the SQL query. If the SQL query is correct, a table with data will appear on the right side of the page.

   ! [](%E2%9F%A6S1%E2%9F%A7)
8. To save the dataset, click the OK button.

After creating datasets, you can use data from {{ ydb-short-name }} to create charts in FineBI. See the [FineBI](https://help.fanruan.com/finebi-en/) documentation.

## Creating a chart {#create-chart}

Now let's create an example chart using a dataset from the table, the [creation](../../dev/yql-tutorial/create_demo_tables.md) and [data population](../../dev/yql-tutorial/fill_tables_with_data.md) of which are described in the [YQL Tutorial](../../dev/yql-tutorial/index.md). We will create a pie chart showing how many `episodes` each season of the series contains.

The `episodes` table contains the following columns:

* `series_id`;
* `season_id`;
* `episode_id`;
* `title`;
* `air_date`.

To create a chart, follow these steps:

1. In FineBI, open the My Analysis tab.
2. Click the New Subject button.

   The Select Data dialog box will open.
3. In the Select Data window, select the dataset for the `episodes` table and click the OK button.
4. Open the Component tab at the bottom of the page.
5. On the **Chart Type** panel, click the **Pie Chart** icon.
6. In the column list of the `episodes` dataset, click the arrow next to the `episode_id` column and select **Convert to Dimension** from the dropdown list.

   ![](_assets/finebi/finebi-convert2dimension.png =350x)
7. Drag the `season_id` column to the **Color** field.
8. Drag the `title` column to the **Label** field.
9. Drag the `series_id` column to the **Filter** field.

   The **Add Filter to episodes.series_id** dialog box opens.
10. In the opened **Add Filter to episodes.series_id** window, select the `Detailed Value` item and click the **Next Step** button.
11. Specify the following condition:

    `series_id` `Equal To` `Fixed Value` `2`
12. Click the **OK** button.

    The chart will use data only for the series with the `2` identifier.

    ! [](%E2%9F%A6S1%E2%9F%A7)
13. Click the **Save** button.
