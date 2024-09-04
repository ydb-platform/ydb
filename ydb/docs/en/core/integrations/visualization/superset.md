# Apache Superset

Apache Superset is a modern data exploration and data visualization platform.

Support for the [PostgreSQL wire protocol](https://ydb.tech/docs/en/postgresql/intro) in {{ ydb-short-name }} allows using [Apache Superset](https://superset.apache.org/) to query and visualize data from {{ ydb-short-name }}.

## Prerequisites

Before you begin, make sure that the following software is installed:

- [Apache Superset](https://superset.apache.org/).
- PostgreSQL driver for Apache Superset.
  For information on how to install the PostgreSQL driver, see the [Apache Superset documentation](https://superset.apache.org/docs/configuration/databases#installing-database-drivers).

## Adding a Database Connection to {{ ydb-short-name }} {#add-database-connection}

To connect to {{ ydb-short-name }} from Apache Superset by using the PostgreSQL wire protocol, perform the following steps:

1. In the Apache Superset Toolbar, hover over **Settings** and choose **Database Connections**.

1. Click the **+ DATABASE** button.
   The **Connect a database** wizard appears.

1. On **Step 1** of the wizard, click the **PostgreSQL** button.

1. On **Step 2** of the wizard, enter the {{ ydb-short-name }} credentials in the associated fields:

   * **HOST**. The [endpoint](https://ydb.tech/docs/en/concepts/connect#endpoint) of the {{ ydb-short-name }} cluster, to which the connection will be made.

   * **PORT**. The port of the {{ ydb-short-name }} endpoint.

   * **DATABASE NAME**. The path to the [database](https://ydb.tech/docs/en/concepts/glossary#database) in the {{ ydb-short-name }} cluster, to which queries will be made.

   * **USERNAME**. The login for connecting to the {{ ydb-short-name }} database.

   * **PASSWORD**. The password for connecting to the {{ ydb-short-name }} database.

   * **DISPLAY NAME**. The name of the {{ ydb-short-name }} connection in Apache Superset.

   ![](_assets/superset-ydb-connection-details.png =400x)

1. Click **CONNECT**.

1. To save the database connection, click **FINISH**.

## Creating a Dataset {#create-dataset}

To create a dataset for a {{ ydb-short-name }} table, perform the following steps:

1. In the Apache Superset Toolbar, hover your cursor over **Settings** and choose **SQL query**.

1. In the **DATABASE** drop-down list, choose the {{ ydb-short-name }} database connection.

1. In the **SCHEMA** drop-down list, choose `public`.

   {% note alert %}

   YDB currently does not provide table schema info via the PostgreSQL protocol. You can skip choosing a table in the **SEE TABLE SCHEMA** drop-down list.

   {% endnote %}

1. Enter the SQL query in the right part of the page. For example, `SELECT * FROM <ydb_table_name>`.

   {% note tip %}

   To create a dataset for a table that is located in a subdirectory of the {{ ydb-short-name }} database, specify a path to the table in the table name. For example, `SELECT * FROM '<path/to/table/table_name>'`.

   {% endnote %}

1. Click **RUN** to test the SQL query.

   ![](_assets/superset-sql-query.png)

1. Click the down arrow next to the **SAVE** button and then click **Save dataset**.

   The **Save or Overwrite Dataset** dialog box appears.

1. In the **Save or Overwrite Dataset** dialog box, select **Save as new**, enter the dataset name, and click **SAVE & EXPLORE**.

After you create datasets, you can use data from {{ ydb-short-name }} to create charts in Apache Superset. For more information, see the [Apache Superset](https://superset.apache.org/docs/intro/) documentation.
