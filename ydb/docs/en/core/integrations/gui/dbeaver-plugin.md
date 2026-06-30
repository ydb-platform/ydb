# Connecting to {{ ydb-short-name }} using the DBeaver plugin

[DBeaver](https://dbeaver.com) is a free cross-platform open-source database management tool that provides a visual interface for connecting to various databases and executing SQL queries.

[YDB DBeaver Plugin](https://github.com/ydb-platform/ydb-dbeaver-plugin) is a DBeaver extension with native support for {{ ydb-short-name }}. Unlike [connecting via JDBC driver](dbeaver.md), the plugin provides a specialized interface for working with {{ ydb-short-name }} objects: hierarchical navigator for tables, topics, views, external data sources, support for all authentication methods, [YQL](../../concepts/glossary.md#yql) editor, execution plan visualization, session and cluster monitoring, access control (ACL) management, and other features.

## Key features of the plugin {#features}

- Connection to {{ ydb-name }} with all [authentication](../../security/authentication.md) methods: anonymous, static, token, service account, metadata.
- Hierarchical object navigator: tables, topics, external data sources, external tables, views.
- System objects: [system views](../../dev/system-views.md) (`.sys`), [resource pools](../../concepts/glossary.md#resource-pool), and [resource pool classifiers](../../concepts/glossary.md#resource-pool-classifier).
- [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting for 150+ keywords and built-in functions.
- Query execution plan visualization (`EXPLAIN` / `EXPLAIN ANALYZE`).
- Active session monitoring via [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Cluster dashboard: CPU load, disk usage, memory, network traffic, node status (updates every 5 seconds).
- [Access control (ACL)](../../security/authorization.md#right) management: grant, revoke, view permissions.
- [Streaming query](../../concepts/glossary.md#streaming-query) management: view, modify, start, stop.
- [Federated queries](../../concepts/query_execution/federated_query/index.md) via external data sources (S3, databases).
- [SQL query converter](../sql-translation/sql-dialect-converter.md) from other dialects (PostgreSQL, MySQL, ClickHouse, and others) to YQL.
- Specialized editors for `JSON`, `JSONDOCUMENT`, `YSON` data types.

## Requirements {#requirements}

The plugin requires DBeaver Community Edition version 24.x or later.

## Installing the plugin {#installation}

The plugin can be installed in two ways:

- **Via P2 repository URL (recommended)**: DBeaver downloads the plugin from cloud storage and remembers the source for subsequent automatic updates.
- **From ZIP archive (GitHub Releases)**: download the archive and install via local file. Suitable for networks where external Eclipse repositories are unavailable, or for reproducible installation of a specific version. Automatic updates are not supported with this method.

### Installation via P2 repository URL {#installation-url}

1. Open DBeaver. In the top menu, select **Help → Install New Software...**.

   {% cut "Screenshot" %}

   ! [](%E2%9F%A6S1%E2%9F%A7)

   {% endcut %}
2. Click the **Add...** button to the right of the **Work with:** field.

   {% cut "Screenshot" %}

   ! [](%E2%9F%A6S1%E2%9F%A7)

   {% endcut %}
3. In the **Add Repository** window that opens, specify a repository name (e.g., `YDB Plugin`) and paste the following URL into the **Location** field:


   ```text
   https://storage.yandexcloud.net/ydb-dbeaver-plugin
   ```


   Click **Add**. DBeaver will download the repository metadata.

   {% cut "Screenshot" %}

   ! [](%E2%9F%A6S1%E2%9F%A7)

   {% endcut %}

   {% note warning %}

   During installation, DBeaver downloads not only the plugin itself but also its dependencies (OSGi components) from Eclipse project servers and other external repositories. If these resources are unavailable from your network, contact your network administrator or use [installation from ZIP archive](#installation-zip).

   {% endnote %}
4. In the component list, the **DBeaver YDB Support** category will appear. Check it and click **Next >**.

   {% cut "Screenshot" %}

   ! [](%E2%9F%A6S1%E2%9F%A7)

   {% endcut %}
5. On the **Install Details** screen, ensure that both components (`org.jkiss.dbeaver.ext.ydb` and `org.jkiss.dbeaver.ext.ydb.ui`) are listed, and click **Next >**.

   {% cut "Screenshot" %}

   ! [](%E2%9F%A6S1%E2%9F%A7)

   {% endcut %}
6. DBeaver may show a warning about unsigned content. This is expected behavior — the plugin JAR files are not signed with a commercial certificate. Click **Install Anyway**.

   {% note info %}

   Eclipse, on which DBeaver is based, checks JAR file signatures to verify authenticity. This open-source plugin is distributed without a signature; the source code is available in the [repository](https://github.com/ydb-platform/ydb-dbeaver-plugin).

   {% endnote %}
7. Review the license (Apache License 2.0), select **I accept the terms of the license agreements**, and click **Finish**.

   {% cut "Screenshot" %}

   ! [](%E2%9F%A6S1%E2%9F%A7)

   {% endcut %}
8. DBeaver will install the plugin and prompt you to restart. Click **Restart Now**. After restart, the plugin will be active.

### Installation from ZIP archive (GitHub Releases) {#installation-zip}

1. Go to the [GitHub Releases page](https://github.com/ydb-platform/ydb-dbeaver-plugin/releases) and download the `ydb-dbeaver-plugin-*.zip` file of the required version.
2. Open DBeaver. In the top menu, select **Help → Install New Software...**.
3. Click the **Add...** button to the right of the **Work with:** field.
4. In the **Add Repository** window that opens, click **Archive...**, specify the path to the downloaded ZIP file, and click **Add**.
5. The **DBeaver YDB Support** category will appear in the component list. Check it.

   {% note warning %}

   Before clicking **Next >**, uncheck the **Contact all update sites during install to find required software** checkbox at the bottom of the **Install New Software** window. If this checkbox is enabled, DBeaver contacts all registered P2 repositories (`dbeaver.io`, `eclipse.org`, and others) when calculating dependencies, and if they are unavailable or respond slowly, the installation dialog hangs at the **Calculating requirements and dependencies** step. The plugin only uses DBeaver components already installed locally, so contacting other repositories during installation is not required.

   {% endnote %}
6. Click **Next >**.
7. On the **Install Details** screen, make sure both components (`org.jkiss.dbeaver.ext.ydb` and `org.jkiss.dbeaver.ext.ydb.ui`) are in the list, and click **Next >**.
8. DBeaver may show a warning about unsigned content. Click **Install Anyway**.
9. Review the license (Apache License 2.0), select **I accept the terms of the license agreements**, and click **Finish**.
10. DBeaver will install the plugin and prompt you to restart. Click **Restart Now**. After the restart, the plugin becomes active.

{% note warning %}

When installing from a ZIP archive, automatic updates are not supported: DBeaver does not know where to look for new versions. To update:

1. Download the new archive from the [GitHub Releases](https://github.com/ydb-platform/ydb-dbeaver-plugin/releases) page.
2. Open **Help → Installation Information**, select the plugin, and click **Uninstall**.
3. Install the new version by following the [instructions for installing from a ZIP archive](#installation-zip).

{% endnote %}

## Creating a connection to {{ ydb-name }} {#connection}

To create a connection to {{ ydb-name }}, follow these steps:

1. In the top menu, select **Database → New Database Connection** (or press `Ctrl+Shift+N`).
2. In the search field, enter `YDB`. Select **YDB** from the list and click **Next**.
3. The {{ ydb-name }} connection settings page will open. Fill in the fields:

   | Field | Description | Example |
   | --- | --- | --- |
   | **Host** | Host of the {{ ydb-name }} cluster [endpoint](../../concepts/connect.md#endpoint) | `ydb.example.com` |
   | **Port** | Port (default `2135`) | `2135` |
   | **Database** | Path to the [database](../../concepts/glossary.md#database) | `/Root/database` |
   | **Monitoring URL** | URL of the [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md) with the database path, used for the dashboard (optional) | `http://ydb.example.com:8765/monitoring/tenant?name=%2FRoot%2Fdatabase` |
   | **Use secure connection** | Use a secure connection (`grpcs://`) | ☑ |
   | **Enable autocomplete API** | Autocomplete via {{ ydb-short-name }} API | ☑ |
4. Select the authentication method from the **Auth type** dropdown (see [Authentication methods](#auth-methods)).
5. Click the **Test Connection** button to verify the settings. If the connection is successful, a dialog will appear with the connection time in milliseconds.
6. Click the **Finish** button. The connection will appear in the **Database Navigator** panel.

## Authentication methods {#auth-methods}

The plugin supports all [authentication](../../security/authentication.md) methods available in {{ ydb-short-name }}. The method is selected from the **Auth type** dropdown on the connection settings page.

### Anonymous {#auth-anonymous}

Connection without credentials. Used for local or test {{ ydb-short-name }} installations. No additional fields need to be filled.

### Static (login and password) {#auth-static}

Authentication by login and password. Enter the username in the **User** field and the password in the **Password** field. on the {{ ydb-short-name }} server. Used if [login and password authentication](../../security/authentication.md#static-credentials) is enabled

{% note info %}

In managed {{ ydb-name }} installations, login and password authentication is disabled: managed services use the cloud platform's centralized access control system ( [IAM](https://yandex.cloud/en/docs/iam/)).

{% endnote %}

### Token {#auth-token}

Authentication by [IAM](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token) or [OAuth token](https://yandex.cloud/en/docs/iam/concepts/authorization/oauth-token). Enter the token in the **Token** field. The token is passed in the header of each request.

### Service Account {#auth-service-account}

Authentication by a Yandex Cloud [service account](https://yandex.cloud/en/docs/iam/concepts/users/service-accounts) key. Specify the path to the JSON key file in the **SA Key File** field (use the **...** button to select the file). For more information on how to create an authorized key, see the [Yandex Cloud documentation](https://yandex.cloud/en/docs/iam/operations/authentication/manage-authorized-keys).

Key file format:


```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```


### Metadata {#auth-metadata}

Authentication via [Yandex Cloud Metadata Service](https://yandex.cloud/en/docs/compute/operations/vm-metadata/get-vm-metadata). The plugin obtains an IAM token from the virtual machine's metadata service. It is used only when DBeaver is running on a Yandex Cloud virtual machine.

## Object Navigator {#object-navigator}

After connection, the **Database Navigator** panel displays the {{ ydb-short-name }} object hierarchy. The root node is the connection, inside it is the database path, which contains the following folders:

- **Tables** — tables organized into subdirectories according to the path in {{ ydb-short-name }} (for example, a table at path `folder1/subfolder/mytable` will be nested in `folder1 → subfolder`).
- **Topics** — [topics](../../concepts/datamodel/topic.md).
- **Views** — [views](../../concepts/datamodel/view.md).
- **External Data Sources** — [external data sources](../../concepts/glossary.md#external-data-source).
- **External Tables** — [external tables](../../concepts/glossary.md#external-table).
- **System Views (.sys)** — [system views](../../dev/system-views.md), such as `partition_stats`, `query_sessions`.
- **Resource Pools** — [resource pools](../../concepts/glossary.md#resource-pool).

## Working with the plugin {#capabilities}

### YQL Editor {#yql-editor}

Open the **SQL Editor** (`F3` or double-click the connection). The editor supports:

- Syntax highlighting for [YQL](../../yql/reference/index.md): keywords (`UPSERT`, `REPLACE`, `EVALUATE`, `PRAGMA`, `WINDOW` and 145+ others), data types, built-in functions.
- Auto-completion of table names, columns, and functions.
- Query execution: `Ctrl+Enter` — current query, `Ctrl+Shift+Enter` — entire script.

Example YQL query:


```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```


### EXPLAIN and execution plan {#explain}

Click **Explain** (or `Ctrl+Shift+E`) to get the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md). The plugin displays:

- **Text plan** — operation tree in text form.
- **Diagram** — graphical representation as a DAG.
- **SVG plan** — interactive visualization.

`EXPLAIN ANALYZE` additionally shows execution statistics (row count, execution time).

### Session Manager {#session-manager}

Select the menu item **Tools → Sessions Manager**, or right-click the connection and select the corresponding item. The opened view displays all active sessions with the current query, state, and duration (data from the system view [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). The **Hide Idle** checkbox hides sessions without an active query.

### Cluster Dashboard {#cluster-dashboard}

Open the **Dashboard** tab in the connection editor (requires the **Monitoring URL** field to be filled in when configuring the connection).

{% note warning %}

The dashboard is only available when working with self-hosted {{ ydb-short-name }} installations that have access to the [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md). In Yandex Cloud Managed Service for {{ ydb-short-name }}, the Embedded UI is not published, so the dashboard data is unavailable — use [cloud platform tools](https://yandex.cloud/en/docs/ydb/operations/monitoring) for monitoring.

{% endnote %}

The dashboard displays in real time (updates every 5 seconds):

- CPU load by node.
- Disk space usage.
- Memory usage.
- Network traffic.
- Number of running queries.
- Cluster node status.

### Streaming Queries {#streaming-queries}

In the navigator, expand the **Streaming Queries** folder. For each query, the following are available:

- View the source YQL.
- View errors (issues).
- View the execution plan.
- Actions: **Start**, **Stop**, **Alter**.

### SQL Dialect Converter {#convert-dialect}

The plugin allows you to convert an SQL query written in another dialect (PostgreSQL, MySQL, ClickHouse, and others) to YQL. The converter is available in the context menu **Tools → Convert Dialect** in the connection editor.

To convert a query:

1. In the **Source Dialect** drop-down list, select the source SQL dialect. The list of dialects is requested from the plugin's external service when the tab is first opened.
2. Paste the source SQL code into the **Input SQL** field.
3. Click **Convert**. The result appears in the lower field.
4. Click **Copy** to copy the result to the clipboard.

For more details on how the converter works, supported dialects, and limitations, see the article [SQL Dialect Converter to YQL](../sql-translation/sql-dialect-converter.md).

{% note warning %}

To perform the conversion, the plugin sends the source query to an external HTTPS service. The converter does not work without internet access. Do not use the converter for queries containing confidential data.

{% endnote %}

### Creating Objects {#create-objects}

Right-click a folder or object and select **Create New**:

- **Create Table** — create a new table.
- **Create Topic** — create a new topic.

## Updating the plugin {#updates}

Automatic updates are only supported when [installing via URL of the P2 repository](#installation-url). DBeaver remembers the source (repository URL) and, when a new version is published, offers an update in one of two ways:

1. Automatically on the next DBeaver launch (if update checking is enabled in **Window → Preferences → Install/Update → Automatic Updates**).
2. Manually via **Help → Check for Updates**: select the available update and follow the same steps as during the initial installation (license → unsigned content warning → restart).

For [installation from a ZIP archive](#installation-zip), the update is performed manually: download the new archive from the [GitHub Releases](https://github.com/ydb-platform/ydb-dbeaver-plugin/releases) page, open **Help → Installation Information**, select the plugin, click **Uninstall**, then install the new version again.
