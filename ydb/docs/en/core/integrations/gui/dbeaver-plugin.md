# Connecting to {{ ydb-short-name }} Using the DBeaver Plugin

[DBeaver](https://dbeaver.com) is a free cross-platform open-source database management tool that provides a visual interface for connecting to various databases and executing SQL queries.

[YDB DBeaver Plugin](https://github.com/ydb-platform/ydb-dbeaver-plugin) is a DBeaver extension with native support for {{ ydb-short-name }}. Unlike [connecting via a JDBC driver](dbeaver.md), the plugin provides a specialized interface for working with {{ ydb-short-name }} objects: a hierarchical navigator for tables, topics, views, external data sources, support for all authentication methods, a [YQL](../../concepts/glossary.md#yql) editor, query execution plan visualization, session and cluster monitoring, access control (ACL) management, and other features.

## Key Features of the Plugin {#features}

- Connecting to {{ ydb-name }} with all [authentication methods](../../security/authentication.md): anonymous, static, token-based, service account, metadata-based.
- Hierarchical object navigator: tables, topics, external data sources, external tables, views.
- System objects: [system views](../../dev/system-views.md) (`.sys`), [resource pools](../../concepts/glossary.md#resource-pool), and [resource pool classifiers](../../concepts/glossary.md#resource-pool-classifier).
- [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting for over 150 keywords and built-in functions.
- Visualization of the query execution plan (`EXPLAIN` / `EXPLAIN ANALYZE`).
- Monitoring of active sessions via [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Cluster dashboard: CPU load, disk usage, memory usage, network traffic, node status (updated every 5 seconds).
- [Access control (ACL)](../../security/authorization.md#right) management: granting, revoking, and viewing permissions.
- Management of [streaming queries](../../concepts/glossary.md#streaming-query): viewing, modifying, starting, stopping.
- [Federated queries](../../concepts/query_execution/federated_query/index.md) via external data sources (S3, databases).
- [SQL Dialect Converter](../sql-dialect-converter.md) from other dialects (PostgreSQL, MySQL, ClickHouse, etc.) to YQL.
- Specialized editors for `JSON`, `JSONDOCUMENT`, `YSON` data types.

## Requirements {#requirements}

The plugin requires DBeaver Community Edition version 24.x or later.

## Installing the Plugin {#installation}

The plugin is installed via DBeaver's extension installation mechanism from a URL repository, which ensures automatic updates.

1. Open DBeaver. In the top menu, select **Help → Install New Software...**.

    {% cut "Screenshot" %}

    ![](./_assets/dbeaver-plugin-context.png)

    {% endcut %}

1. Click the **Add...** button to the right of the **Work with:** field.

    {% cut "Screenshot" %}

    ![](./_assets/dbeaver-plugin-add.png)

    {% endcut %}

1. In the **Add Repository** window that opens, specify a repository name (for example, `YDB Plugin`) and paste the following URL into the **Location** field:

    ```text
    https://storage.yandexcloud.net/ydb-dbeaver-plugin
    ```

    Click **Add**. DBeaver will load the repository metadata.

    {% cut "Screenshot" %}

    ![](./_assets/dbeaver-plugin-add-repository.png)

    {% endcut %}

1. The **DBeaver YDB Support** category will appear in the component list. Select it and click **Next >**.

    {% cut "Screenshot" %}

    ![](./_assets/dbeaver-plugin-ydb-support.png)

    {% endcut %}

1. On the **Install Details** screen, make sure both components (`org.jkiss.dbeaver.ext.ydb` and `org.jkiss.dbeaver.ext.ydb.ui`) are listed, then click **Next >**.

    {% cut "Screenshot" %}

    ![](./_assets/dbeaver-plugin-install-details.png)

    {% endcut %}

1. DBeaver may display a warning about unsigned content. This is expected behavior — the plugin's JAR files are not signed with a commercial certificate. Click **Install Anyway**.

    {% note info %}

    Eclipse, on which DBeaver is based, checks JAR file signatures to verify authenticity. This open-source plugin is distributed without a signature; the source code is available in the [repository](https://github.com/ydb-platform/ydb-dbeaver-plugin).

    {% endnote %}

1. Review the license (Apache License 2.0), select **I accept the terms of the license agreements**, and click **Finish**.

    {% cut "Screenshot" %}

    ![](./_assets/dbeaver-plugin-license.png)

    {% endcut %}

1. DBeaver will install the plugin and prompt you to restart. Click **Restart Now**. After restarting, the plugin will become active.

## Creating a Connection to {{ ydb-name }} {#connection}

To create a connection to {{ ydb-name }}, follow these steps:

1. In the top menu, select **Database → New Database Connection** (or press `Ctrl+Shift+N`).
1. In the search field, type `YDB`. Select **YDB** from the list and click **Next**.
1. The {{ ydb-name }} connection setup page will open. Fill in the fields:

    | Field | Description | Example |
    |------|----------|--------|
    | **Host** | Host of the [endpoint](../../concepts/connect.md#endpoint) for the {{ ydb-name }} cluster | `ydb.example.com` |
    | **Port** | Port (default is `2135`) | `2135` |
    | **Database** | Path to the [database](../../concepts/glossary.md#database) | `/Root/database` |
    | **Monitoring URL** | URL of the [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md) with the database path, used for the dashboard (optional) | `http://ydb.example.com:8765/monitoring/tenant?name=%2FRoot%2Fdatabase` |
    | **Use secure connection** | Use a secure connection (`grpcs://`) | ☑ |
    | **Enable autocomplete API** | Autocomplete via {{ ydb-short-name }} API | ☑ |

1. Select an authentication method from the **Auth type** dropdown list (see [Authentication Methods](#auth-methods)).
1. Click the **Test Connection** button to verify the settings. If the connection is successful, a dialog will appear with the connection time in milliseconds.
1. Click the **Finish** button. The connection will appear in the **Database Navigator** panel.

## Authentication Methods {#auth-methods}

The plugin supports all [authentication methods](../../security/authentication.md) available in {{ ydb-short-name }}. The method is selected from the **Auth type** dropdown list on the connection setup page.

### Anonymous {#auth-anonymous}

Connection without credentials. Used for local or test installations of {{ ydb-short-name }}. No additional fields need to be filled out.

### Static (Username and Password) {#auth-static}

Authentication using a username and password. Enter the username in the **User** field and the password in the **Password** field. Used if {{ ydb-short-name }} server has [username and password authentication](../../security/authentication.md#static-credentials) enabled.

{% note info %}

In managed installations of {{ ydb-name }}, username and password authentication is disabled: managed services use the centralized access control system of the cloud platform ([IAM](https://yandex.cloud/ru/docs/iam/)).

{% endnote %}

### Token {#auth-token}

Authentication using an [IAM](https://yandex.cloud/ru/docs/iam/concepts/authorization/iam-token) or [OAuth token](https://yandex.cloud/ru/docs/iam/concepts/authorization/oauth-token). Enter the token in the **Token** field. The token is passed in the header of each request.

### Service Account {#auth-service-account}

Authentication using the key of a [Yandex Cloud service account](https://yandex.cloud/ru/docs/iam/concepts/users/service-accounts). Specify the path to the JSON file with the key in the **SA Key File** field (use the **...** button to select the file). For more information on how to create an authorized key, see the [Yandex Cloud documentation](https://yandex.cloud/ru/docs/iam/operations/authentication/manage-authorized-keys).

Key file format:

```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```

### Metadata {#auth-metadata}

Authentication via the [Yandex Cloud metadata service](https://yandex.cloud/ru/docs/compute/operations/vm-metadata/get-vm-metadata). The plugin obtains an IAM token from the virtual machine's metadata service. Used only when DBeaver is running on a Yandex Cloud virtual machine.

## Object Navigator {#object-navigator}

After connecting, the {{ ydb-short-name }} object hierarchy is displayed in the **Database Navigator** panel. The root node is the connection, and inside it is the database path, which contains the following folders:

- **Tables** — tables organized by subdirectories according to the path in {{ ydb-short-name }} (for example, a table with the path `folder1/subfolder/mytable` will be nested under `folder1 → subfolder`).
- **Topics** — [topics](../../concepts/datamodel/topic.md).
- **Views** — [views](../../concepts/datamodel/view.md).
- **External Data Sources** — [external data sources](../../concepts/glossary.md#external-data-source).
- **External Tables** — [external tables](../../concepts/glossary.md#external-table).
- **System Views (.sys)** — [system views](../../dev/system-views.md), such as `partition_stats`, `query_sessions`.
- **Resource Pools** — [resource pools](../../concepts/glossary.md#resource-pool).

## Working with the Plugin {#capabilities}

### YQL Editor {#yql-editor}

Open the **SQL Editor** (`F3` or double-click on the connection). The editor supports:

- Syntax highlighting for [YQL](../../yql/reference/index.md): keywords (`UPSERT`, `REPLACE`, `EVALUATE`, `PRAGMA`, `WINDOW`, and 145+ others), data types, built-in functions.
- Autocompletion of table, column, and function names.
- Query execution: `Ctrl+Enter` for the current query, `Ctrl+Shift+Enter` for the entire script.

Example YQL query:

```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```

### EXPLAIN and Execution Plan {#explain}

Click **Explain** (or `Ctrl+Shift+E`) to get the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md). The plugin displays:

- **Text plan** — a tree of operations in text form.
- **Diagram** — a graphical representation as a DAG.
- **SVG plan** — an interactive visualization.

`EXPLAIN ANALYZE` additionally shows execution statistics (number of rows, execution time).

### Session Manager {#session-manager}

Right-click on the connection and select **Manage Sessions**, or use the **Database → Manage Sessions** menu item. The opened view displays all active sessions with the current query, state, and duration (data from the system view [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). The **Hide Idle** checkbox hides sessions without an active query.

### Cluster Dashboard {#cluster-dashboard}

Open the **Dashboard** tab in the connection editor (requires the **Monitoring URL** field to be filled in during connection setup).

{% note warning %}

The dashboard is only available when working with self-hosted installations of {{ ydb-short-name }} where access to the [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md) is available. In Yandex Cloud Managed Service for {{ ydb-short-name }}, Embedded UI is not published, so dashboard data is unavailable — use [cloud platform tools](https://yandex.cloud/ru/docs/ydb/operations/monitoring) for monitoring.

{% endnote %}

The dashboard displays real-time data (updated every 5 seconds):

- CPU load by nodes.
- Disk space usage.
- Memory usage.
- Network traffic.
- Number of executing queries.
- Status of cluster nodes.

### Streaming Queries {#streaming-queries}

In the navigator, expand the **Streaming Queries** folder. For each query, you can:

- View the original YQL.
- View errors (issues).
- View the execution plan.
- Perform actions: **Start**, **Stop**, **Alter**.

### SQL Dialect Converter {#convert-dialect}

The plugin allows you to convert an SQL query written in another dialect (PostgreSQL, MySQL, ClickHouse, etc.) to YQL. The converter is available on the **Convert Dialect** tab in the connection editor.

To convert a query:

1. In the **Source Dialect** dropdown list, select the source SQL dialect. The list of dialects is requested from the plugin's external service when the tab is first opened.
1. Paste the source SQL code into the **Input SQL** field.
1. Click **Convert**. The result will appear in the lower field.
1. Click **Copy** to copy the result to the clipboard.

For more information on how the converter works, supported dialects, and limitations, see the [SQL Dialect Converter to YQL](../sql-dialect-converter.md) article.

{% note warning %}

The plugin sends the original query to an external HTTPS service for conversion. Do not use the converter for queries containing confidential data.

{% endnote %}

### Creating Objects {#create-objects}

Right-click on a folder or object and select **Create New**:

- **Create Table** — create a new table.
- **Create Topic** — create a new topic.
- **Create Resource Pool** — create a resource pool.

## Updating the Plugin {#updates}

DBeaver uses the Eclipse P2 mechanism to detect and install updates. When you install the plugin, DBeaver remembers the source — the repository URL. When a new version is published, DBeaver compares the installed version with the version in the repository and offers an update in one of two ways:

1. Automatically on the next DBeaver startup (if update checks are enabled in **Window → Preferences → Install/Update → Automatic Updates**).
1. Manually via **Help → Check for Updates**: select the available update and follow the same steps as during the initial installation (license → warning about unsigned content → restart).
