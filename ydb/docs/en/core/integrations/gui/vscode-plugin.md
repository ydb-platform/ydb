# Connecting to {{ ydb-short-name }} using the VS Code plugin

[Visual Studio Code](https://code.visualstudio.com) is a free cross-platform open-source code editor that supports a wide ecosystem of extensions for working with databases, language servers, and development tools.

[YDB for VS Code](https://github.com/ydb-platform/ydb-vscode-plugin) is a VS Code extension with native support for {{ ydb-short-name }}. The plugin provides a specialized interface for working with {{ ydb-short-name }} objects: a hierarchical navigator for tables, topics, views, external data sources, support for all authentication methods, a [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting, visualization of execution plans, monitoring of sessions and the cluster, access control management (ACL), a built-in [MCP server](https://modelcontextprotocol.io/) for AI assistants, and other features.
## Key Features of the Plugin {#features}

- Connecting to {{ ydb-name }} with all [authentication methods](../../security/authentication.md): anonymous, static, token-based, service account, metadata-based.
- Hierarchical object navigator: tables ([row-oriented](../../concepts/glossary.md#row-oriented-table) and [column-oriented](../../concepts/glossary.md#column-oriented-table)), [topics](../../concepts/datamodel/topic.md), [views](../../concepts/datamodel/view.md), [external data sources](../../concepts/glossary.md#external-data-source), [external tables](../../concepts/glossary.md#external-table), [transfers](../../concepts/transfer.md), [streaming queries](../../concepts/glossary.md#streaming-query).
- System objects: [system views](../../dev/system-views.md) (`.sys`), [resource pools](../../concepts/glossary.md#resource-pool).
- YQL editor with syntax highlighting, table and column autocompletion.
- Query execution and results visualization: table, JSON, chart.
- Visualization of the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md) (`EXPLAIN`).
- Monitoring active sessions via [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Cluster dashboard based on [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md): CPU load, memory usage, network traffic (updated every 10 seconds).
- Managing [access rights (ACL)](../../security/authorization.md#right): viewing permissions for database objects.
- Generating DDL scripts ([`CREATE`](../../yql/reference/syntax/create_table/index.md)) for any database object.
- Managing [streaming queries](../../concepts/glossary.md#streaming-query): viewing, starting, stopping.
- [SQL query converter](../sql-dialect-converter.md) from other dialects (PostgreSQL, MySQL, ClickHouse, etc.) to YQL.
- Built-in [MCP server](#mcp) — direct access to databases from AI assistants (Claude Code and others).
- [Semantic search for YQL documentation](#rag) (RAG) for AI-assisted query writing.
## Requirements {#requirements}

The plugin requires Visual Studio Code version 1.75.0 or later.
## Plugin installation {#installation}

The plugin can be installed from the VS Code Marketplace or from a `.vsix` file on the GitHub Releases page.
### Installing from the VS Code Marketplace {#install-marketplace}

1. Open the Extensions panel in VS Code (`Ctrl+Shift+X` / `Cmd+Shift+X`).
1. In the search bar, enter `YDB for VS Code` and select the extension from the publisher `ydb-tech` ([direct link](https://marketplace.visualstudio.com/items?itemName=ydb-tech.ydb-vscode-plugin)).
1. Click **Install**.

Alternatively, you can install the plugin from the Marketplace with a single command in the terminal:
```bash
code --install-extension ydb-tech.ydb-vscode-plugin
```
### Installation from a VSIX file {#install-vsix}

This method is suitable if you need a specific version or do not have access to the Marketplace.

1. Go to the [GitHub Releases page](https://github.com/ydb-platform/ydb-vscode-plugin/releases) and download the `ydb-vscode-plugin-*.vsix` file of the required version.

1. Install the extension using one of the methods:

    - **Via the terminal:**
        ```bash
        code --install-extension ydb-vscode-plugin-X.X.X.vsix
        ```
Where `X.X.X` is the number of the downloaded version.

- **Via the VS Code interface:**

    1. Open the extensions panel (`Ctrl+Shift+X` / `Cmd+Shift+X`).
    1. Click `...` (three dots) in the top right corner of the panel.
    1. Select **Install from VSIX...**.
    1. Specify the path to the downloaded `.vsix` file.

After installing using any of the methods, restart VS Code. The **YDB** icon will appear in the Activity Bar.
## Creating a connection to {{ ydb-name }} {#connection}

1. Click the **YDB** icon in the Activity Bar on the left.
1. In the **Connections** panel, click the **Add Connection** button (the `+` icon).
1. The connection creation form will open. Fill in the fields:
| Field | Description | Example |
|------|----------|--------|
| **Connection Name** | Arbitrary connection name | `my-ydb` |
| **Host** | Host of the [endpoint](../../concepts/connect.md#endpoint) of the {{ ydb-name }} cluster | `ydb.example.com` |
| **Port** | Port (default `2135`) | `2135` |
| **Database** | Path to the [database](../../concepts/glossary.md#database) | `/Root/database` |
| **Monitoring URL** | URL of [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md), used for the dashboard (filled in automatically based on the host, can be overridden) | `http://ydb.example.com:8765` |
| **Secure connection (grpcs)** | Use a secure connection (`grpcs://`) | ☑ |
| **Use RAG** | Enable [YQL documentation search](#rag) for this connection | ☑ |
1. If necessary, specify the path to the custom CA certificate (PEM) in the **CA Certificate File** field for connections with non-standard TLS. If the field is left empty, the built-in Yandex Cloud certificate is used.

1. Select an authentication method from the **Auth type** dropdown list (see [Authentication methods](#auth-methods)).

1. Click **Test Connection** to check the settings. If the connection is successful, a success message will appear.

1. Click **Save**. The connection will appear in the **Connections** panel.
## Authentication methods {#auth-methods}

The plugin supports all [authentication methods](../../security/authentication.md) available in {{ ydb-short-name }}. The method is selected in the **Auth type** dropdown list on the connection creation form.
### Anonymous {#auth-anonymous}

Connection without credentials. Used for local or test installations of {{ ydb-short-name }}. No additional fields need to be filled in.
### Static Credentials (login and password) {#auth-static}

Authentication using login and password. Specify the username in the **Username** field and the password in the **Password** field. Used if {{ ydb-short-name }} server has [login and password authentication enabled](../../security/authentication.md#static-credentials).
{% note info %}

In managed installations of {{ ydb-name }}, authentication via login and password is disabled: managed services use the centralized access management system of the cloud platform ([IAM](https://yandex.cloud/en/docs/iam/)).

{% endnote %}
### Access Token {#auth-token}

Authentication using [IAM-](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token) or [OAuth token](https://yandex.cloud/en/docs/iam/concepts/authorization/oauth-token). Enter the token in the **Token** field. The token is passed in the header of each request.
{% note warning %}

The IAM token has a limited [lifespan — no more than 12 hours](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime) — after which it must be obtained again and updated in the connection settings. For long-lived connections, use authentication via a [service account](#auth-service-account) or [metadata service](#auth-metadata).

{% endnote %}
### Service Account Key File {#auth-service-account}

Authentication using the [service account](https://yandex.cloud/en/docs/iam/concepts/users/service-accounts) key of Yandex Cloud. Specify the path to the JSON file with the key in the **Service Account Key File** field (use the **Browse** button to select the file). For more information on how to create an authorized key, see the [Yandex Cloud documentation](https://yandex.cloud/en/docs/iam/operations/authentication/manage-authorized-keys).

The format of the key file:
```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```
### Metadata Service {#auth-metadata}

Authentication via the [Yandex Cloud metadata service](https://yandex.cloud/en/docs/compute/operations/vm-metadata/get-vm-metadata). The plugin receives an IAM token from the virtual machine's metadata service. It is used only when VS Code is running on a Yandex Cloud virtual machine.
## Object Navigator {#object-navigator}

After connecting, click on the connection in the **Connections** panel — the **Navigator** panel will open with the hierarchy of {{ ydb-short-name }} objects. The navigator contains the following sections:

- **Tables** — tables (string and columnar), organized into subdirectories according to the path in {{ ydb-short-name }}.
- **System Views** — [system views](../../dev/system-views.md) (`.sys`), such as `query_sessions`, `partition_stats`.
- **Views** — [views](../../concepts/datamodel/view.md).
- **Topics** — [topics](../../concepts/datamodel/topic.md).
- **External Data Sources** — [external data sources](../../concepts/glossary.md#external-data-source).
- **External Tables** — [external tables](../../concepts/glossary.md#external-table).
- **Resource Pools** — [resource pools](../../concepts/glossary.md#resource-pool).
- **Transfers** — data transfers.
- **Streaming Queries** — [streaming queries](../../concepts/glossary.md#streaming-query).

Right-clicking on any object in the navigator opens a context menu with available actions.
## Working with the plugin {#capabilities}
### Query Workspace {#query-workspace}

Open the query workspace using `Ctrl+Shift+Q` (`Cmd+Shift+Q` on macOS) or click **Open Query Workspace** in the connection context menu. In the workspace, you can write and execute YQL queries, view history and results.

To quickly open the editor with a pre-filled query, right-click on a table or view in the navigator and select:

- **Show Preview** — `SELECT` of the first 100 rows.
- **Make Query** — `SELECT` with the object name.
### YQL Editor {#yql-editor}

The editor supports:

- Syntax highlighting for [YQL](../../yql/reference/index.md): keywords, data types, built-in functions.
- Autocompletion of table and column names (based on the connected database schema).
- Query execution: `Ctrl+Enter` (`Cmd+Enter` on macOS).

Example of a YQL query:
```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```
The execution results are displayed in the **Results** panel as a table, JSON, or chart (switch between them using tabs).
### EXPLAIN and query execution plan {#explain}

Select **Explain YQL Query** in the editor context menu or in the command palette (`Ctrl+Shift+P`) to get the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md). The plugin displays the plan's operation tree in text form.
### Session Manager {#session-manager}

The **Sessions** panel (Activity Bar → YDB) displays all active sessions with the current query, state, and duration (data from the system view [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). The **Toggle Hide Idle** button hides sessions without an active query.
### Cluster Dashboard {#cluster-dashboard}

The **Database Load** panel (Activity Bar → YDB) displays real-time cluster load (updated every 10 seconds):

- CPU usage (percentage and number of cores).
- Memory usage (percentage and volume).
- Network traffic.
{% note warning %}

The dashboard is available only when working with self-hosted installations of {{ ydb-short-name }} that have access to [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md). In Yandex Cloud Managed Service for {{ ydb-short-name }}, Embedded UI is not published, so dashboard data is unavailable — use [cloud platform tools](https://yandex.cloud/en/docs/ydb/operations/monitoring) for monitoring.

{% endnote %}
### Streaming Queries {#streaming-queries}

In the navigator, expand the **Streaming Queries** section. For each query, the following is available:

- Viewing the original YQL (**View Source**).
- Starting and stopping (**Start** / **Stop**).
### SQL Dialect Converter {#convert-dialect}

The plugin allows you to convert an SQL query written in another dialect (PostgreSQL, MySQL, ClickHouse, and others) to YQL. The converter is available in the **Convert Dialect** panel on the Activity Bar.

To convert a query:

1. Select the source SQL dialect in the **Source Dialect** dropdown list.
1. Paste the source SQL code into the **Input SQL** field.
1. Click **Convert**. The result will appear in the lower field.
1. Copy the result for use in the YQL editor.

For more information about how the converter works, the supported dialects, and the limitations, see the article [SQL Dialect Converter to YQL](../sql-dialect-converter.md).
{% note warning %}

To convert, the plugin sends the original query to an external HTTPS service. The converter does not work without internet access. Do not use the converter for queries containing confidential data.

{% endnote %}
### Viewing Permissions {#permissions}

Right-click on an object in the navigator and select **View Permissions** to view the [access rights (ACL)](../../security/authorization.md#right) assigned to this object.
### DDL Generation {#ddl}

Right-click on a table, topic, or another object in the navigator and select **Create DDL** to get the `CREATE` script for the object.
### Creating objects {#create-objects}

Right-click on the corresponding folder in the navigator to create a new object:

- **New Row Table** / **New Column Table** — create a row or column table.
- **New Topic** — create a topic.
- **New View** — create a view.
- **New Object Storage Data Source** / **New YDB Data Source** — create an external data source.
- **New CSV/JSON/Parquet External Table** — create an external table.
- **New Transfer** — create a transfer.
- **New Streaming Query** — create a streaming query.
## Integration with AI assistants (MCP) {#mcp}

The plugin launches a built-in [MCP server](https://modelcontextprotocol.io/) (Model Context Protocol), which allows AI assistants (Claude Code and others) to directly execute queries to {{ ydb-short-name }} databases configured in the plugin.
### Configuring the port {#mcp-port}

The server starts on port **3333** (localhost only) by default. You can change the port in VS Code settings:
```json
{
  "ydb.mcpPort": 3333
}
```
If the port is already in use, the extension will display a warning and continue working without MCP.
### Connecting Claude Code {#mcp-claude}

1. Make sure that the YDB extension is running in VS Code and at least one connection is added in the **Connections** panel (see [Creating a connection](#connection)).

1. Register the MCP server in Claude Code globally for the current user:
    ```bash
    claude mcp add --scope user --transport sse ydb http://localhost:3333/sse
    ```
The `--scope user` flag saves the configuration globally — the server will be accessible from any directory where Claude Code is launched. Without this flag, the `local` scope is used (by default), and registration will have to be repeated for each project.

1. Check the connection:
    ```bash
    claude mcp list
    ```
### Available MCP tools {#mcp-tools}
| Tool | Parameters | Description |
|------------|-----------|----------|
| `ydb_list_connections` | — | List of all connections configured in the plugin |
| `ydb_query` | `connection`, `sql` | Execute a YQL query |
| `ydb_describe_table` | `connection`, `path` | Get the table schema (columns, primary key) |
| `ydb_list_directory` | `connection`, `path?` | List of objects in the database directory |
| `ydb_list_all` | `connection`, `path?`, `limit?`, `offset?` | Recursive list of all objects |
| `ydb_yql_help` | `query`, `connection?` | Search YQL documentation (requires [RAG](#rag) enabled) |
The `connection` parameter is the name of the connection as it appears in the **Connections** panel.
## YQL documentation search (RAG) {#rag}

The plugin supports semantic and keyword search of YQL documentation using the built-in RAG index (Retrieval-Augmented Generation). This allows AI assistants (via the `ydb_yql_help` tool) to find relevant fragments of the syntax reference when writing queries.
### Enabling RAG {#rag-enable}

1. When creating or editing a connection, set the **Use RAG** flag.
1. The plugin will automatically detect the {{ ydb-short-name }} version and load the corresponding index from the cloud on the first connection.
### Search modes {#rag-modes}

- **Keyword search** — works without additional dependencies, used by default.
- **Semantic search (vector)** — requires locally running [Ollama](https://ollama.com) with the embedding model [`nomic-embed-text`](https://ollama.com/library/nomic-embed-text). See [Installing Ollama and the embedding model](#install-ollama).

The RAG status (Running / Not running) and the Ollama status are displayed directly in the connection form.
### Installing Ollama and the embedding model {#install-ollama}

[Ollama](https://ollama.com) is a local server for running language models and embedding models. The plugin accesses it via HTTP to convert documentation texts and search queries into vectors for semantic search.

1. Install Ollama following the instructions on the [official download page](https://ollama.com/download) (macOS, Windows, and Linux are supported).

1. Make sure that the Ollama service is running and accessible at `http://localhost:11434`:
    ```bash
    curl http://localhost:11434/api/tags
    ```
A response in JSON format with a list of models means that the service is working.

1. Download the `nomic-embed-text` embedding model (~270 MB):
    ```bash
    ollama pull nomic-embed-text
    ```
1. Make sure the model is available:
    ```bash
    ollama list
    ```
The output should include the line `nomic-embed-text`.

1. Open the connection form in the plugin and make sure that the Ollama status is displayed as **Running**. If the status is **Not running**, check that the Ollama service is running and available at `http://localhost:11434`.
{% note info %}

The Ollama URL (`http://localhost:11434`) and model name (`nomic-embed-text`) are already set in the plugin by default — you only need to override the `ydb.ragOllamaUrl` and `ydb.ragOllamaModel` settings in case of a non-standard installation.

It is not possible to use another embedding model: the indices published by the plugin in the cloud are based on `nomic-embed-text`, and if you change the model, the vectors of the query and documentation will become incompatible.

{% endnote %}
## Plugin update {#updates}

The update method depends on how the plugin was installed.
### Updating from the VS Code Marketplace {#update-marketplace}

If the plugin is installed from the [Marketplace](#install-marketplace), VS Code updates it automatically.

To update the plugin manually:

1. Open the Extensions panel (`Ctrl+Shift+X` / `Cmd+Shift+X`).
1. Enter `YDB for VS Code` in the search bar and open the installed extension.
1. If a new version is available, the **Update** button will appear — click it.
### Updating from a VSIX file {#update-vsix}

If the plugin is installed from a [VSIX file](#install-vsix), automatic updates are not available — VS Code does not track updates for manually installed extensions.

1. Download a new `.vsix` file from the [GitHub Releases](https://github.com/ydb-platform/ydb-vscode-plugin/releases) page.
1. Install it over the old version using the same method as during the initial installation — VS Code will automatically replace the previous version.
