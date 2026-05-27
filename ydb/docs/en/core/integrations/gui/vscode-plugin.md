# Connecting to {{ ydb-short-name }} using the VS Code plugin

[Visual Studio Code](https://code.visualstudio.com) is a free, cross-platform, open-source code editor that supports a wide ecosystem of extensions for working with databases, language servers, and development tools.

[YDB for VS Code](https://github.com/ydb-platform/ydb-vscode-plugin) is a VS Code extension with native support for {{ ydb-short-name }}. The plugin provides a specialized interface for working with {{ ydb-short-name }} objects: a hierarchical navigator for tables, topics, views, external data sources, support for all authentication methods, a [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting, query execution plan visualization, session and cluster monitoring, access control (ACL) management, a built-in [MCP server](https://modelcontextprotocol.io/) for AI assistants, and other features.

## Key plugin features {#features}

- Connecting to {{ ydb-name }} with all [authentication methods](../../security/authentication.md): anonymous, static, token-based, service account, and metadata-based.
- Hierarchical navigator for objects: tables ([row-oriented](../../concepts/glossary.md#row-oriented-table) and [column-oriented](../../concepts/glossary.md#column-oriented-table)), [topics](../../concepts/datamodel/topic.md), [views](../../concepts/datamodel/view.md), [external data sources](../../concepts/glossary.md#external-data-source), [external tables](../../concepts/glossary.md#external-table), [transfers](../../concepts/transfer.md), [streaming queries](../../concepts/glossary.md#streaming-query).
- System objects: [system views](../../dev/system-views.md) (`.sys`), [resource pools](../../concepts/glossary.md#resource-pool).
- [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting and auto-completion for tables and columns.
- Query execution and result visualization: table, JSON, chart.
- Visualization of the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md) (`EXPLAIN`).
- Monitoring active sessions via [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Cluster dashboard based on [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md): CPU load, memory usage, network traffic (updated every 10 seconds).
- Access control (ACL) management: view permissions for database objects.[правами доступа (ACL)](../../security/authorization.md#right)
- Generation of DDL scripts ([`CREATE`](../../yql/reference/syntax/create_table/index.md)) for any database object.
- Streaming query management: view, start, stop.[потоковыми запросами](../../concepts/glossary.md#streaming-query)
- [SQL query converter](../sql-dialect-converter.md) from other dialects (PostgreSQL, MySQL, ClickHouse, etc.) to YQL.
- Built-in [MCP server](#mcp)
- [Semantic search for YQL documentation](#rag)

## Requirements {#requirements}

The plugin requires Visual Studio Code version 1.75.0 or later.

## Plugin installation {#installation}

The plugin can be installed from the VS Code Marketplace or from a `.vsix` file on the GitHub Releases page.

### Installing from the VS Code Marketplace {#install-marketplace}

1. Open the Extensions panel in VS Code (`Ctrl+Shift+X` / `Cmd+Shift+X`).
1. Enter `YDB for VS Code` in the search bar and select the extension from the `ydb-tech` publisher ([direct link](https://marketplace.visualstudio.com/items?itemName=ydb-tech.ydb-vscode-plugin)).
1. Click **Install**.

Alternatively, you can install the plugin from the Marketplace with a single command in the terminal:

```bash
code --install-extension ydb-tech.ydb-vscode-plugin
```

### Installing from a VSIX file {#install-vsix}

This method is suitable if you need a specific version or don't have access to the Marketplace.

1. Go to the [GitHub Releases page](https://github.com/ydb-platform/ydb-vscode-plugin/releases) and download the `ydb-vscode-plugin-*.vsix` file of the desired version.

1. Install the extension using one of the following methods:

   - **Via the terminal:**

        ```bash
        code --install-extension ydb-vscode-plugin-X.X.X.vsix
        ```

Where `X.X.X` is the version number you downloaded.

- **Via the VS Code interface:**

  1. Open the Extensions panel (`Ctrl+Shift+X` / `Cmd+Shift+X`).
  2. Click `...` (three dots) in the upper-right corner of the panel.
  3. Select **Install from VSIX...**.
  4. Specify the path to the downloaded `.vsix` file.

After installing using any method, restart VS Code. The **YDB** icon will appear in the Activity Bar.

## Creating a connection to {{ ydb-name }} {#connection}

1. Click the **YDB** icon in the Activity Bar on the left.
2. In the **Connections** panel, click the **Add Connection** button (the `+` icon).
3. The connection creation form will open. Fill in the fields:

| Field | Description | Example |
|------|----------|--------|
| **Connection Name** | Arbitrary connection name | `my-ydb` |
| **Host** | Host of the {{ ydb-name }} cluster endpoint | `ydb.example.com` |[эндпойнта](../../concepts/connect.md#endpoint)
| **Port** | Port (default is `2135`) | `2135` |
| **Database** | Path to the database | `/Root/database` |[базе данных](../../concepts/glossary.md#database)
| **Monitoring URL** | URL of the {{ ydb-short-name }} Embedded UI, used for the dashboard (filled in automatically based on the host, can be overridden) | `http://ydb.example.com:8765` |
| **Secure connection (grpcs)** | Use a secure connection (`grpcs://`) | ☑ |
| **Use RAG** | Enable YQL documentation search for this connection | ☑ |[поиск по документации YQL](#rag)

1. If necessary, specify the path to a custom CA certificate (PEM) in the **CA Certificate File** field for connections with non-standard TLS. If the field is left empty, the built-in Yandex Cloud certificate is used.

2. Select an authentication method from the **Auth type** dropdown list (see [Authentication methods](#auth-methods)).

3. Click **Test Connection** to check the settings. If the connection is successful, a success message will appear.

4. Click **Save**. The connection will appear in the **Connections** panel.

## Authentication methods {#auth-methods}

The plugin supports all authentication methods available in {{ ydb-short-name }}. The method is selected from the **Auth type** dropdown list on the connection creation form.[аутентификации](../../security/authentication.md)

### Anonymous {#auth-anonymous}

Connection without credentials. Used for local or test installations of {{ ydb-short-name }}. No additional fields need to be filled out.

### Static Credentials (username and password) {#auth-static}

Authentication using username and password. Enter the username in the **Username** field and the password in the **Password** field. This is used if the {{ ydb-short-name }} server has username/password authentication enabled.[аутентификация по логину и паролю](../../security/authentication.md#static-credentials)

{% note info %}
In managed installations of {{ ydb-name }}, username/password authentication is disabled: managed services use the centralized access management system of the cloud platform ([IAM](https://yandex.cloud/en/docs/iam/)).
{% endnote %}[IAM](https://yandex.cloud/en/docs/iam/)

### Access Token {#auth-token}

Authentication using an [IAM token](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token) or [OAuth token](https://yandex.cloud/en/docs/iam/concepts/authorization/oauth-token). Enter the token in the **Token** field. The token is passed in the header of each request.

{% note warning %}[IAM-](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token)[OAuth-токену](https://yandex.cloud/en/docs/iam/concepts/authorization/oauth-token)
An IAM token has a limited lifetime — no more than 12 hours ([see documentation](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime)) — after which it must be obtained again and updated in the connection settings. For long-lived connections, use authentication via [service account](#auth-service-account) or [metadata service](#auth-metadata).
{% endnote %}

### Service Account Key File {#auth-service-account}[срок жизни — не более 12 часов](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime)[сервисному аккаунту](#auth-service-account)[сервису метаданных](#auth-metadata) {#auth-service-account}

Authentication using a Yandex Cloud service account key. Specify the path to the JSON file with the key in the **Service Account Key File** field (use the **Browse** button to select the file). For instructions on how to create an authorized key, see the [Yandex Cloud documentation](https://yandex.cloud/en/docs/iam/operations/authentication/manage-authorized-keys).

The key file format:




```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```

### Metadata Service {#auth-metadata}

Authentication via the [Yandex Cloud metadata service](https://yandex.cloud/en/docs/compute/operations/vm-metadata/get-vm-metadata). The plugin receives an IAM token from the virtual machine's metadata service. This is used only when VS Code is running on a Yandex Cloud virtual machine.

## Object Navigator {#object-navigator}

After connecting, click on the connection in the **Connections** panel — the **Navigator** panel will open with the hierarchy of {{ ydb-short-name }} objects. The navigator includes the following sections:

- **Tables** — tables (row and column-based), organized by subdirectories according to the path in {{ ydb-short-name }}.
- **System Views** — [system views](../../dev/system-views.md) (`.sys`), such as `query_sessions`, `partition_stats`.
- **Views** — [views](../../dev/system-views.md).
- **Topics** — [topics](../../concepts/datamodel/view.md).
- **External Data Sources** — [external data sources](../../concepts/datamodel/topic.md).
- **External Tables** — [external tables](../../concepts/glossary.md#external-data-source).
- **Resource Pools** — [resource pools](../../concepts/glossary.md#external-table).
- **Transfers** — data transfers.[пулы ресурсов](../../concepts/glossary.md#resource-pool)
- **Streaming Queries** — [streaming queries](../../concepts/glossary.md#streaming-query).

Right-clicking on any object in the navigator opens a context menu with available actions.

## Working with the Plugin {#capabilities}

### Query Workspace {#query-workspace}

Open the query workspace using `Ctrl+Shift+Q` (`Cmd+Shift+Q` on macOS) or click **Open Query Workspace** in the connection's context menu. In the workspace, you can write and execute YQL queries, view history and results.

To quickly open the editor with a pre-filled query, right-click on a table or view in the navigator and select:

- **Show Preview** — `SELECT` the first 100 rows.
- **Make Query** — `SELECT` with the object name.

### YQL Editor {#yql-editor}

The editor supports:

- Syntax highlighting for [YQL](../../yql/reference/index.md): keywords, data types, built-in functions.
- Autocompletion of table and column names (based on the connected database schema).[YQL](../../yql/reference/index.md)
- Query execution: `Ctrl+Enter` (`Cmd+Enter` on macOS).

Example of a YQL query:

```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```

The execution results are displayed in the **Results** panel as a table, JSON, or chart (switch between tabs).

### EXPLAIN and execution plan {#explain}

Select **Explain YQL Query** in the editor's context menu or in the command palette (`Ctrl+Shift+P`) to get the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md). The plugin displays the operation tree of the plan in text format.

### Session manager {#session-manager}

The **Sessions** panel (Activity Bar → YDB) displays all active sessions with the current query, state, and duration (data from the system view [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). The **Toggle Hide Idle** button hides sessions without an active query.

### Cluster dashboard {#cluster-dashboard}

The **Database Load** panel (Activity Bar → YDB) displays real-time cluster load (updated every 10 seconds):

- CPU usage (percentage and number of cores).
- Memory usage (percentage and volume).
- Network traffic.

{% note warning %}

The dashboard is only available when working with self-hosted {{ ydb-short-name }} installations that have access to [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md). In Yandex Cloud Managed Service for {{ ydb-short-name }}, Embedded UI is not published, so dashboard data is unavailable — use [cloud platform tools](https://yandex.cloud/en/docs/ydb/operations/monitoring) for monitoring.

{% endnote %}

### Streaming queries {#streaming-queries}

In the navigator, expand the **Streaming Queries** section. For each query, you can:

- View the original YQL (**View Source**).
- Start and stop (**Start** / **Stop**).

### SQL dialect converter {#convert-dialect}

The plugin allows you to convert an SQL query written in another dialect (PostgreSQL, MySQL, ClickHouse, etc.) to YQL. The converter is available in the **Convert Dialect** panel on the Activity Bar.

To convert a query:

1. Select the source SQL dialect in the **Source Dialect** dropdown list.
2. Paste the source SQL code into the **Input SQL** field.
3. Click **Convert**. The result will appear in the lower field.
4. Copy the result for use in the YQL editor.

For more information about how the converter works, supported dialects, and limitations, see the [SQL Dialect Converter to YQL](../sql-dialect-converter.md) article.

{% note warning %}

To perform the conversion, the plugin sends the original query to an external HTTPS service. The converter does not work without internet access. Do not use the converter for queries containing confidential data.

{% endnote %}

### Viewing permissions {#permissions}

Right-click on an object in the navigator and select **View Permissions** to view the [access rights (ACL)](../../security/authorization.md#right) assigned to that object.

### DDL generation {#ddl}

Right-click on a table, topic, or other object in the navigator and select **Create DDL** to get the `CREATE` script for the object.

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

### Port configuration {#mcp-port}

The server runs on port **3333** (localhost only) by default. You can change the port in VS Code settings:

```json
{
  "ydb.mcpPort": 3333
}
```

If the port is already in use, the extension will display a warning and continue working without MCP.

### Connecting Claude Code {#mcp-claude}

1. Make sure the YDB extension is running in VS Code and at least one connection is added in the **Connections** panel (see [Creating a connection](#connection)).

1. Register the MCP server in Claude Code globally for the current user:

    ```bash
    claude mcp add --scope user --transport sse ydb http://localhost:3333/sse
    ```

The `--scope user` flag saves the configuration globally — the server will be accessible from any directory where Claude Code is launched. Without this flag, the `local` scope (default) is used, and registration will have to be repeated for each project.

1. Check the connection:

    ```bash
    claude mcp list
    ```

### Available MCP tools {#mcp-tools}

| Tool | Parameters | Description |
|------------|-----------|----------|
| `ydb_list_connections` | — | List all connections configured in the plugin |
| `ydb_query` | `connection`, `sql` | Execute a YQL query |
| `ydb_describe_table` | `connection`, `path` | Get the table schema (columns, primary key) |
| `ydb_list_directory` | `connection`, `path?` | List objects in the database directory |
| `ydb_list_all` | `connection`, `path?`, `limit?`, `offset?` | Recursive list of all objects |
| `ydb_yql_help` | `query`, `connection?` | Search YQL documentation (requires RAG enabled) |

The `connection` parameter is the name of the connection as it appears in the **Connections** panel.

## YQL documentation search (RAG) {#rag}

The plugin supports semantic and keyword search of YQL documentation using the built-in RAG index (Retrieval-Augmented Generation). This allows AI assistants (via the `ydb_yql_help` tool) to find relevant fragments of the syntax reference when writing queries.

### Enabling RAG {#rag-enable}

1. When creating or editing a connection, set the **Use RAG** flag.
2. The plugin will automatically detect the {{ ydb-short-name }} version and download the appropriate index from the cloud on the first connection.

### Search modes {#rag-modes}

- **Keyword search** — works without additional dependencies and is used by default.
- **Semantic search (vector)** — requires [Ollama](https://ollama.com) (https://ollama.com) running locally with the [`nomic-embed-text`](https://ollama.com/library/nomic-embed-text) embedding model. See [Installing Ollama and the embedding model](#install-ollama).

The RAG status (Running / Not running) and Ollama status are displayed directly in the connection form.

### Installing Ollama and the embedding model {#install-ollama}

[Ollama](https://ollama.com) is a local server for running language models and embedding models. The plugin accesses it via HTTP to convert documentation texts and search queries into vectors for semantic search.

1. Install Ollama following the instructions on the [official download page](https://ollama.com/download) (macOS, Windows, and Linux are supported).

2. Make sure the Ollama service is running and accessible at `http://localhost:11434`:

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

The output should include the `nomic-embed-text` string.

1. Open the connection form in the plugin and make sure the Ollama status is displayed as **Running**. If the status is **Not running**, check that the Ollama service is running and available at `http://localhost:11434`.

{% note info %}

The Ollama URL (`http://localhost:11434`) and model name (`nomic-embed-text`) are already set in the plugin by default — you only need to override the `ydb.ragOllamaUrl` and `ydb.ragOllamaModel` settings in case of a non-standard installation.

You cannot use another embedding model: the indices published by the plugin in the cloud are built specifically on `nomic-embed-text`, and if you change the model, the vectors of the query and documentation will become incompatible.

{% endnote %}

## Plugin update {#updates}

The update method depends on how the plugin was installed.

### Updating from the VS Code Marketplace {#update-marketplace}

If the plugin is installed from the [Marketplace](#install-marketplace), VS Code updates it automatically.

To update the plugin manually:

1. Open the Extensions panel (`Ctrl+Shift+X` / `Cmd+Shift+X`).
2. Enter `YDB for VS Code` in the search bar and open the installed extension.
3. If a new version is available, the **Update** button will appear — click it.

### Updating from a VSIX file {#update-vsix}

If the plugin is installed from a [VSIX file](#install-vsix), automatic updates are not available — VS Code does not track updates for manually installed extensions.

1. Download a new `.vsix` file from the [GitHub Releases page](https://github.com/ydb-platform/ydb-vscode-plugin/releases).
2. Install it over the old version using the same method as during the first installation — VS Code will automatically replace the previous version.[GitHub Releases](https://github.com/ydb-platform/ydb-vscode-plugin/releases)
