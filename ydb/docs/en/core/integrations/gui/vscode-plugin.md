# Connecting to {{ ydb-short-name }} using the VS Code plugin

[Visual Studio Code](https://code.visualstudio.com) is a free cross-platform open-source code editor that supports a wide ecosystem of extensions for working with databases, language servers, and development tools.

[YDB for VS Code](https://github.com/ydb-platform/ydb-vscode-plugin) is a VS Code extension with native support for {{ ydb-short-name }}. The plugin provides a specialized interface for working with {{ ydb-short-name }} objects: a hierarchical navigator for tables, topics, views, external data sources, support for all authentication methods, a [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting, query execution plan visualization, session and cluster monitoring, access control (ACL) management, a built-in [MCP server](https://modelcontextprotocol.io/) for AI assistants, and other features.

## Key plugin features {#features}

- Connecting to {{ ydb-name }} with all [authentication methods](../../security/authentication.md): anonymous, static, token-based, service account, metadata-based.
- Hierarchical navigator for objects: tables ([row-oriented](../../concepts/glossary.md#row-oriented-table) and [column-oriented](../../concepts/glossary.md#column-oriented-table)), [topics](../../concepts/datamodel/topic.md), [views](../../concepts/datamodel/view.md), [external data sources](../../concepts/glossary.md#external-data-source), [external tables](../../concepts/glossary.md#external-table), [transfers](../../concepts/transfer.md), [streaming queries](../../concepts/glossary.md#streaming-query).
- System objects: [system views](../../dev/system-views.md) (`.sys`), [resource pools](../../concepts/glossary.md#resource-pool).
- [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting and auto-completion for tables and columns.
- Query execution and result visualization: table, JSON, chart.
- Visualization of the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md) (`EXPLAIN`).
- Monitoring active sessions via [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Cluster dashboard based on [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md): CPU load, memory usage, network traffic (updated every 10 seconds).
- Access control (ACL) management: viewing permissions for database objects.
- Generation of DDL scripts ([`CREATE`](../../yql/reference/syntax/create_table/index.md)) for any database object.
- Management of [streaming queries](../../concepts/glossary.md#streaming-query): viewing, starting, stopping.
- [SQL query converter](../sql-dialect-converter.md) from other dialects (PostgreSQL, MySQL, ClickHouse, etc.) to YQL.
- Built-in [MCP server](#mcp) — direct access to databases from AI assistants (Claude Code and others).
- [Semantic search for YQL documentation](#rag) (RAG) for AI-assisted query writing.

## Requirements {#requirements}

The plugin requires Visual Studio Code version 1.75.0 or later.

## Plugin installation {#installation}

The plugin can be installed from the VS Code Marketplace or from a `.vsix` file on the GitHub Releases page.

### Installation from the VS Code Marketplace {#install-marketplace}

1. Open the Extensions panel in VS Code (`Ctrl+Shift+X` / `Cmd+Shift+X`).
1. Enter `YDB for VS Code` in the search bar and select the extension from the `ydb-tech` publisher ([direct link](https://marketplace.visualstudio.com/items?itemName=ydb-tech.ydb-vscode-plugin)).
1. Click **Install**.

Alternatively, you can install the plugin from the Marketplace with a single command in the terminal:

```bash
code --install-extension ydb-tech.ydb-vscode-plugin
```

### Installation from a VSIX file {#install-vsix}

This method is suitable if you need a specific version or do not have access to the Marketplace.

1. Go to the [GitHub Releases page](https://github.com/ydb-platform/ydb-vscode-plugin/releases) and download the `ydb-vscode-plugin-*.vsix` file of the desired version.

1. Install the extension using one of the following methods:

    - **Via the terminal:**

        ```bash
        code --install-extension ydb-vscode-plugin-X.X.X.vsix
        ```

        Where `X.X.X` is the number of the downloaded version.

    - **Via the VS Code interface:**

        1. Open the Extensions panel (`Ctrl+Shift+X` / `Cmd+Shift+X`).
        1. Click `...` (three dots) in the upper right corner of the panel.
        1. Select **Install from VSIX...**.
        1. Specify the path to the downloaded `.vsix` file.

After installation using any method, restart VS Code. The **YDB** icon will appear in the Activity Bar.
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
    | **Monitoring URL** | URL of the [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md), used for the dashboard (filled in automatically based on the host, can be overridden) | `http://ydb.example.com:8765` |
    | **Secure connection (grpcs)** | Use a secure connection (`grpcs://`) | ☑ |
    | **Use RAG** | Enable [YQL documentation search](#rag) for this connection | ☑ |

1. If necessary, specify the path to a custom CA certificate (PEM) in the **CA Certificate File** field — for connections with non-standard TLS. If the field is not filled in, the built-in Yandex Cloud certificate is used.

1. Select the authentication method from the **Auth type** dropdown list (see [Authentication methods](#auth-methods)).

1. Click **Test Connection** to check the settings. If the connection is successful, a success message will appear.

1. Click **Save**. The connection will appear in the **Connections** panel.

## Authentication methods {#auth-methods}

The plugin supports all [authentication methods](../../security/authentication.md) available in {{ ydb-short-name }}. The method is selected from the **Auth type** dropdown list in the connection creation form.

### Anonymous {#auth-anonymous}

Connection without credentials. Used for local or test installations of {{ ydb-short-name }}. No additional fields need to be filled in.

### Static Credentials (username and password) {#auth-static}

Authentication using username and password. Enter the username in the **Username** field and the password in the **Password** field. Used if the {{ ydb-short-name }} server has [username and password authentication](../../security/authentication.md#static-credentials) enabled.

{% note info %}

In managed installations of {{ ydb-name }}, username and password authentication is disabled: managed services use the centralized access control system of the cloud platform ([IAM](https://yandex.cloud/en/docs/iam/)).

{% endnote %}

### Access Token {#auth-token}

Authentication using an [IAM token](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token) or [OAuth token](https://yandex.cloud/en/docs/iam/concepts/authorization/oauth-token). Enter the token in the **Token** field. The token is passed in the header of each request.

{% note warning %}

An IAM token has a limited [lifetime — no more than 12 hours](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime), after which it must be obtained again and updated in the connection settings. For long-lived connections, use authentication via [service account](#auth-service-account) or [metadata service](#auth-metadata).

{% endnote %}

### Service Account Key File {#auth-service-account}

Authentication using the key of a [Yandex Cloud service account](https://yandex.cloud/en/docs/iam/concepts/users/service-accounts). Specify the path to the JSON file with the key in the **Service Account Key File** field (use the **Browse** button to select the file). For more information on how to create an authorized key, see the [Yandex Cloud documentation](https://yandex.cloud/en/docs/iam/operations/authentication/manage-authorized-keys).

Key file format:

```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```

### Metadata Service {#auth-metadata}

Authentication via the [Yandex Cloud metadata service](https://yandex.cloud/en/docs/compute/operations/vm-metadata/get-vm-metadata). The plugin receives an IAM token from the virtual machine's metadata service. Used only when VS Code is running on a Yandex Cloud virtual machine.
## Object Navigator {#object-navigator}

After connecting, click on the connection in the **Connections** panel — the **Navigator** panel will open with the hierarchy of {{ ydb-short-name }} objects. The navigator contains the following sections:

- **Tables** — tables (row and columnar), organized by subdirectories according to the path in {{ ydb-short-name }}.
- **System Views** — [system views](../../dev/system-views.md) (`.sys`), such as `query_sessions`, `partition_stats`.
- **Views** — [views](../../concepts/datamodel/view.md).
- **Topics** — [topics](../../concepts/datamodel/topic.md).
- **External Data Sources** — [external data sources](../../concepts/glossary.md#external-data-source).
- **External Tables** — [external tables](../../concepts/glossary.md#external-table).
- **Resource Pools** — [resource pools](../../concepts/glossary.md#resource-pool).
- **Transfers** — data transfers.
- **Streaming Queries** — [streaming queries](../../concepts/glossary.md#streaming-query).

Right-clicking on any object in the navigator opens a context menu with available actions.

## Working with the Plugin {#capabilities}

### Query Workspace {#query-workspace}

Open the query workspace via `Ctrl+Shift+Q` (`Cmd+Shift+Q` on macOS) or click **Open Query Workspace** in the connection context menu. In the workspace, you can write and execute YQL queries, view history and results.

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

Query results are displayed in the **Results** panel as a table, JSON, or chart (switchable via tabs).

### EXPLAIN and Execution Plan {#explain}

Select **Explain YQL Query** in the editor context menu or in the command palette (`Ctrl+Shift+P`) to get the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md). The plugin displays the operation tree of the plan in text form.

### Session Manager {#session-manager}

The **Sessions** panel (Activity Bar → YDB) displays all active sessions with the current query, state, and duration (data from the system view [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). The **Toggle Hide Idle** button hides sessions without an active query.

### Cluster Dashboard {#cluster-dashboard}

The **Database Load** panel (Activity Bar → YDB) displays cluster load in real time (updated every 10 seconds):

- CPU load (percentage and number of cores).
- Memory usage (percentage and volume).
- Network traffic.

{% note warning %}

The dashboard is only available when working with self-hosted installations of {{ ydb-short-name }} that have access to [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md). In Yandex Cloud Managed Service for {{ ydb-short-name }}, Embedded UI is not published, so dashboard data is unavailable — use [cloud platform tools](https://yandex.cloud/en/docs/ydb/operations/monitoring) for monitoring.

{% endnote %}

### Streaming Queries {#streaming-queries}

In the navigator, expand the **Streaming Queries** section. For each query, you can:

- View the source YQL (**View Source**).
- Start and stop (**Start** / **Stop**).

### SQL Dialect Converter {#convert-dialect}

The plugin allows you to convert an SQL query written in another dialect (PostgreSQL, MySQL, ClickHouse, etc.) to YQL. The converter is available in the **Convert Dialect** panel on the Activity Bar.

To convert a query:

1. Select the source SQL dialect in the **Source Dialect** dropdown list.
1. Paste the source SQL code into the **Input SQL** field.
1. Click **Convert**. The result will appear in the lower field.
1. Copy the result for use in the YQL editor.

For more information about the converter's operation principles, supported dialects, and limitations, see the article [SQL Dialect Converter to YQL](../sql-dialect-converter.md).

{% note warning %}

To perform the conversion, the plugin sends the source query to an external HTTPS service. The converter does not work without internet access. Do not use the converter for queries containing confidential data.

{% endnote %}

### Viewing Permissions {#permissions}

Right-click on an object in the navigator and select **View Permissions** to view the [access rights (ACL)](../../security/authorization.md#right) assigned to that object.

### Generating DDL {#ddl}

Right-click on a table, topic, or other object in the navigator and select **Create DDL** to get the `CREATE` script for the object.

### Creating Objects {#create-objects}

Right-click on the corresponding folder in the navigator to create a new object:

- **New Row Table** / **New Column Table** — create a row or columnar table.
- **New Topic** — create a topic.
- **New View** — create a view.
- **New Object Storage Data Source** / **New YDB Data Source** — create an external data source.
- **New CSV/JSON/Parquet External Table** — create an external table.
- **New Transfer** — create a transfer.
- **New Streaming Query** — create a streaming query.
## Integration with AI Assistants (MCP) {#mcp}

The plugin launches a built-in [MCP server](https://modelcontextprotocol.io/) (Model Context Protocol), which allows AI assistants (Claude Code and others) to directly execute queries to {{ ydb-short-name }} databases configured in the plugin.

### Configuring the Port {#mcp-port}

The server starts on port **3333** (localhost only) by default. You can change the port in VS Code settings:

```json
{
  "ydb.mcpPort": 3333
}
```

If the port is already in use, the extension will show a warning and continue working without MCP.

### Connecting Claude Code {#mcp-claude}

1. Make sure the YDB extension is running in VS Code and at least one connection is added in the **Connections** panel (see [Creating a Connection](#connection)).

1. Register the MCP server in Claude Code globally for the current user:

    ```bash
    claude mcp add --scope user --transport sse ydb http://localhost:3333/sse
    ```

    The `--scope user` flag saves the configuration globally — the server will be available from any directory where Claude Code is launched. Without this flag, the [local scope](https://docs.claude.com/en/docs/claude-code/mcp#mcp-installation-scopes) (default) is used, and registration will have to be repeated for each project.

1. Check the connection:

    ```bash
    claude mcp list
    ```

### Available MCP Tools {#mcp-tools}

| Tool | Parameters | Description |
|------------|-----------|----------|
| `ydb_list_connections` | — | List all connections configured in the plugin |
| `ydb_query` | `connection`, `sql` | Execute a YQL query |
| `ydb_describe_table` | `connection`, `path` | Get the table schema (columns, primary key) |
| `ydb_list_directory` | `connection`, `path?` | List objects in the database directory |
| `ydb_list_all` | `connection`, `path?`, `limit?`, `offset?` | Recursive list of all objects |
| `ydb_yql_help` | `query`, `connection?` | Search YQL documentation (requires RAG enabled, see [#rag](#rag)) |

The `connection` parameter is the connection name as it appears in the **Connections** panel.

## YQL Documentation Search (RAG) {#rag}

The plugin supports semantic and keyword search of YQL documentation using the built-in RAG index (Retrieval-Augmented Generation). This allows AI assistants (via the `ydb_yql_help` tool) to find relevant fragments of the syntax reference when writing queries.

### Enabling RAG {#rag-enable}

1. When creating or editing a connection, set the **Use RAG** flag.
1. The plugin will automatically detect the {{ ydb-short-name }} version and load the corresponding index from the cloud on the first connection.

### Search Modes {#rag-modes}

- **Keyword search** — works without additional dependencies, used by default.
- **Semantic search** — requires locally running [Ollama](https://ollama.com) with the [`nomic-embed-text`](https://ollama.com/library/nomic-embed-text) embedding model. See [Installing Ollama and the Embedding Model](#install-ollama).

The RAG status (Running / Not running) and Ollama status are displayed directly in the connection form.

### Installing Ollama and the Embedding Model {#install-ollama}

[Ollama](https://ollama.com) is a local server for running language models and embedding models. The plugin accesses it via HTTP to convert documentation texts and search queries into vectors for semantic search.

1. Install Ollama following the instructions on the [official download page](https://ollama.com/download) (macOS, Windows, and Linux are supported).

1. Make sure the Ollama service is running and accessible at `http://localhost:11434`:

    ```bash
    curl http://localhost:11434/api/tags
    ```

    A JSON response with a list of models indicates that the service is working.

1. Download the `nomic-embed-text` embedding model (~270 MB):

    ```bash
    ollama pull nomic-embed-text
    ```

1. Verify that the model is available:

    ```bash
    ollama list
    ```

    The output should include the `nomic-embed-text` line.

1. Open the connection form in the plugin and make sure the Ollama status is **Running**. If the status is **Not running**, check that the Ollama service is running and accessible at `http://localhost:11434`.

{% note info %}

The Ollama URL (`http://localhost:11434`) and model name (`nomic-embed-text`) are already set in the plugin by default — you only need to override the `ydb.ragOllamaUrl` and `ydb.ragOllamaModel` settings in case of a non-standard installation.

You cannot use another embedding model: the indexes published by the plugin in the cloud are built specifically on `nomic-embed-text`, and changing the model will make the request and documentation vectors incompatible.

{% endnote %}
## Plugin Update {#updates}

The update method depends on how the plugin was installed.

### Updating from the VS Code Marketplace {#update-marketplace}

If the plugin is installed from the [Marketplace](#install-marketplace), VS Code updates it automatically.

To update the plugin manually:

1. Open the Extensions panel (`Ctrl+Shift+X` / `Cmd+Shift+X`).
1. Enter `YDB for VS Code` in the search bar and open the installed extension.
1. If a new version is available, an **Update** button will appear — click it.

### Updating from a VSIX File {#update-vsix}

If the plugin is installed from a [VSIX file](#install-vsix), automatic updates are not available — VS Code does not track updates for manually installed extensions.

1. Download a new `.vsix` file from the [GitHub Releases](https://github.com/ydb-platform/ydb-vscode-plugin/releases) page.
1. Install it over the old version using the same method as during the first installation — VS Code will automatically replace the previous version.
