# Connecting to {{ ydb-short-name }} using the VS Code plugin

[Visual Studio Code](https://code.visualstudio.com) is a free cross-platform code editor with open-source code, supporting a wide ecosystem of extensions for working with databases, language servers, and development tools.

[YDB for VS Code](https://github.com/ydb-platform/ydb-vscode-plugin) is a VS Code extension with native support for {{ ydb-short-name }}. The plugin provides a specialized interface for working with {{ ydb-short-name }} objects: a hierarchical navigator for tables, topics, views, external data sources, support for all authentication methods, a [YQL][(][(](../../concepts/glossary.md#column-oriented-table)#row-oriented-table)#yql) editor with syntax highlighting, visualization of execution plans, monitoring of sessions and the cluster, access control management (ACL), a built-in [MCP server](https://modelcontextprotocol.io/) for AI assistants, and other features.

## Key plugin features {#features}

- Connecting to {{ ydb-name }} with all [authentication methods](../../security/authentication.md): anonymous, static, token-based, service account, metadata-based.
- Hierarchical navigator for objects: tables (row-oriented and column-oriented), [topics](../../concepts/datamodel/topic.md), [views](../../concepts/datamodel/view.md), [external data sources](../../concepts/glossary.md#external-data-source), [external tables](../../concepts/glossary.md#external-table), [transfers](../../concepts/transfer.md), [streaming queries](../../concepts/glossary.md#streaming-query).
- System objects: [system views](../../dev/system-views.md) (.sys), [resource pools](../../concepts/glossary.md#resource-pool).
- [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting and table and column autocompletion.
- Query execution and result visualization: table, JSON, chart.
- Visualization of the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md) (EXPLAIN).
- Monitoring active sessions via [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Cluster dashboard based on [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md): CPU load, memory usage, network traffic (updated every 10 seconds).
- Access control (ACL) management: view permissions for database objects.
- Generation of DDL scripts ([`CREATE`](../../yql/reference/syntax/create_table/index.md)) for any database object.
- Streaming query management: view, start, stop.
- [SQL dialect converter](../sql-dialect-converter.md) from other dialects (PostgreSQL, MySQL, ClickHouse, etc.) to YQL.
- Built-in [MCP server](#mcp) for direct database access from AI assistants (Claude Code and others).
- [Semantic search for YQL documentation](#rag) (RAG) for AI-assisted query writing.

## Requirements {#requirements}

The plugin requires Visual Studio Code version 1.75.0 or later.

## Installing the plugin {#installation}

You can install the plugin from the VS Code Marketplace or from a .vsix file on the GitHub Releases page.

### Installing from the VS Code Marketplace {#install-marketplace}

1. Open the Extensions panel in VS Code (Ctrl+Shift+X / Cmd+Shift+X).
2. In the search bar, enter `YDB for VS Code` and select the extension from the `ydb-tech` publisher ([direct link](https://marketplace.visualstudio.com/items?itemName=ydb-tech.ydb-vscode-plugin)).
3. Click **Install**.

Alternatively, you can install the plugin from the Marketplace with a single command in the terminal:

```bash
code --install-extension ydb-tech.ydb-vscode-plugin
```

### Installing from a VSIX file {#install-vsix}

This method is suitable if you need a specific version or don't have access to the Marketplace.

1. Go to the [GitHub Releases page](https://github.com/ydb-platform/ydb-vscode-plugin/releases) and download the `ydb-vscode-plugin-*.vsix` file of the desired version.

2. Install the extension in one of the following ways:

    - **Via the terminal:**

        ```bash
        code --install-extension ydb-vscode-plugin-X.X.X.vsix
        ```

        Where `X.X.X` is the number of the downloaded version.

    - **Via the VS Code interface:**

        1. Open the Extensions panel (Ctrl+Shift+X / Cmd+Shift+X).
        2. Click the `...` (three dots) in the upper-right corner of the panel.
        3. Select **Install from VSIX...**.
        4. Specify the path to the downloaded .vsix file.

After installation, restart VS Code. The **YDB** icon will appear in the Activity Bar.

## Creating a connection to {{ ydb-name }} {#connection}

1. Click the **YDB** icon in the Activity Bar on the left.
2. In the **Connections** panel, click the **Add Connection** button (the `+` icon).
3. The connection creation form will open. Fill in the fields:

    | Field | Description | Example |
    |------|----------|--------|
    | **Connection Name** | Arbitrary connection name | `my-ydb` |
    | **Host** | Host of the [endpoint](../../concepts/connect.md#endpoint) of the {{ ydb-name }} cluster | `ydb.example.com` |
    | **Port** | Port (default `2135`) | `2135` |
    | **Database** | Path to the [database](../../concepts/glossary.md#database) | `/Root/database` |
    | **Monitoring URL** | URL of [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md), used for the dashboard (filled in automatically based on the host, can be overridden) | `http://ydb.example.com:8765` |
    | **Secure connection (grpcs)** | Use a secure connection (`grpcs://`) | ☑ |
    | **Use RAG** | Enable [YQL documentation search](#rag) for this connection | ☑ |

4. If necessary, specify the path to a custom CA certificate (PEM) in the **CA Certificate File** field for connections with non-standard TLS. If the field is not filled in, the Yandex Cloud built-in certificate is used.

5. Select the authentication method from the **Auth type** dropdown list (see [Authentication methods](#auth-methods)).

6. Click **Test Connection** to check the settings. If the connection is successful, a success message will appear.

7. Click **Save**. The connection will appear in the **Connections** panel.

## Authentication methods {#auth-methods}

The plugin supports all [authentication methods](../../security/authentication.md) available in {{ ydb-short-name }}. The method is selected from the **Auth type** dropdown list on the connection creation form.

### Anonymous {#auth-anonymous}

Connection without credentials. Used for local or test installations of {{ ydb-short-name }}. No additional fields need to be filled in.

### Static Credentials (username and password) {#auth-static}

Authentication using a username and password. Enter the username in the **Username** field and the password in the **Password** field. Used if {{ ydb-short-name }} server has [username and password authentication](../../security/authentication.md#static-credentials) enabled.

{% note info %}

In managed installations of {{ ydb-name }}, username and password authentication is disabled: managed services use the centralized access management system of the cloud platform ([IAM](https://yandex.cloud/en/docs/iam/)).

{% endnote %}

### Access Token {#auth-token}

Authentication using an [IAM](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token) or [OAuth token](https://yandex.cloud/en/docs/iam/concepts/authorization/oauth-token). Enter the token in the **Token** field. The token is passed in the header of each request.

{% note warning %}

An IAM token has a limited [lifetime — no more than 12 hours](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime), after which it must be obtained again and updated in the connection settings. For long-lived connections, use authentication via [service account](#auth-service-account) or [metadata service](#auth-metadata).

{% endnote %}

### Service Account Key File {#auth-service-account}

Authentication using a Yandex Cloud service account key. Specify the path to the JSON file with the key in the **Service Account Key File** field (use the **Browse** button to select the file). For more information on how to create an authorized key, see the [Yandex Cloud documentation](https://yandex.cloud/en/docs/iam/operations/authentication/manage-authorized-keys).

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

## Object navigator {#object-navigator}

After connecting, click on the connection in the **Connections** panel — the **Navigator** panel will open with the hierarchy of {{ ydb-short-name }} objects. The navigator includes the following sections:

- **Tables** — tables (row-oriented and column-oriented), organized by subdirectories according to the path in {{ ydb-short-name }}.
- **System Views** — [system views](../../dev/system-views.md) (.sys), such as `query_sessions`, `partition_stats`.
- **Views** — [views](../../concepts/datamodel/view.md).
- **Topics** — [topics](../../concepts/datamodel/topic.md).
- **External Data Sources** — [external data sources](../../concepts/glossary.md#external-data-source).
- **External Tables** — [external tables](../../concepts/glossary.md#external-table).
- **Resource Pools** — [resource pools](../../concepts/glossary.md#resource-pool).
- **Transfers** — data transfers.
- **Streaming Queries** — [streaming queries](../../concepts/glossary.md#streaming-query).

Right-clicking on any object in the navigator opens a context menu with available actions.

## Working with the plugin {#capabilities}

### Query workspace {#query-workspace}

Open the query workspace via `Ctrl+Shift+Q` (`Cmd+Shift+Q` on macOS) or click **Open Query Workspace** in the connection context menu. In the workspace, you can write and execute YQL queries, view history and results.

To quickly open the editor with a pre-filled query, right-click on a table or view in the navigator and select:

- **Show Preview** — `SELECT` of the first 100 rows.
- **Make Query** — `SELECT` with the object name.

### YQL editor {#yql-editor}

The editor supports:

- [YQL](../../yql/reference/index.md) syntax highlighting: keywords, data types, built-in functions.
- Autocompletion of table and column names (based on the connected database schema).
- Query execution: `Ctrl+Enter` (`Cmd+Enter` on macOS).

Example YQL query:

```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```

Query results are displayed in the **Results** panel as a table, JSON, or chart (switchable via tabs).

### EXPLAIN and execution plan {#explain}

Select **Explain YQL Query** in the editor context menu or in the command palette (`Ctrl+Shift+P`) to get the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md). The plugin displays the plan's operation tree in text form.

### Session manager {#session-manager}

The **Sessions** panel (Activity Bar → YDB) displays all active sessions with the current query, state, and duration (data from the system view [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). The **Toggle Hide Idle** button hides sessions without an active query.

### Cluster dashboard {#cluster-dashboard}

The **Database Load** panel (Activity Bar → YDB) displays cluster load in real time (updated every 10 seconds):

- CPU load (% and number of cores).
- Memory usage (% and volume).
- Network traffic.

{% note warning %}

The dashboard is only available when working with self-hosted {{ ydb-short-name }} installations where {{ ydb-short-name }} Embedded UI is accessible. In Yandex Cloud Managed Service for {{ ydb-short-name }}, Embedded UI is not published, so dashboard data is unavailable — use [cloud platform monitoring tools](https://yandex.cloud/en/docs/ydb/operations/monitoring) instead.

{% endnote %}

### Streaming queries {#streaming-queries}

In the navigator, expand the **Streaming Queries** section. For each query, you can:

- View the source YQL (**View Source**).
- Start and stop (**Start** / **Stop**).

### SQL dialect converter {#convert-dialect}

The plugin allows you to convert an SQL query written in another dialect (PostgreSQL, MySQL, ClickHouse, etc.) to YQL. The converter is available in the **Convert Dialect** panel on the Activity Bar.

To convert a query:

1. In the **Source Dialect** dropdown list, select the source SQL dialect.
2. Paste the source SQL code into the **Input SQL** field.
3. Click **Convert**. The result will appear in the lower field.
4. Copy the result for use in the YQL editor.

For more information on the converter's operation, supported dialects, and limitations, see the [SQL dialect converter to YQL article](../sql-dialect-converter.md).

{% note warning %}

The plugin sends the source query to an external HTTPS service for conversion. The converter does not work without internet access. Do not use the converter for queries containing confidential data.

{% endnote %}

### Viewing permissions {#permissions}

Right-click on an object in the navigator and select **View Permissions** to view the [access rights (ACL)](../../security/authorization.md#right) assigned to that object.

### Generating DDL {#ddl}

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

## Integrating with AI assistants (MCP) {#mcp}

The plugin launches a built-in [MCP server](https://modelcontextprotocol.io/) (Model Context Protocol), which allows AI assistants (Claude Code and others) to directly execute queries to {{ ydb-short-name }} databases configured in the plugin.

### Configuring the port {#mcp-port}

The server starts on port **3333** (localhost only) by default. You can change the port in VS Code settings:

```json
{
  "ydb.mcpPort": 3333
}
```

If the port is already in use, the extension will show a warning and continue working without MCP.

### Connecting Claude Code {#mcp-claude}

1. Make sure the YDB extension is running in VS Code and at least one connection is added in the **Connections** panel (see [Creating a connection](#connection)).

2. Register the MCP server in Claude Code globally for the current user:

    ```bash
    claude mcp add --scope user --transport sse ydb http://localhost:3333/sse
    ```

    The `--scope user` flag saves the configuration globally — the server will be available from any directory where Claude Code is launched. Without this flag, the [local scope](https://docs.claude.com/en/docs/claude-code/mcp#mcp-installation-scopes) is used (default), and registration must be repeated for each project.

3. Check the connection:

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
| `ydb_yql_help` | `query`, `connection?` | Search YQL documentation (requires [RAG](#rag) to be enabled) |

The `connection` parameter is the connection name as it appears in the **Connections** panel.

## Searching YQL documentation (RAG) {#rag}

The plugin supports semantic and keyword search of YQL documentation using a built-in RAG index (Retrieval-Augmented Generation). This allows AI assistants (via the `ydb_yql_help` tool) to find relevant fragments of the syntax reference when writing queries.

### Enabling RAG {#rag-enable}

1. When creating or editing a connection, set the **Use RAG** flag.
2. The plugin will automatically detect the {{ ydb-short-name }} version and load the corresponding index from the cloud on the first connection.

### Search modes {#rag-modes}

- **Keyword search** — works without additional dependencies, used by default.
- **Semantic search (vector)** — requires locally running [Ollama](https://ollama.com) with the [`nomic-embed-text`](https://ollama.com/library/nomic-embed-text) embedding model. See [Installing Ollama and the embedding model](#install-ollama).

The RAG status (Running / Not running) and Ollama status are displayed directly in the connection form.

### Installing Ollama and the embedding model {#install-ollama}

[Ollama](https://ollama.com) is a local server for running language models and embedding models. The plugin accesses it via HTTP to convert documentation and search queries into vectors for semantic search.

1. Install Ollama following the instructions on the [official download page](https://ollama.com/download) (macOS, Windows, and Linux are supported).

2. Make sure the Ollama service is running and accessible at `http://localhost:11434`:

    ```bash
    curl http://localhost:11434/api/tags
    ```

    A JSON response with a list of models indicates that the service is running.

3. Download the `nomic-embed-text` embedding model (~270 MB):

    ```bash
    ollama pull nomic-embed-text
    ```

4. Verify that the model is available:

    ```bash
    ollama list
    ```

    The output should include the `nomic-embed-text` line.

5. Open the connection form in the plugin and make sure the Ollama status is **Running**. If the status is **Not running**, check that the Ollama service is running and accessible at `http://localhost:11434`.

{% note info %}

The Ollama URL (`http://localhost:11434`) and model name (`nomic-embed-text`) are already set in the plugin by default — you only need to override the `ydb.ragOllamaUrl` and `ydb.ragOllamaModel` settings in case of a non-standard installation.

You cannot use another embedding model: the indexes published by the plugin in the cloud are built specifically on `nomic-embed-text`, and changing the model will make the request and documentation vectors incompatible.

{% endnote %}

## Updating the plugin {#updates}

The update method depends on how the plugin was installed.

### Updating from the VS Code Marketplace {#update-marketplace}

If the plugin was installed from the [Marketplace](#install-marketplace), VS Code updates it automatically.

To update the plugin manually:

1. Open the Extensions panel (Ctrl+Shift+X / Cmd+Shift+X).
2. In the search bar, enter `YDB for VS Code` and open the installed extension.
3. If a new version is available, the **Update** button will appear — click it.

### Updating from a VSIX file {#update-vsix}

If the plugin was installed from a [VSIX file](#install-vsix), automatic updates are not available — VS Code does not track updates for manually installed extensions.

1. Download a new .vsix file from the [GitHub Releases page](https://github.com/ydb-platform/ydb-vscode-plugin/releases).
2. Install it over the old version using the same method as the initial installation — VS Code will automatically replace the previous version.
