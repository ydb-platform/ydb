# Connecting to {{ ydb-short-name }} using the VS Code plugin

[Visual Studio Code](https://code.visualstudio.com) is a free, cross-platform, open-source code editor that supports a wide ecosystem of extensions for working with databases, language servers, and development tools.

[YDB for VS Code](https://github.com/ydb-platform/ydb-vscode-plugin) is a VS Code extension with native support for {{ ydb-short-name }}. The plugin provides a specialized interface for working with {{ ydb-short-name }} objects: a hierarchical navigator for tables, topics, views, external data sources, support for all authentication methods, a [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting, execution plan visualization, session and cluster monitoring, access control management (ACL), a built-in [MCP server](https://modelcontextprotocol.io/) for AI assistants, and other features.

## Key features of the plugin {#features}

- Connecting to {{ ydb-name }} with all [authentication](../../security/authentication.md) methods: anonymous, static, token, service account, and metadata.
- Hierarchical object navigator: tables ( [row-oriented](../../concepts/glossary.md#row-oriented-table) and [column-oriented](../../concepts/glossary.md#column-oriented-table)), [topics](../../concepts/datamodel/topic.md), [views](../../concepts/datamodel/view.md), [external data sources](../../concepts/glossary.md#external-data-source), [external tables](../../concepts/glossary.md#external-table), [transfers](../../concepts/transfer.md), [streaming queries](../../concepts/glossary.md#streaming-query).
- System objects: [system views](../../dev/system-views.md) (`.sys`), [resource pools](../../concepts/glossary.md#resource-pool).
- [YQL](../../concepts/glossary.md#yql) editor with syntax highlighting, autocompletion for tables and columns.
- Query execution and result visualization: table, JSON, chart.
- Visualization of the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md) (`EXPLAIN`).
- Monitoring active sessions via [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Cluster dashboard based on [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md): CPU load, memory usage, network traffic (updates every 10 seconds).
- Managing [access rights (ACL)](../../security/authorization.md#right): viewing permissions on database objects.
- Generating DDL scripts ( [`CREATE`](../../yql/reference/syntax/create_table/index.md)) for any database object.
- Managing [streaming queries](../../concepts/glossary.md#streaming-query): viewing, starting, stopping.
- [SQL query converter](../sql-dialect-converter.md) from other dialects (PostgreSQL, MySQL, ClickHouse, and others) to YQL.
- Built-in [MCP server](#mcp)
- [Semantic search in YQL documentation](#rag)

## Requirements {#requirements}

The plugin requires Visual Studio Code version 1.75.0 or later.

## Installing the plugin {#installation}

The plugin can be installed from the VS Code Marketplace or from a `.vsix` file on the GitHub Releases page.

### Installing from VS Code Marketplace {#install-marketplace}

1. Open the Extensions panel in VS Code (`Ctrl+Shift+X `/` Cmd+Shift+X`).
2. In the search bar, enter `YDB for VS Code`and select the extension from publisher`ydb-tech` ( [direct link](https://marketplace.visualstudio.com/items?itemName=ydb-tech.ydb-vscode-plugin)).
3. Click **Install**.

Alternatively, you can install the plugin from the Marketplace with a single command in the terminal:

```bash
code --install-extension ydb-tech.ydb-vscode-plugin
```

### Installing from a VSIX file {#install-vsix}

This method is suitable if you need a specific version or don't have access to the Marketplace.

1. Go to the [GitHub Releases page](https://github.com/ydb-platform/ydb-vscode-plugin/releases) and download the `ydb-vscode-plugin-*.vsix` file of the required version.
2. Install the extension using one of the following methods:

   - **Via terminal:**

        ```bash
        code --install-extension ydb-vscode-plugin-X.X.X.vsix
        ```
   - **Via VS Code interface:**

     1. Open the Extensions panel (`Ctrl+Shift+X `/` Cmd+Shift+X`).
     2. Click `...` (three dots) in the top right corner of the panel.
     3. Select **Install from VSIX...**.
     4. Specify the path to the downloaded `.vsix` file.

After installation using any method, restart VS Code. The **YDB** icon will appear in the Activity Bar.

## Creating a connection to {{ ydb-name }} {#connection}

1. Click the **YDB** icon in the Activity Bar on the left.
2. In the **Connections** panel, click the **Add Connection** button (`+` icon).
3. The connection creation form will open. Fill in the fields:

   | Field | Description | Example |
   | --- | --- | --- |
   | **Connection Name** | Arbitrary connection name | `my-ydb` |
   | **Host** | Host of the [endpoint](../../concepts/connect.md#endpoint) of the {{ ydb-name }} cluster | `ydb.example.com` |
   | **Port** | Port (default `2135 `) | ` 2135` |
   | **Database** | Path to the [database](../../concepts/connect.md#endpoint) | `/Root/database` |
   | **Monitoring URL** | URL of [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md), used for the dashboard (auto-filled from the host, can be overridden) | `http://ydb.example.com:8765` |
   | **Secure connection (grpcs)** | Use a secure connection (`grpcs://`) | ☑ |[эндпойнта](../../concepts/connect.md#endpoint)
   | **Use RAG** | Enable [search in YQL documentation](#rag) for this connection | ☑ |
4. If necessary, specify the path to a custom CA certificate (PEM) in the **CA Certificate File** field — for connections with non-standard TLS. If the field is left empty, the built-in Yandex Cloud certificate is used.[базе данных](../../concepts/glossary.md#database)
5. Select the authentication method from the **Auth type** dropdown (see [Authentication methods](../../reference/embedded-ui/index.md)
6. Click **Test Connection** to verify the settings. On successful connection, a success message will appear.
7. Click **Save**. The connection will appear in the **Connections** panel.[поиск по документации YQL](#rag)

## Authentication methods {#auth-methods}

The plugin supports all [authentication](#auth-methods)).

### Anonymous {#auth-anonymous}

Connection without credentials. Used for local or test installations of {{ ydb-short-name }}. No additional fields need to be filled.

### Static Credentials (login and password) {#auth-static}

Authentication by login and password. Enter the username in the **Username** field and the password in the **Password** field. on the {{ ydb-short-name }} server. Used if [login and password authentication](../../security/authentication.md) is enabled

{% note info %}

In managed installations of {{ ydb-name }}, login and password authentication is disabled: managed services use the cloud platform's centralized access control system ( [IAM](https://yandex.cloud/en/docs/iam/)).

{% endnote %}

### Access Token {#auth-token}

Authentication by [IAM](https://yandex.cloud/en/docs/iam/) or 

{% note warning %}[IAM](https://yandex.cloud/en/docs/iam/)

The IAM token has a limited [lifetime of no more than 12 hours](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime), after which you need to obtain it again and update it in the connection settings. For long-lived connections, use authentication by [service account](#auth-service-account) or [metadata service](#auth-metadata).

{% endnote %}[IAM-](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token)[OAuth-токену](https://yandex.cloud/en/docs/iam/concepts/authorization/oauth-token)

### Service Account Key File {#auth-service-account}

Authentication by the key of the [service account](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime) of Yandex Cloud. Specify the path to the JSON file with the key in the **Service Account Key File** field (use the **Browse** button to select the file). For more information on how to create an authorized key, see the [Yandex Cloud documentation](#auth-service-account).

Key file format:[срок жизни — не более 12 часов](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token#lifetime)[сервисному аккаунту](#auth-service-account)[сервису метаданных](#auth-metadata)

```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."[сервисного аккаунта](https://yandex.cloud/en/docs/iam/concepts/users/service-accounts)[документации Yandex Cloud](https://yandex.cloud/en/docs/iam/operations/authentication/manage-authorized-keys)
}
```

### Metadata Service {#auth-metadata}

Authentication via the [Yandex Cloud metadata service](https://yandex.cloud/en/docs/compute/operations/vm-metadata/get-vm-metadata). The plugin obtains an IAM token from the virtual machine's metadata service. It is used only when VS Code is running on a Yandex Cloud virtual machine.

## Object Navigator {#object-navigator}

After connecting, click on the connection in the **Connections** panel — the **Navigator** panel will open with the hierarchy of {{ ydb-short-name }} objects. The Navigator contains the following sections:

- **Tables** — tables (row and column), organized by subdirectories according to the path in {{ ydb-short-name }}.
- **System Views** — [system views](../../dev/system-views.md) (`.sys`), such as `query_sessions `, ` partition_stats`.
- **Views** — [views](https://yandex.cloud/en/docs/compute/operations/vm-metadata/get-vm-metadata).
- **Topics** — [topics](../../concepts/datamodel/topic.md).
- **External Data Sources** — [external data sources](../../concepts/glossary.md#external-data-source).
- **External Tables** — [external tables](../../concepts/glossary.md#external-table).
- **Resource Pools** — [resource pools](../../concepts/glossary.md#resource-pool).
- **Transfers** — data transfers.
- **Streaming Queries** — [streaming queries](../../concepts/glossary.md#streaming-query).

Right-click on any object in the navigator opens a context menu with available actions.[представления](../../concepts/datamodel/view.md)

## Working with the plugin {#capabilities}

### Query workspace {#query-workspace}

Open the query workspace via `Ctrl+Shift+Q ` (` Cmd+Shift+Q` on macOS) or click **Open Query Workspace** in the connection context menu. In the workspace, you can write and execute YQL queries, view history and results.[потоковые запросы](../../concepts/glossary.md#streaming-query)

To quickly open the editor with a pre-filled query, right-click on a table or view in the navigator and select:

- **Show Preview** — `SELECT` of the first 100 rows.
- **Make Query** — `SELECT` with the object name.

### YQL editor {#yql-editor}

The editor supports:

- Syntax highlighting of [YQL](../../yql/reference/index.md): keywords, data types, built-in functions.
- Auto-completion of table and column names (based on the schema of the connected database).
- Query execution: `Ctrl+Enter ` (` Cmd+Enter` on macOS).

Example YQL query:

```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());[YQL](../../yql/reference/index.md)
```

Execution results are displayed in the **Results** panel as a table, JSON, or diagram (switch by tabs).

### EXPLAIN and execution plan {#explain}

Select **Explain YQL Query** in the editor context menu or in the command palette (`Ctrl+Shift+P`) to get the [query execution plan](../../dev/query-execution-optimization/query-plans-optimization.md). The plugin displays the plan's operation tree in text form.

### Session manager {#session-manager}

The **Sessions** panel (Activity Bar → YDB) displays all active sessions with the current query, state, and duration (data from the system view [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). The **Toggle Hide Idle** button hides sessions without an active query.

### Cluster dashboard {#cluster-dashboard}

The **Database Load** panel (Activity Bar → YDB) displays the cluster load in real time (updated every 10 seconds):[план выполнения запроса](../../dev/query-execution-optimization/query-plans-optimization.md)

- CPU load (% and number of cores).[`.sys/query_sessions`](../../dev/system-views.md#query-sessions)
- Memory usage (% and amount).
- Network traffic.[`.sys/query_sessions`](../../dev/system-views.md#query-sessions)

{% note warning %}

The dashboard is only available when working with self-hosted installations of {{ ydb-short-name }}, where there is access to the [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md). In Yandex Cloud Managed Service for {{ ydb-short-name }}, the Embedded UI is not published, so the dashboard data is unavailable — for monitoring, use [cloud platform tools](https://yandex.cloud/en/docs/ydb/operations/monitoring).

{% endnote %}

### Streaming queries {#streaming-queries}

In the navigator, expand the **Streaming Queries** section. For each query, the following are available:[{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md)

- View the source YQL (**View Source**).[{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md)[средства облачной платформы](https://yandex.cloud/en/docs/ydb/operations/monitoring)
- Start and stop (**Start** / **Stop**).

### SQL dialect converter {#convert-dialect}

The plugin allows you to convert an SQL query written in another dialect (PostgreSQL, MySQL, ClickHouse, and others) to YQL. The converter is available in the **Convert Dialect** panel on the Activity Bar.

To convert a query:

1. In the **Source Dialect** dropdown, select the source SQL dialect.
2. Paste the source SQL code into the **Input SQL** field.
3. Click **Convert**. The result appears in the bottom field.
4. Copy the result to use in the YQL editor.

For details on how the converter works, supported dialects, and limitations, see the article [SQL Dialect Converter to YQL](../sql-dialect-converter.md).

{% note warning %}

To perform the conversion, the plugin sends the original query to an external HTTPS service. The converter does not work without internet access. Do not use the converter for queries containing confidential data.

{% endnote %}

### Viewing permissions {#permissions}

Right-click an object in the navigator and select **View Permissions** to view the [access rights (ACL)](../../security/authorization.md#right) assigned to that object.

### DDL generation {#ddl}

Right-click a table, topic, or other object in the navigator and select **Create DDL** to get the `CREATE` script of the object.

### Creating objects {#create-objects}

Right-click the appropriate folder in the navigator to create a new object:[прав доступа (ACL)](../../security/authorization.md#right)

- **New Row Table** / **New Column Table** — create a row or column table.
- **New Topic** — create a topic.
- **New View** — create a view.
- **New Object Storage Data Source** / **New YDB Data Source** — create an external data source.
- **New CSV/JSON/Parquet External Table** — create an external table.
- **New Transfer** — create a transfer.
- **New Streaming Query** — create a streaming query.

## Integration with AI assistants (MCP) {#mcp}

The plugin runs a built-in [MCP server](https://modelcontextprotocol.io/) (Model Context Protocol) that allows AI assistants (Claude Code and others) to directly execute queries against {{ ydb-short-name }} databases configured in the plugin.

### Port configuration {#mcp-port}

The server runs on port **3333** (localhost only) by default. You can change the port in VS Code settings:

```json
{
  "ydb.mcpPort": 3333 [MCP-сервер](https://modelcontextprotocol.io/)
}
```

If the port is already in use, the extension will show a warning and continue working without MCP.

### Connecting Claude Code {#mcp-claude}

1. Make sure the YDB extension is running in VS Code and at least one connection is added in the **Connections** panel (see [Creating a connection](#connection)).
2. Register the MCP server in Claude Code globally for the current user:

    ```bash
    claude mcp add --scope user --transport sse ydb http://localhost:3333/sse
    ```

| Tool | Parameters | Description |
| --- | --- | --- |[Создание подключения](#connection)
| `ydb_list_connections` | — | List of all connections configured in the plugin |
| `ydb_query `|`connection`, ` sql` | Execute a YQL query |
| `ydb_describe_table `|`connection`, ` path`| Get table schema (columns, primary key) |[scope`local`](https://docs.claude.com/en/docs/claude-code/mcp#mcp-installation-scopes)
| `ydb_list_directory `|`connection`, ` path?` | List of objects in the database directory |
| `ydb_list_all `|`connection`, ` path?`, `limit?`, `offset?` | Recursive list of all objects |
| `ydb_yql_help `|`query`, ` connection?` | Search YQL documentation (requires [RAG](https://docs.claude.com/en/docs/claude-code/mcp#mcp-installation-scopes)

The `connection`parameter is the connection name as it appears in the **Connections** panel.[scope`local`](https://docs.claude.com/en/docs/claude-code/mcp#mcp-installation-scopes)

## Search YQL documentation (RAG) {#rag}

The plugin supports semantic and keyword search over YQL documentation using a built-in RAG index (Retrieval-Augmented Generation). This allows AI assistants (via the `ydb_yql_help` tool) to find relevant fragments of the syntax reference when writing queries.

### Enabling RAG {#rag-enable}

1. When creating or editing a connection, set the **Use RAG** flag.
2. The plugin will automatically detect the {{ ydb-short-name }} version and download the corresponding index from the cloud on the first connection.

### Search modes {#rag-modes}

- **Keyword search** — works without additional dependencies, used by default.
- **Semantic search (vector)** — requires a locally running [Ollama](https://ollama.com) with the [`nomic-embed-text`](https://ollama.com/library/nomic-embed-text) embedding model. See [Installing Ollama and embedding models](#install-ollama).

The RAG status (Running / Not running) and Ollama status are displayed directly in the connection form.

### Installing Ollama and embedding models {#install-ollama}

[Ollama](https://ollama.com) is a local server for running language models and embedding models. The plugin accesses it via HTTP to convert documentation texts and search queries into vectors for semantic search.

1. Install Ollama following the instructions on the [official download page](https://ollama.com) (macOS, Windows, and Linux are supported).[`nomic-embed-text`](https://ollama.com/library/nomic-embed-text)
2. Make sure the Ollama service is running and available at `http://localhost:11434`:

    ```bash

   A JSON response with a list of models means the service is working.
3. Download the `nomic-embed-text`embedding model (~270 MB):[официальной странице загрузки](https://ollama.com/download)```bash
    ollama pull nomic-embed-text [Ollama](https://ollama.com)[`nomic-embed-text`](https://ollama.com/library/nomic-embed-text)
    ```

{% note info %}[Ollama](https://ollama.com)[`nomic-embed-text`](https://ollama.com/library/nomic-embed-text)

The Ollama URL (`http://localhost:11434`) and model name (`nomic-embed-text`) are already set by default in the plugin — you only need to override the `ydb.ragOllamaUrl `and` ydb.ragOllamaModel` settings for a non-standard installation.

You cannot use a different embedding model: the indexes published by the plugin in the cloud are built specifically on `nomic-embed-text`, and if you change the model, the query and documentation vectors will become incompatible.

{% endnote %}[Ollama](https://ollama.com)

## Updating the plugin {#updates}

The update method depends on how the plugin was installed.

### Updating from VS Code Marketplace {#update-marketplace}

If the plugin is installed from the [Marketplace](#install-marketplace), VS Code updates it automatically.

To update the plugin manually:

1. Open the Extensions panel (`Ctrl+Shift+X `/` Cmd+Shift+X`).
2. In the search bar, enter `YDB for VS Code` and open the installed extension.
3. If a new version is available, an **Update** button will appear — click it.

### Updating from a VSIX file {#update-vsix}

If the plugin is installed from a [VSIX file](#install-vsix), automatic updates are not available — VS Code does not track updates for manually installed extensions.

1. Download the new `.vsix` file from the [GitHub Releases](https://github.com/ydb-platform/ydb-vscode-plugin/releases) page.
2. Install it over the old version using the same method as during the initial installation — VS Code will automatically replace the previous version.
