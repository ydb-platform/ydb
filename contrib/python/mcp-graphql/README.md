# MCP GraphQL

An MCP (Model Context Protocol) server that enables interaction with GraphQL APIs.

## Description

MCP GraphQL is a tool that implements the Model Context Protocol (MCP) to provide a standardized interface for interacting with GraphQL APIs. It automatically exposes each GraphQL query as a separate MCP tool, allowing MCP-compatible clients to seamlessly communicate with GraphQL services.

## Features

- Each GraphQL query is exposed as a distinct MCP tool
- Tool parameters automatically match the corresponding GraphQL query parameters
- JSON schema for tool inputs is dynamically generated from GraphQL query parameters
- No schema definition required - simply provide the API URL and credentials
- Currently supports GraphQL queries (mutations support planned for future releases)
- Configurable authentication (Bearer, Basic, custom headers)
- Automatic handling of complex GraphQL types

## Requirements

- Python 3.11 or higher

## Installation

### Using uv (recommended)

When using [`uv`](https://docs.astral.sh/uv/) no specific installation is needed. We will
use [`uvx`](https://docs.astral.sh/uv/guides/tools/) to directly run *mcp-graphql*.

### Using pip

Alternatively you can install `mcp-graphql` via pip:

```bash
pip install mcp-graphql
```

### Installation from source code

```bash
git clone https://github.com/your-username/mcp_graphql.git
cd mcp_graphql
pip install .
```

## Usage

### As a command line tool

Using uvx:
```bash
uvx mcp-graphql --api-url="https://api.example.com/graphql" --auth-token="your-token"
```

Using pip installation:
```bash
mcp-graphql --api-url="https://api.example.com/graphql" --auth-token="your-token"
```
or
```bash
python -m mcp_graphql --api-url="https://api.example.com/graphql" --auth-token="your-token"
```

### Available options

- `--api-url`: GraphQL API URL (required)
- `--auth-token`: Authentication token (optional, can also be set via `MCP_AUTH_TOKEN` environment variable)
- `--auth-type`: Authentication type, default is "Bearer" (optional)
- `--auth-headers`: Custom authentication headers in JSON format (optional)
- `--queries-file`: Path to a .gql file containing predefined GraphQL queries (optional)
- `--queries`: Predefined GraphQL queries passed directly as a string (optional)
- `--max-depth`: Maximum depth when auto-generating queries (default: 5)

Example with custom headers:

```bash
mcp-graphql --api-url="https://api.example.com/graphql" --auth-headers='{"Authorization": "Bearer token", "X-API-Key": "key"}'
```

Example with predefined queries file:

```bash
mcp-graphql --api-url="https://api.example.com/graphql" --queries-file="./queries.gql"
```

Example passing queries directly as a string (use single quotes to avoid shell conflicts):

```bash
mcp-graphql --api-url="https://api.example.com/graphql" --queries='query Hello { hello }'
```

### About automatic query generation

If neither `--queries-file` nor `--queries` is supplied, *mcp-graphql* will
automatically build a query by introspecting the GraphQL schema and selecting
**all** scalar fields up to a configurable depth. This is convenient for
quickly exploring an API, but it has two main drawbacks:

1. **Too much depth** – drilling deep into nested objects (especially lists)
   can return a large amount of data and overflow the LLM context window.
2. **Lack of control** – you cannot precisely choose which fields are
   included, so tokens may be wasted on irrelevant information.

The `--max-depth` option mitigates the first issue by limiting the recursion
depth (default = 5). Even so, the best practice is to **define the exact
queries you need** through `--queries-file` or `--queries`. In doing so:

* You control exactly which fields are returned and avoid unnecessary lists.
* Every named operation in your file/string is automatically exposed as an MCP
  *tool* with no manual boilerplate.

Example using `--max-depth` to limit the auto-generated query to depth 2:

```bash
mcp-graphql --api-url="https://api.example.com/graphql" --max-depth 2
```

For production workloads you should supply your own queries:

```bash
# Using a file
mcp-graphql --api-url="https://api.example.com/graphql" \
            --queries-file="./queries.gql"

# Or as a string
mcp-graphql --api-url="https://api.example.com/graphql" \
            --queries='query UserMini { viewer { id name } }'
```

The `queries.gql` file should contain one or more **named** operations, e.g.:

```graphql
# queries.gql
query GetUser($id: ID!) {
  user(id: $id) {
    id
    name
    email
  }
}

query ListPosts {
  posts {
    id
    title
  }
}
```

### As a library

```python
import asyncio
from pathlib import Path
from mcp_graphql import serve

auth_headers = {"Authorization": "Bearer your-token"}
api_url = "https://api.example.com/graphql"
queries_file = Path("queries.gql")  # optional, set to None to expose all queries

asyncio.run(serve(api_url, auth_headers, queries_file=queries_file))
```

Passing the queries directly as a string from code:

```python
queries_str = """
query Hello($name: String!) {
  hello(name: $name)
}
"""

asyncio.run(serve(api_url, auth_headers, queries=queries_str, max_depth=3))
```

## Configuration

### Configure for Claude.app

Add to your Claude settings:

<details>
<summary>Using uvx</summary>

```json
"mcpServers": {
  "graphql": {
    "command": "uvx",
    "args": ["mcp-graphql", "--api-url", "https://api.example.com/graphql"]
  }
}
```
</details>

<details>
<summary>Using docker</summary>

```json
"mcpServers": {
  "graphql": {
    "command": "docker",
    "args": ["run", "-i", "--rm", "mcp/graphql", "--api-url", "https://api.example.com/graphql"]
  }
}
```
</details>

<details>
<summary>Using pip installation</summary>

```json
"mcpServers": {
  "graphql": {
    "command": "python",
    "args": ["-m", "mcp_graphql", "--api-url", "https://api.example.com/graphql"]
  }
}
```
</details>

## How It Works

MCP GraphQL automatically:

1. Introspects the provided GraphQL API
2. Creates an MCP tool for each available GraphQL query
3. Generates JSON schema for tool inputs based on query parameters
4. Handles type conversions between GraphQL and JSON

When a tool is called, the server:
1. Converts the tool call parameters to a GraphQL query
2. Executes the query against the API
3. Returns the results to the MCP client

## Planned Features

- Support for GraphQL mutations (with appropriate safeguards)
- Improved error handling and validation
- Additional optimizations based on specific GraphQL API implementations

## Development

### Setting up the development environment

```bash
# Create virtual environment using uv
uv venv

# Install dependencies
uv sync
```

### Linting

```bash
ruff check .
```

### Running the server in development mode

When working locally you can start the MCP GraphQL server with hot-reloading and inspect its tools using the Model Context Protocol Inspector:

```bash
npx "@modelcontextprotocol/inspector" uv run -n --project $PWD mcp-graphql --api-url http://localhost:3010/graphql
```

Replace `http://localhost:3010/graphql` with the URL of your local GraphQL endpoint if it differs.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome. Please feel free to submit a Pull Request or open an Issue.
