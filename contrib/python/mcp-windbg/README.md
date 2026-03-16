# MCP Server for WinDbg Crash Analysis

A Model Context Protocol server that bridges AI models with WinDbg for crash dump analysis and remote debugging.

<!-- mcp-name: io.github.svnscha/mcp-windbg -->

## Overview

This MCP server integrates with [CDB](https://learn.microsoft.com/en-us/windows-hardware/drivers/debugger/opening-a-crash-dump-file-using-cdb) to enable AI models to analyze Windows crash dumps and connect to remote debugging sessions using WinDbg/CDB.

## What is this?

An AI-powered tool that bridges LLMs with WinDbg for crash dump analysis and live debugging. Execute debugger commands through natural language queries like *"Show me the call stack and explain this access violation"*.

## What This is Not

Not a magical auto-fix solution. It's a Python wrapper around CDB that leverages LLM knowledge to assist with debugging.

## Usage Modes

- **Crash Dump Analysis**: Examine Windows crash dumps
- **Live Debugging**: Connect to remote debugging targets
- **Directory Analysis**: Process multiple dumps for patterns

## Quick Start

### Prerequisites
- Windows with [Debugging Tools for Windows](https://developer.microsoft.com/en-us/windows/downloads/windows-sdk/) or [WinDbg from Microsoft Store](https://apps.microsoft.com/detail/9pgjgd53tn86).
- Python 3.10 or higher
- Any MCP-compatible client (GitHub Copilot, Claude Desktop, Cline, Cursor, Windsurf etc.)
- Configure MCP server in your chosen client

> [!TIP]
> In enterprise environments, MCP server usage might be restricted by organizational policies. Check with your IT team about AI tool usage and ensure you have the necessary permissions before proceeding.

### Installation
```bash
pip install mcp-windbg
```

## Transport Options

The MCP server supports multiple transport protocols:

| Transport | Description | Use Case |
|-----------|-------------|----------|
| `stdio` (default) | Standard input/output | Local MCP clients like VS Code, Claude Desktop |
| `streamable-http` | Streamable HTTP | Modern HTTP clients with bidirectional streaming |

### Starting with Different Transports

**Standard I/O (default):**
```bash
mcp-windbg
# or explicitly
mcp-windbg --transport stdio
```

**Streamable HTTP:**
```bash
mcp-windbg --transport streamable-http --host 127.0.0.1 --port 8000
```
Endpoint: `http://127.0.0.1:8000/mcp`

### Command Line Options

```
--transport {stdio,streamable-http}  Transport protocol (default: stdio)
--host HOST                              HTTP server host (default: 127.0.0.1)
--port PORT                              HTTP server port (default: 8000)
--cdb-path PATH                          Custom path to cdb.exe
--symbols-path PATH                      Custom symbols path
--timeout SECONDS                        Command timeout (default: 30)
--verbose                                Enable verbose output
```


## Configuration for Visual Studio Code

To make MCP servers available in all your workspaces, use the global user configuration:

1. Press `F1`, type `>` and select **MCP: Open User Configuration**.
2. Paste the following JSON snippet into your user configuration:

```json
{
    "servers": {
        "mcp_windbg": {
            "type": "stdio",
            "command": "python",
            "args": ["-m", "mcp_windbg"],
            "env": {
                "_NT_SYMBOL_PATH": "SRV*C:\\Symbols*https://msdl.microsoft.com/download/symbols"
            }
        }
    }
}
```

This enables MCP Windbg in any workspace, without needing a local `.vscode/mcp.json` file.

### HTTP Transport Configuration

For scenarios where you need to run the MCP server separately (e.g., remote access, shared server, or debugging the server itself), you can use the HTTP transport:

**1. Start the server manually:**
```bash
python -m mcp_windbg --transport streamable-http --host 127.0.0.1 --port 8000
```

**2. Configure VS Code to connect via HTTP:**
```json
{
    "servers": {
        "mcp_windbg_http": {
            "type": "http",
            "url": "http://localhost:8000/mcp"
        }
    }
}
```

> **Workspace-specific and alternative configuration**: See [Installation documentation](https://github.com/svnscha/mcp-windbg/wiki/Installation) for details on configuring Claude Desktop, Cline, and other clients, or for workspace-only setup.

Once configured, restart your MCP client and start debugging:

```
Analyze the crash dump at C:\dumps\app.dmp
```

## MCP Compatibility

This server implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/), making it compatible with any MCP-enabled client:

The beauty of MCP is that you write the server once, and it works everywhere. Choose your favorite AI assistant!

### Tools

| Tool | Purpose | Use Case |
|------|---------|----------|
| [`list_windbg_dumps`](https://github.com/svnscha/mcp-windbg/wiki/Tools#list_windbg_dumps) | List crash dump files | Discovery and batch analysis |
| [`open_windbg_dump`](https://github.com/svnscha/mcp-windbg/wiki/Tools#open_windbg_dump) | Analyze crash dumps | Initial crash dump analysis |
| [`close_windbg_dump`](https://github.com/svnscha/mcp-windbg/wiki/Tools#close_windbg_dump) | Cleanup dump sessions | Resource management |
| [`open_windbg_remote`](https://github.com/svnscha/mcp-windbg/wiki/Tools#open_windbg_remote) | Connect to remote debugging | Live debugging sessions |
| [`close_windbg_remote`](https://github.com/svnscha/mcp-windbg/wiki/Tools#close_windbg_remote) | Cleanup remote sessions | Resource management |
| [`run_windbg_cmd`](https://github.com/svnscha/mcp-windbg/wiki/Tools#run_windbg_cmd) | Execute WinDbg commands | Custom analysis and investigation |

## Documentation

**[Documentation](https://github.com/svnscha/mcp-windbg/wiki)**

| Topic | Description |
|-------|-------------|
| **[Getting Started](https://github.com/svnscha/mcp-windbg/wiki/Getting-Started)** | Quick setup and first steps |
| **[Installation](https://github.com/svnscha/mcp-windbg/wiki/Installation)** | Detailed installation for pip, MCP registry, and from source |
| **[Usage](https://github.com/svnscha/mcp-windbg/wiki/Usage)** | MCP client integration, command-line usage, and workflows |
| **[Tools Reference](https://github.com/svnscha/mcp-windbg/wiki/Tools)** | Complete API reference and examples |
| **[Troubleshooting](https://github.com/svnscha/mcp-windbg/wiki/Troubleshooting)** | Common issues and solutions |

## Examples

### Crash Dump Analysis

> Analyze this heap address with !heap -p -a 0xABCD1234 and check for buffer overflow"

> Execute !peb and tell me if there are any environment variables that might affect this crash"

> Run .ecxr followed by k and explain the exception's root cause"

### Remote Debugging

> "Connect to tcp:Port=5005,Server=192.168.0.100 and show me the current thread state"

> "Check for timing issues in the thread pool with !runaway and !threads"

> "Show me all threads with ~*k and identify which one is causing the hang"

## Blog

Read about the development journey: [The Future of Crash Analysis: AI Meets WinDbg](https://svnscha.de/posts/ai-meets-windbg/)

### Links

- [Reddit: I taught Copilot to analyze Windows Crash Dumps](https://www.reddit.com/r/programming/comments/1kes3wq/i_taught_copilot_to_analyze_windows_crash_dumps/)
- [Hackernews: AI Meets WinDbg](https://news.ycombinator.com/item?id=43892096)

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=svnscha/mcp-windbg&type=Date)](https://www.star-history.com/#svnscha/mcp-windbg&Date)

## License

MIT
