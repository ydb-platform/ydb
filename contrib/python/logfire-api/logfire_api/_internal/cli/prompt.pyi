import argparse
from logfire._internal.cli.auth import parse_auth as parse_auth
from logfire._internal.client import LogfireClient as LogfireClient
from logfire.exceptions import LogfireConfigError as LogfireConfigError
from rich.console import Console
from typing import Any

LOGFIRE_MCP_TOML: str

def parse_prompt(args: argparse.Namespace) -> None:
    """Creates a prompt to be used with your favorite LLM.

    The prompt assumes you are using Logfire MCP.
    """
def configure_claude(client: LogfireClient, organization: str, project: str, console: Console) -> None: ...
def configure_codex(client: LogfireClient, organization: str, project: str, console: Console) -> None: ...
def configure_opencode(client: LogfireClient, organization: str, project: str, console: Console) -> None: ...
def opencode_mcp_json(token: str) -> dict[str, Any]: ...
