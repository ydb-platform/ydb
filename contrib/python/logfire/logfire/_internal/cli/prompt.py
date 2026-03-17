"""Prompt command for Logfire CLI."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from rich.console import Console

from logfire._internal.cli.auth import parse_auth
from logfire._internal.client import LogfireClient
from logfire.exceptions import LogfireConfigError

LOGFIRE_MCP_TOML = """
[mcp_servers.logfire]
command = "uvx"
args = ["logfire-mcp@latest"]
env = {{ "LOGFIRE_READ_TOKEN" = "{token}" }}
"""


def parse_prompt(args: argparse.Namespace) -> None:
    """Creates a prompt to be used with your favorite LLM.

    The prompt assumes you are using Logfire MCP.
    """
    console = Console(file=sys.stderr)

    try:
        client = LogfireClient.from_url(args.logfire_url)
    except LogfireConfigError:  # pragma: no cover
        parse_auth(args)
        client = LogfireClient.from_url(args.logfire_url)

    if args.claude:
        configure_claude(client, args.organization, args.project, console)
    elif args.codex:
        configure_codex(client, args.organization, args.project, console)
    elif args.opencode:
        configure_opencode(client, args.organization, args.project, console)

    response = client.get_prompt(args.organization, args.project, args.issue)
    sys.stdout.write(response['prompt'])


def _create_read_token(client: LogfireClient, organization: str, project: str, console: Console) -> str:
    console.print('Logfire MCP server not found. Creating a read token...', style='yellow')
    response = client.create_read_token(organization, project)
    return response['token']


def configure_claude(client: LogfireClient, organization: str, project: str, console: Console) -> None:
    if not shutil.which('claude'):
        console.print('claude is not installed. Install `claude`, or remove the `--claude` flag.')
        exit(1)

    output = subprocess.check_output(['claude', 'mcp', 'list'])
    if 'logfire-mcp' not in output.decode('utf-8'):
        token = _create_read_token(client, organization, project, console)
        subprocess.check_output(
            shlex.split(f'claude mcp add logfire -e LOGFIRE_READ_TOKEN={token} -- uvx logfire-mcp@latest')
        )
        console.print('Logfire MCP server added to Claude.', style='green')


def configure_codex(client: LogfireClient, organization: str, project: str, console: Console) -> None:
    if not shutil.which('codex'):
        console.print('codex is not installed. Install `codex`, or remove the `--codex` flag.')
        exit(1)

    codex_home = Path(os.getenv('CODEX_HOME', Path.home() / '.codex'))
    codex_config = codex_home / 'config.toml'
    if not codex_config.exists():
        console.print('Codex config file not found. Install `codex`, or remove the `--codex` flag.')
        exit(1)

    codex_config_content = codex_config.read_text()

    if 'logfire-mcp' not in codex_config_content:
        token = _create_read_token(client, organization, project, console)
        mcp_server_toml = LOGFIRE_MCP_TOML.format(token=token)
        codex_config.write_text(codex_config_content + mcp_server_toml)
        console.print('Logfire MCP server added to Codex.', style='green')


def configure_opencode(client: LogfireClient, organization: str, project: str, console: Console) -> None:
    # Check if opencode is installed
    if not shutil.which('opencode'):
        console.print('opencode is not installed. Install `opencode`, or remove the `--opencode` flag.')
        exit(1)

    try:
        output = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'])
    except subprocess.CalledProcessError:
        root_dir = Path.cwd()
    else:
        root_dir = Path(output.decode('utf-8').strip())

    opencode_config = root_dir / 'opencode.jsonc'
    opencode_config.touch()

    opencode_config_content = opencode_config.read_text()

    if 'logfire-mcp' not in opencode_config_content:
        token = _create_read_token(client, organization, project, console)
        if not opencode_config_content:
            opencode_config.write_text(json.dumps(opencode_mcp_json(token), indent=2))
        else:
            opencode_config_json = json.loads(opencode_config_content)
            opencode_config_json.setdefault('mcp', {})
            opencode_config_json['mcp'] = {'logfire-mcp': opencode_mcp_json(token)}
            opencode_config.write_text(json.dumps(opencode_config_json, indent=2))
        console.print('Logfire MCP server added to OpenCode.', style='green')


# https://opencode.ai/docs/mcp-servers/#local
def opencode_mcp_json(token: str) -> dict[str, Any]:
    return {
        'mcp': {
            'logfire-mcp': {
                'type': 'local',
                'command': ['uvx', 'logfire-mcp@latest'],
                'environment': {'LOGFIRE_READ_TOKEN': token},
            }
        }
    }
