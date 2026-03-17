import argparse
from collections.abc import Sequence
from typing import Any

__all__ = ['main', 'logfire_info']

def logfire_info() -> str:
    """Show versions of logfire, OS and related packages."""

class SplitArgs(argparse.Action):
    def __call__(self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: str | Sequence[Any] | None, option_string: str | None = None): ...

def main(args: list[str] | None = None) -> None:
    """Run the CLI."""
