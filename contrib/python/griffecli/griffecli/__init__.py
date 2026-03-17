# This top-level module imports all public names from the CLI package,
# and exposes them as public objects.

"""Griffe CLI package.

The CLI (Command Line Interface) for the griffe library.
This package provides command-line tools for interacting with griffe.

## CLI entrypoints

- [`griffecli.main`][]: Run the main program.
- [`griffecli.check`][]: Check for API breaking changes in two versions of the same package.
- [`griffecli.dump`][]: Load packages data and dump it as JSON.
- [`griffecli.get_parser`][]: Get the argument parser for the CLI.
"""

from __future__ import annotations

from griffecli._internal.cli import DEFAULT_LOG_LEVEL, check, dump, get_parser, main

__all__ = [
    "DEFAULT_LOG_LEVEL",
    "check",
    "dump",
    "get_parser",
    "main",
]
