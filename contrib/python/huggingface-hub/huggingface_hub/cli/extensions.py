# Copyright 2026 The HuggingFace Team. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Contains helper utilities for hf CLI extensions."""

import errno
import json
import os
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Optional

import typer

from huggingface_hub.errors import CLIError
from huggingface_hub.utils import get_session, tabulate

from ._cli_utils import typer_factory


DEFAULT_EXTENSION_OWNER = "huggingface"
EXTENSIONS_ROOT = Path("~/.local/share/hf/extensions")
MANIFEST_FILENAME = "manifest.json"
EXTENSIONS_HELP = (
    "Manage hf CLI extensions.\n\n"
    "Security Warning: extensions are third-party executables. "
    "Install only from sources you trust."
)
extensions_cli = typer_factory(help=EXTENSIONS_HELP)


@dataclass
class ExtensionManifest:
    owner: str
    repo: str
    repo_id: str
    short_name: str
    executable_name: str
    executable_path: str
    type: str  # "binary" (future: "python"?), not sure yet how to handle different types of extensions
    installed_at: str
    source: str


@extensions_cli.command(
    "install",
    examples=[
        "hf extensions install hf-claude",
        "hf extensions install hanouticelina/hf-claude",
    ],
)
def extension_install(
    ctx: typer.Context,
    repo_id: Annotated[
        str,
        typer.Argument(help="GitHub extension repository in `[OWNER/]hf-<name>` format."),
    ],
    force: Annotated[bool, typer.Option("--force", help="Overwrite if already installed.")] = False,
) -> None:
    """Install an extension from a public GitHub repository.

    Security warning: this installs a third-party executable. Install only from sources you trust.
    """
    owner, repo_name, short_name = _normalize_repo_id(repo_id)
    root_ctx = ctx.find_root()
    reserved_commands = set(getattr(root_ctx.command, "commands", {}).keys())
    if short_name in reserved_commands:
        raise CLIError(
            f"Cannot install extension '{short_name}' because it conflicts with an existing `hf {short_name}` command."
        )

    extension_dir = _get_extension_dir(short_name)
    if extension_dir.exists():
        if not force:
            raise CLIError(f"Extension '{short_name}' is already installed. Use --force to overwrite.")
        shutil.rmtree(extension_dir)

    executable_name = _get_executable_name(short_name)
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo_name}/refs/heads/main/{executable_name}"
    try:
        response = get_session().get(raw_url, follow_redirects=True)
        response.raise_for_status()
    except Exception as e:
        raise CLIError(f"Failed to download '{executable_name}' from '{owner}/{repo_name}': {e}") from e

    with TemporaryDirectory() as tmp_dir:
        tmp_executable = Path(tmp_dir) / executable_name
        tmp_executable.write_bytes(response.content)
        if os.name != "nt":
            os.chmod(tmp_executable, 0o755)

        manifest = ExtensionManifest(
            owner=owner,
            repo=repo_name,
            repo_id=f"{owner}/{repo_name}",
            short_name=short_name,
            executable_name=executable_name,
            executable_path=str(_get_extension_executable_path(short_name)),
            type="binary",
            installed_at=datetime.now(timezone.utc).isoformat(),
            source=f"https://github.com/{owner}/{repo_name}",
        )
        _persist_installed_extension(
            extension_dir=extension_dir,
            source_executable=tmp_executable,
            manifest=manifest,
        )

    print(f"Installed extension '{owner}/{repo_name}'.")
    print(f"Run it with: hf {short_name}")
    print(f"Or with: hf extensions exec {short_name}")


@extensions_cli.command(
    "exec",
    context_settings={"allow_extra_args": True, "allow_interspersed_args": False, "ignore_unknown_options": True},
    examples=[
        "hf extensions exec claude -- --help",
        "hf extensions exec claude --model zai-org/GLM-5",
    ],
)
def extension_exec(
    ctx: typer.Context,
    name: Annotated[
        str,
        typer.Argument(help="Extension name (with or without `hf-` prefix)."),
    ],
) -> None:
    """Execute an installed extension."""
    short_name = _normalize_extension_name(name)
    executable_path = _get_extension_executable_path(short_name)

    if not executable_path.is_file():
        raise CLIError(f"Extension '{short_name}' is not installed.")

    exit_code = _execute_extension_binary(executable_path=executable_path, args=list(ctx.args))
    raise typer.Exit(code=exit_code)


@extensions_cli.command("list", examples=["hf extensions list"])
def extension_list() -> None:
    """List installed extension commands."""
    root_dir = _get_extensions_root()
    if not root_dir.is_dir():
        print("No extensions installed.")
        return

    rows = []
    for extension_dir in sorted(root_dir.iterdir()):
        if not extension_dir.is_dir() or not extension_dir.name.startswith("hf-"):
            continue

        short_name = extension_dir.name[3:]
        manifest_path = extension_dir / MANIFEST_FILENAME

        repository = ""
        installed_at = ""
        if manifest_path.is_file():
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            repository = str(data.get("repo_id", ""))
            installed_at = str(data.get("installed_at", ""))

        rows.append([f"hf {short_name}", repository, installed_at])

    if not rows:
        print("No extensions installed.")
        return

    print(tabulate(rows, headers=["COMMAND", "REPOSITORY", "INSTALLED_AT"]))  # type: ignore[arg-type]


@extensions_cli.command("remove", examples=["hf extensions remove claude"])
def extension_remove(
    name: Annotated[
        str,
        typer.Argument(help="Extension name to remove (with or without `hf-` prefix)."),
    ],
) -> None:
    """Remove an installed extension."""
    short_name = _normalize_extension_name(name)
    extension_dir = _get_extension_dir(short_name)

    if not extension_dir.is_dir():
        raise CLIError(f"Extension '{short_name}' is not installed.")

    shutil.rmtree(extension_dir)
    print(f"Removed extension '{short_name}'.")


### HELPER FUNCTIONS


def _dispatch_unknown_top_level_extension(args: list[str], known_commands: set[str]) -> Optional[int]:
    if not args:
        return None

    command_name = args[0]
    if command_name.startswith("-"):
        return None
    if command_name in known_commands:
        return None

    short_name = command_name[3:] if command_name.startswith("hf-") else command_name
    if not short_name:
        return None

    executable_path = _get_extension_executable_path(short_name)
    if not executable_path.is_file():
        return None

    return _execute_extension_binary(executable_path=executable_path, args=list(args[1:]))


def _persist_installed_extension(extension_dir: Path, source_executable: Path, manifest: ExtensionManifest) -> None:
    executable_path = extension_dir / manifest.executable_name
    manifest_path = extension_dir / MANIFEST_FILENAME

    try:
        extension_dir.mkdir(parents=True, exist_ok=False)
        shutil.copy2(source_executable, executable_path)
        if os.name != "nt":
            os.chmod(executable_path, 0o755)
        manifest_path.write_text(json.dumps(asdict(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except Exception:
        shutil.rmtree(extension_dir, ignore_errors=True)
        raise


def _get_extensions_root() -> Path:
    root_dir = EXTENSIONS_ROOT.expanduser()
    root_dir.mkdir(parents=True, exist_ok=True)
    return root_dir


def _get_extension_dir(short_name: str) -> Path:
    safe_name = _validate_extension_short_name(short_name, original_input=short_name)
    root = _get_extensions_root().resolve()
    target = (root / f"hf-{safe_name}").resolve()
    if root not in target.parents:
        raise CLIError(f"Invalid extension name '{short_name}'.")
    return target


def _get_executable_name(short_name: str) -> str:
    name = f"hf-{short_name}"
    if os.name == "nt":
        name += ".exe"
    return name


def _get_extension_executable_path(short_name: str) -> Path:
    return _get_extension_dir(short_name) / _get_executable_name(short_name)


_ALLOWED_EXTENSION_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _validate_extension_short_name(short_name: str, *, original_input: str) -> str:
    name = short_name.strip()
    if not name:
        raise CLIError("Extension name cannot be empty.")
    if any(sep in name for sep in ("/", "\\")):
        raise CLIError(f"Invalid extension name '{original_input}'.")
    if ".." in name or ":" in name:
        raise CLIError(f"Invalid extension name '{original_input}'.")
    if not _ALLOWED_EXTENSION_NAME.fullmatch(name):
        raise CLIError(
            f"Invalid extension name '{original_input}'. Allowed characters: letters, digits, '.', '_' and '-'."
        )
    return name


def _normalize_repo_id(repo_id: str) -> tuple[str, str, str]:
    if "://" in repo_id:
        raise CLIError("Only GitHub repositories in `[OWNER/]hf-<name>` format are supported.")

    parts = repo_id.split("/")
    if len(parts) == 1:
        owner = DEFAULT_EXTENSION_OWNER
        repo_name = parts[0]
    elif len(parts) == 2 and all(parts):
        owner, repo_name = parts
    else:
        raise CLIError(f"Expected `[OWNER/]REPO` format, got '{repo_id}'.")

    if not repo_name.startswith("hf-"):
        raise CLIError(f"Extension repository name must start with 'hf-', got '{repo_name}'.")

    short_name = repo_name[3:]
    if not short_name:
        raise CLIError("Invalid extension repository name 'hf-'.")
    _validate_extension_short_name(short_name, original_input=repo_id)

    return owner, repo_name, short_name


def _normalize_extension_name(name: str) -> str:
    candidate = name.strip()
    if not candidate:
        raise CLIError("Extension name cannot be empty.")
    normalized = candidate[3:] if candidate.startswith("hf-") else candidate
    return _validate_extension_short_name(normalized, original_input=name)


def _execute_extension_binary(executable_path: Path, args: list[str]) -> int:
    try:
        return subprocess.call([str(executable_path)] + args)
    except OSError as e:
        if os.name == "nt" or e.errno != errno.ENOEXEC:
            raise
        return subprocess.call(["sh", str(executable_path)] + args)
