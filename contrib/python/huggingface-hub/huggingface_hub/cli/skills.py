# Copyright 2025 The HuggingFace Team. All rights reserved.
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
"""Contains commands to manage skills for AI assistants.

Usage:
    # install the hf-cli skill for Claude (project-level, in current directory)
    hf skills add --claude

    # install for Cursor (project-level, in current directory)
    hf skills add --cursor

    # install for multiple assistants (project-level)
    hf skills add --claude --codex --opencode --cursor

    # install globally (user-level)
    hf skills add --claude --global

    # install to a custom directory
    hf skills add --dest=~/my-skills

    # overwrite an existing skill
    hf skills add --claude --force
"""

import os
import shutil
from pathlib import Path
from typing import Annotated, Optional

import typer
from click import Context, Group
from typer.main import get_command

from huggingface_hub.errors import CLIError

from ._cli_utils import typer_factory


DEFAULT_SKILL_ID = "hf-cli"

_SKILL_YAML_PREFIX = """\
---
name: hf-cli
description: "Hugging Face Hub CLI (`hf`) for downloading, uploading, and managing repositories, models, datasets, and Spaces on the Hugging Face Hub. Replaces now deprecated `huggingface-cli` command."
---

Install: `curl -LsSf https://hf.co/cli/install.sh | bash -s`.

The Hugging Face Hub CLI tool `hf` is available. IMPORTANT: The `hf` command replaces the deprecated `huggingface_cli` command.

Use `hf --help` to view available functions. Note that auth commands are now all under `hf auth` e.g. `hf auth whoami`.
"""

_SKILL_TIPS = """
## Tips

- Use `hf <command> --help` for full options, usage, and real-world examples
- Use `--format json` for machine-readable output on list commands
- Use `-q` / `--quiet` to print only IDs
- Authenticate with `HF_TOKEN` env var (recommended) or with `--token`
"""

CENTRAL_LOCAL = Path(".agents/skills")
CENTRAL_GLOBAL = Path("~/.agents/skills")

GLOBAL_TARGETS = {
    "codex": Path("~/.codex/skills"),
    "claude": Path("~/.claude/skills"),
    "cursor": Path("~/.cursor/skills"),
    "opencode": Path("~/.config/opencode/skills"),
}

LOCAL_TARGETS = {
    "codex": Path(".codex/skills"),
    "claude": Path(".claude/skills"),
    "cursor": Path(".cursor/skills"),
    "opencode": Path(".opencode/skills"),
}

skills_cli = typer_factory(help="Manage skills for AI assistants.")


def _format_params(cmd) -> str:
    """Format required params for a command as uppercase placeholders."""
    parts = []
    for p in cmd.params:
        if p.required and not p.name.startswith("_") and p.human_readable_name != "--help":
            parts.append(p.human_readable_name)
    return " ".join(parts)


def build_skill_md() -> str:
    # Lazy import to avoid circular dependency (hf.py imports skills_cli from this module)
    from huggingface_hub import __version__
    from huggingface_hub.cli.hf import app

    click_app = get_command(app)
    ctx = Context(click_app, info_name="hf")

    # wrap in list to widen list[LiteralString] -> list[str] for `ty``
    lines: list[str] = list(_SKILL_YAML_PREFIX.splitlines())
    lines.append("")
    lines.append(f"Generated with `huggingface_hub v{__version__}`. Run `hf skills add --force` to regenerate.")
    lines.append("")
    lines.append("## Commands")
    lines.append("")

    top_level = []
    groups = []
    for name in sorted(click_app.list_commands(ctx)):  # type: ignore[attr-defined]
        cmd = click_app.get_command(ctx, name)  # type: ignore[attr-defined]
        if cmd is None or cmd.hidden:
            continue
        if isinstance(cmd, Group):
            groups.append((name, cmd))
        else:
            top_level.append((name, cmd))

    for name, cmd in top_level:
        help_text = (cmd.help or "").split("\n")[0].strip()
        params = _format_params(cmd)
        parts = ["hf", name] + ([params] if params else [])
        lines.append(f"- `{' '.join(parts)}` — {help_text}")

    for name, cmd in groups:
        help_text = (cmd.help or "").split("\n")[0].strip()
        lines.append("")
        lines.append(f"### `hf {name}` — {help_text}")
        lines.append("")
        sub_ctx = Context(cmd, parent=ctx, info_name=name)
        for sub_name in cmd.list_commands(sub_ctx):
            sub_cmd = cmd.get_command(sub_ctx, sub_name)
            if sub_cmd is None or sub_cmd.hidden:
                continue
            sub_help = (sub_cmd.help or "").split("\n")[0].strip()
            params = _format_params(sub_cmd)
            parts = ["hf", name, sub_name] + ([params] if params else [])
            lines.append(f"- `{' '.join(parts)}` — {sub_help}")

    lines.extend(_SKILL_TIPS.splitlines())

    return "\n".join(lines)


def _remove_existing(path: Path, force: bool) -> None:
    """Remove existing file/directory/symlink if force is True, otherwise raise an error."""
    if not (path.exists() or path.is_symlink()):
        return
    if not force:
        raise SystemExit(f"Skill already exists at {path}.\nRe-run with --force to overwrite.")
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _install_to(skills_dir: Path, force: bool) -> Path:
    """Download and install the skill files into a skills directory. Returns the installed path."""
    skills_dir = skills_dir.expanduser().resolve()
    skills_dir.mkdir(parents=True, exist_ok=True)
    dest = skills_dir / DEFAULT_SKILL_ID

    _remove_existing(dest, force)
    dest.mkdir()

    (dest / "SKILL.md").write_text(build_skill_md(), encoding="utf-8")

    return dest


def _create_symlink(agent_skills_dir: Path, central_skill_path: Path, force: bool) -> Path:
    """Create a relative symlink from agent directory to the central skill location."""
    agent_skills_dir = agent_skills_dir.expanduser().resolve()
    agent_skills_dir.mkdir(parents=True, exist_ok=True)
    link_path = agent_skills_dir / DEFAULT_SKILL_ID

    _remove_existing(link_path, force)
    link_path.symlink_to(os.path.relpath(central_skill_path, agent_skills_dir))

    return link_path


@skills_cli.command(
    "add",
    examples=[
        "hf skills add --claude",
        "hf skills add --cursor",
        "hf skills add --claude --global",
        "hf skills add --codex --opencode --cursor",
    ],
)
def skills_add(
    claude: Annotated[bool, typer.Option("--claude", help="Install for Claude.")] = False,
    codex: Annotated[bool, typer.Option("--codex", help="Install for Codex.")] = False,
    cursor: Annotated[bool, typer.Option("--cursor", help="Install for Cursor.")] = False,
    opencode: Annotated[bool, typer.Option("--opencode", help="Install for OpenCode.")] = False,
    global_: Annotated[
        bool,
        typer.Option(
            "--global",
            "-g",
            help="Install globally (user-level) instead of in the current project directory.",
        ),
    ] = False,
    dest: Annotated[
        Optional[Path],
        typer.Option(
            help="Install into a custom destination (path to skills directory).",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite existing skills in the destination.",
        ),
    ] = False,
) -> None:
    """Download a skill and install it for an AI assistant."""
    if not (claude or codex or cursor or opencode or dest):
        raise CLIError("Pick a destination via --claude, --codex, --cursor, --opencode, or --dest.")

    if dest:
        if claude or codex or cursor or opencode or global_:
            print("--dest cannot be combined with --claude, --codex, --cursor, --opencode, or --global.")
            raise typer.Exit(code=1)
        skill_dest = _install_to(dest, force)
        print(f"Installed '{DEFAULT_SKILL_ID}' to {skill_dest}")
        return

    targets_dict = GLOBAL_TARGETS if global_ else LOCAL_TARGETS
    agent_targets: list[Path] = []
    if claude:
        agent_targets.append(targets_dict["claude"])
    if codex:
        agent_targets.append(targets_dict["codex"])
    if cursor:
        agent_targets.append(targets_dict["cursor"])
    if opencode:
        agent_targets.append(targets_dict["opencode"])

    central_path = CENTRAL_GLOBAL if global_ else CENTRAL_LOCAL
    central_skill_path = _install_to(central_path, force)
    print(f"Installed '{DEFAULT_SKILL_ID}' to central location: {central_skill_path}")

    for agent_target in agent_targets:
        link_path = _create_symlink(agent_target, central_skill_path, force)
        print(f"Created symlink: {link_path}")
