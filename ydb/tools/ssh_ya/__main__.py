#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    host: str
    repo_path: str
    branch: str
    ssh_args: list[str]
    dry_run: bool
    verbose: bool
    sync: bool


def config_path() -> str:
    base = os.environ.get("XDG_CONFIG_HOME")
    if not base:
        base = os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(base, "ssh_ya", "config.json")


def load_saved_host() -> str | None:
    path = config_path()
    try:
        with open(path) as stream:
            data = json.load(stream)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as error:
        fail(f"Invalid ssh_ya config at {path}: {error}")

    host = data.get("host")
    if host is None:
        return None
    if not isinstance(host, str):
        fail(f"Invalid ssh_ya config at {path}: 'host' must be a string.")
    host = host.strip()
    return host or None


def save_host(host: str) -> None:
    path = config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as stream:
        json.dump({"host": host}, stream, indent=2, sort_keys=True)
        stream.write("\n")


def prompt_host() -> str:
    try:
        host = input("ssh_ya host: ").strip()
    except EOFError:
        fail("Host is not configured. Pass --host once or run interactively to save it.")

    if not host:
        fail("Host must not be empty.")

    save_host(host)
    return host


def resolve_host(explicit_host: str | None) -> str:
    if explicit_host:
        host = explicit_host.strip()
        if not host:
            fail("Host must not be empty.")
        save_host(host)
        return host

    saved_host = load_saved_host()
    if saved_host:
        return saved_host

    return prompt_host()


def run_local(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    if capture_output:
        return subprocess.run(
            command,
            env=env,
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    return subprocess.run(command, env=env, check=check, text=True)


def local_output(command: list[str], *, env: dict[str, str] | None = None) -> str:
    return run_local(command, env=env, capture_output=True).stdout.strip()


def fail(message: str) -> None:
    raise SystemExit(message)


def status(message: str) -> None:
    print(message, file=sys.stderr)


def current_branch(explicit_branch: str | None) -> str:
    if explicit_branch:
        return explicit_branch

    result = subprocess.run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        fail("Detached HEAD is not supported, pass --branch explicitly.")
    return result.stdout.strip()


def local_origin_url() -> str | None:
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        return None
    origin = result.stdout.strip()
    return origin or None


def git_ssh_command(ssh_args: list[str]) -> str | None:
    if not ssh_args:
        return None
    return shlex.join(["ssh", *ssh_args])


def git_env(ssh_args: list[str]) -> dict[str, str]:
    env = os.environ.copy()
    ssh_command = git_ssh_command(ssh_args)
    if ssh_command:
        env["GIT_SSH_COMMAND"] = ssh_command
    return env


def shell_join(command: list[str]) -> str:
    return shlex.join(command)


def remote_path_setup(repo_path: str) -> str:
    quoted_repo_path = shlex.quote(repo_path)
    return "\n".join(
        [
            f"repo_path_literal={quoted_repo_path}",
            'case "$repo_path_literal" in',
            '    "~") repo_path="$HOME" ;;',
            '    "~/"*) repo_path="$HOME${repo_path_literal#\\~}" ;;',
            '    *) repo_path="$repo_path_literal" ;;',
            "esac",
        ]
    )


def run_remote(
    host: str,
    ssh_args: list[str],
    script: str,
    *,
    dry_run: bool,
    verbose: bool,
    capture_output: bool = False,
) -> str | None:
    remote_command = f"sh -lc {shlex.quote(script)}"
    command = ["ssh", *ssh_args, host, remote_command]
    if dry_run or verbose:
        print("+", shell_join(command), file=sys.stderr)
    if dry_run:
        return ""
    if capture_output:
        return run_local(command, capture_output=True).stdout
    run_local(command)
    return None


def ensure_remote_repo_and_get_branch_commit(config: Config, origin_url: str | None) -> str | None:
    origin_lines: list[str] = []
    if origin_url:
        quoted_origin = shlex.quote(origin_url)
        origin_lines = [
            'if git -C "$repo_path" remote get-url origin >/dev/null 2>&1; then',
            f'    git -C "$repo_path" remote set-url origin {quoted_origin}',
            "else",
            f'    git -C "$repo_path" remote add origin {quoted_origin}',
            "fi",
        ]

    script = "\n".join(
        [
            "set -eu",
            remote_path_setup(config.repo_path),
            'mkdir -p "$(dirname -- "$repo_path")"',
            'if [ ! -d "$repo_path/.git" ]; then',
            '    git init "$repo_path" >/dev/null 2>&1',
            "fi",
            'git -C "$repo_path" config receive.denyCurrentBranch updateInstead',
            f'git -C "$repo_path" symbolic-ref HEAD {shlex.quote(f"refs/heads/{config.branch}")}',
            *origin_lines,
            f'git -C "$repo_path" rev-parse -q --verify {shlex.quote(f"refs/heads/{config.branch}")} 2>/dev/null || true',
        ]
    )
    output = run_remote(
        config.host,
        config.ssh_args,
        script,
        dry_run=config.dry_run,
        verbose=config.verbose,
        capture_output=True,
    )
    if output is None:
        return None
    branch_commit = output.strip()
    return branch_commit or None


def working_tree_has_tracked_changes() -> bool:
    result = run_local(
        ["git", "diff-index", "--quiet", "HEAD", "--"],
        capture_output=True,
        check=False,
    )
    if result.returncode not in (0, 1):
        fail(result.stderr.strip() or "Failed to inspect tracked changes.")
    return result.returncode == 1


def working_tree_has_untracked_files() -> bool:
    output = local_output(["git", "ls-files", "--others", "--exclude-standard"])
    return bool(output)


def target_commit(branch: str) -> str:
    head = local_output(["git", "rev-parse", "HEAD"])
    if not working_tree_has_tracked_changes() and not working_tree_has_untracked_files():
        return head
    return snapshot_commit(branch)


def snapshot_commit(branch: str) -> str:
    head = local_output(["git", "rev-parse", "HEAD"])
    head_tree = local_output(["git", "rev-parse", f"{head}^{{tree}}"])

    with tempfile.NamedTemporaryFile(prefix="ssh_ya_index_") as temp_index:
        env = os.environ.copy()
        env["GIT_INDEX_FILE"] = temp_index.name

        run_local(["git", "read-tree", head], env=env)
        run_local(["git", "add", "-A", ":/"], env=env)
        tree = local_output(["git", "write-tree"], env=env)

    if tree == head_tree:
        return head

    return local_output(
        [
            "git",
            "commit-tree",
            tree,
            "-p",
            head,
            "-m",
            f"ssh_ya snapshot for {branch}",
        ]
    )

def push_snapshot(config: Config, commit: str) -> None:
    target = f"{config.host}:{config.repo_path}"
    command = ["git", "push", "--force", target, f"{commit}:refs/heads/{config.branch}"]
    env = git_env(config.ssh_args)

    if config.dry_run or config.verbose:
        print("+", shell_join(command), file=sys.stderr)
    if config.dry_run:
        return
    run_local(command, env=env)


def sync_repo(config: Config) -> None:
    commit = target_commit(config.branch)
    remote_commit = ensure_remote_repo_and_get_branch_commit(config, local_origin_url())
    if not config.dry_run and remote_commit == commit:
        return
    status(f"Synchronizing {config.branch} to {config.host}:{config.repo_path}")
    push_snapshot(config, commit)


def run_in_remote_repo(config: Config, body: str) -> None:
    script = "\n".join(
        [
            "set -eu",
            remote_path_setup(config.repo_path),
            'cd "$repo_path"',
            body,
        ]
    )
    run_remote(config.host, config.ssh_args, script, dry_run=config.dry_run, verbose=config.verbose)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync the current branch with local changes to a remote checkout and "
            "run commands there."
        )
    )
    parser.add_argument("--host", "-H", help="Override the saved remote host and store it in config.")
    parser.add_argument("--repo-path", default="~/ydb")
    parser.add_argument("--branch")
    parser.add_argument(
        "--ssh-arg",
        action="append",
        default=[],
        help="Extra raw argument for ssh, may be specified multiple times.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true", help="Print ssh/git commands being executed.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="Only synchronize the remote repository.")
    sync_parser.set_defaults(sync=True)

    execute_parser = subparsers.add_parser(
        "execute",
        help="Run an arbitrary shell command in the remote checkout.",
    )
    execute_parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Run without synchronizing the repository first.",
    )
    execute_parser.set_defaults(sync=True)

    ya_parser = subparsers.add_parser("ya", help="Run ./ya in the remote checkout.")
    ya_parser.set_defaults(sync=True)

    make_parser = subparsers.add_parser(
        "make",
        help="Run './ya make --build relwithdebinfo ...' in the remote checkout.",
    )
    make_parser.set_defaults(sync=True)

    test_parser = subparsers.add_parser(
        "test",
        help="Run './ya make --build relwithdebinfo -tA ... 2>&1 | tail' remotely.",
    )
    test_parser.set_defaults(sync=True)

    arguments, extras = parser.parse_known_args()

    if arguments.command == "sync":
        if extras:
            parser.error(f"unrecognized arguments: {' '.join(extras)}")
        return arguments

    if arguments.command == "execute":
        arguments.remote_command = extras
        return arguments

    if arguments.command == "ya":
        arguments.ya_args = extras
        return arguments

    if arguments.command == "make":
        arguments.make_args = extras
        return arguments

    if arguments.command == "test":
        arguments.test_args = extras
        return arguments

    return arguments


def build_config(arguments: argparse.Namespace) -> Config:
    return Config(
        host=resolve_host(arguments.host),
        repo_path=arguments.repo_path,
        branch=current_branch(arguments.branch),
        ssh_args=arguments.ssh_arg,
        dry_run=arguments.dry_run,
        verbose=arguments.verbose,
        sync=getattr(arguments, "sync", True) and not getattr(arguments, "skip_sync", False),
    )


def command_body(arguments: argparse.Namespace) -> str | None:
    if arguments.command == "sync":
        return None

    if arguments.command == "execute":
        if not arguments.remote_command:
            fail("execute requires a command to run.")
        return shlex.join(arguments.remote_command)

    if arguments.command == "ya":
        return shell_join(["./ya", *arguments.ya_args])

    if arguments.command == "make":
        return shell_join(["./ya", "make", "--build", "relwithdebinfo", *arguments.make_args])

    if arguments.command == "test":
        ya_command = shell_join(
            ["./ya", "make", "--build", "relwithdebinfo", "-tA", *arguments.test_args]
        )
        return f"{ya_command} 2>&1 | tail"

    fail(f"Unsupported command: {arguments.command}")


def main() -> int:
    try:
        arguments = parse_args()
        config = build_config(arguments)

        if config.sync:
            sync_repo(config)

        body = command_body(arguments)
        if body is not None:
            run_in_remote_repo(config, body)

        return 0
    except subprocess.CalledProcessError as error:
        if error.stdout:
            print(error.stdout, end="", file=sys.stdout)
        if error.stderr:
            print(error.stderr, end="", file=sys.stderr)
        return error.returncode


if __name__ == "__main__":
    raise SystemExit(main())
