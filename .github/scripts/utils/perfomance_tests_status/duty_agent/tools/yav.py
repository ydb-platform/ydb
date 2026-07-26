"""Load tokens from YAV via ``ya vault`` (same pattern as arc_import_duty/prci.py).

Only ``init-token`` should call YAV. Normal investigate uses env vars.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOKEN_CONFIG = ROOT / "token_config.json"
TOKEN_CONFIG_ENV = "DUTY_TOKEN_CONFIG"

# Env vars accepted for sandbox OAuth (first wins).
SANDBOX_TOKEN_ENVS = ("SANDBOX_TOKEN", "YA_TOKEN")


class YavError(RuntimeError):
    """YAV / token config failure."""


def token_config_path(override: Path | str | None = None) -> Path:
    if override:
        return Path(override).expanduser()
    env = os.environ.get(TOKEN_CONFIG_ENV)
    if env:
        return Path(env).expanduser()
    return DEFAULT_TOKEN_CONFIG


def read_token_config(path: Path | None = None) -> dict[str, Any]:
    p = path or token_config_path()
    if not p.is_file():
        return {"tokens": {}}
    with p.open(encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {"tokens": {}}


def token_specs_from_config(config: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """Return {ENV_NAME: (secret_id, key)} from current or legacy config."""
    default_secret_id = str(config.get("secret_id") or "")
    specs: dict[str, tuple[str, str]] = {}
    for env_name, spec in (config.get("tokens") or {}).items():
        if isinstance(spec, str):
            secret_id = default_secret_id
            yav_key = spec
        elif isinstance(spec, dict):
            secret_id = str(spec.get("secret_id") or default_secret_id)
            yav_key = str(spec.get("key") or "")
        else:
            continue
        if secret_id and yav_key:
            specs[str(env_name)] = (secret_id, yav_key)
    return specs


def fetch_tokens_from_yav(config_path: Path | None = None) -> dict[str, str]:
    """Fetch configured tokens from YAV. Called only by init-token."""
    path = config_path or token_config_path()
    config = read_token_config(path)
    tokens: dict[str, str] = {}
    for env_name, (secret_id, yav_key) in token_specs_from_config(config).items():
        # Existing env wins — do not overwrite / re-fetch.
        if os.environ.get(env_name):
            tokens[env_name] = os.environ[env_name]
            continue
        proc = subprocess.run(
            ["ya", "vault", "get", "version", secret_id, "-o", yav_key],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise YavError(
                f"Failed to load {env_name} from YAV secret {secret_id} key {yav_key}:\n"
                f"{(proc.stderr or proc.stdout or '').strip()}"
            )
        value = (proc.stdout or "").strip()
        if value:
            tokens[env_name] = value
    return tokens


def sandbox_oauth_token() -> str | None:
    """Return OAuth token for proxy.sandbox from env (no YAV call)."""
    for name in SANDBOX_TOKEN_ENVS:
        v = os.environ.get(name)
        if v and v.strip():
            return v.strip()
    # Optional local ya token file (same as ya bootstrap).
    ya_path = os.environ.get("YA_TOKEN_PATH") or str(Path.home() / ".ya_token")
    try:
        p = Path(ya_path)
        if p.is_file():
            t = p.read_text(encoding="utf-8").strip()
            if t:
                return t
    except OSError:
        pass
    return None


def cmd_init_token(
    *,
    config_path: Path | None = None,
    shell_exports: bool = False,
) -> int:
    """Load tokens from YAV into a subshell, or print export lines for eval."""
    path = config_path or token_config_path()
    try:
        tokens = fetch_tokens_from_yav(path)
    except YavError as e:
        print(str(e), file=sys.stderr)
        return 1
    if not tokens:
        print(f"No tokens configured in {path}", file=sys.stderr)
        return 1

    if shell_exports:
        if sys.stdout.isatty():
            print(
                "Refusing to print token values to an interactive terminal. "
                "Use: eval \"$(python3 run.py init-token --shell)\"",
                file=sys.stderr,
            )
            return 1
        for env_name in sorted(tokens):
            print(f"export {env_name}={shlex.quote(tokens[env_name])}")
        print(
            f"# Loaded from YAV config {path}: {', '.join(sorted(tokens))}",
            file=sys.stderr,
        )
        return 0

    env = dict(os.environ)
    env.update(tokens)
    shell = os.environ.get("SHELL") or "/bin/bash"
    print(
        f"Loaded tokens from YAV config: {', '.join(sorted(tokens))}. "
        f"Starting {shell}; run `exit` to return.",
        file=sys.stderr,
    )
    os.execvpe(shell, [shell, "-i"], env)
    return 0  # unreachable
