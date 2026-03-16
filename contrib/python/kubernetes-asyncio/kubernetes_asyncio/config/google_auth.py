import asyncio.subprocess
import json
import shlex
from types import SimpleNamespace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kubernetes_asyncio.config.kube_config import ConfigNode


async def google_auth_credentials(provider: "ConfigNode") -> SimpleNamespace:
    if "cmd-path" not in provider or "cmd-args" not in provider:
        raise ValueError(
            "GoogleAuth via gcloud is supported! Values for cmd-path, cmd-args are required."
        )
    cmd_args = provider["cmd-args"]
    cmd_path = provider["cmd-path"]
    if not isinstance(cmd_args, str) or not isinstance(cmd_path, str):
        raise ValueError(
            "GoogleAuth via gcloud is supported! Values for cmd-path, cmd-args have to be strings."
        )

    cmd_args_splited = shlex.split(cmd_args)
    cmd_exec = asyncio.create_subprocess_exec(
        cmd_path,
        *cmd_args_splited,
        stdin=None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    proc = await cmd_exec

    if proc.stdout:
        raw_data = await proc.stdout.read()
    else:
        raise RuntimeError("Unable to read stdout")
    data = json.loads(raw_data.decode("ascii").rstrip())

    await proc.wait()
    return SimpleNamespace(
        token=data["credential"]["access_token"],
        expiry=data["credential"]["token_expiry"],
    )
