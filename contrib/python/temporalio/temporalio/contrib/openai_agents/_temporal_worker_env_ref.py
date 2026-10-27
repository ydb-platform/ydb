"""Worker-environment secrets: the reference form, and the allowlist both forms share."""

from __future__ import annotations

import dataclasses
import os
import re
from collections.abc import Collection, Mapping, MutableMapping
from typing import Any, cast

from agents.tool import ShellToolContainerAutoEnvironment, ShellToolEnvironment
from openai.types.responses.tool_param import CodeInterpreter, Mcp

_REF_PREFIX = "temporal.worker_env_ref:"

_REF_PATTERN = re.compile(re.escape(_REF_PREFIX) + r"\{([^}{]*)\}")


@dataclasses.dataclass(frozen=True)
class AllowAllWorkerEnvVars:
    """Make every environment variable on the worker resolvable.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    Pass an instance in place of a list of names::

        OpenAIAgentsPlugin(resolvable_worker_env_vars=AllowAllWorkerEnvVars())

    This grants far more on the sandbox form than on the hosted tool form. A
    sandbox manifest is written in workflow code, so allowing every name lets a
    workflow name any variable on the worker and have its value set inside the
    sandbox container, where a shell command the model composes can read it.
    """


def _snapshot_resolvable_env_vars(
    resolvable_worker_env_vars: Collection[str] | AllowAllWorkerEnvVars,
) -> frozenset[str] | AllowAllWorkerEnvVars:
    if isinstance(resolvable_worker_env_vars, AllowAllWorkerEnvVars):
        return resolvable_worker_env_vars
    if isinstance(resolvable_worker_env_vars, str):
        raise TypeError(
            "resolvable_worker_env_vars takes a collection of environment variable "
            'names, such as ["MY_MCP_TOKEN"], or AllowAllWorkerEnvVars(). A single '
            "string is read as the collection of its characters, so pass a list even "
            "for one name."
        )
    if cast(object, resolvable_worker_env_vars) is AllowAllWorkerEnvVars:
        raise TypeError(
            "resolvable_worker_env_vars takes an AllowAllWorkerEnvVars instance, not "
            "the class itself. Pass AllowAllWorkerEnvVars()."
        )
    return frozenset(resolvable_worker_env_vars)


def _is_resolvable(
    resolvable: frozenset[str] | AllowAllWorkerEnvVars, name: str
) -> bool:
    return isinstance(resolvable, AllowAllWorkerEnvVars) or name in resolvable


def temporal_worker_env_ref(name: str) -> str:
    """Refer to a secret held in the Temporal Worker's environment.

    .. warning::
        This function is experimental and may change in future versions.
        Use with caution in production environments.

    Use it for a hosted tool credential that should come from the worker's
    environment rather than being written into your workflow. Put the returned
    reference in a ``HostedMCPTool``'s ``authorization`` or header value, or in
    the ``value`` of a ``domain_secrets`` entry under a ``ShellTool`` or
    ``CodeInterpreterTool``. The reference carries only the variable's name,
    never its value.

    Every worker that runs model activities must set the variable and name it in
    ``OpenAIAgentsPlugin(resolvable_worker_env_vars=[...])``. A name the worker
    allows but has not set resolves to an empty value.

    Args:
        name: Name of the environment variable to read on the worker.

    Returns:
        A reference string to use in place of the secret.
    """
    return f"{_REF_PREFIX}{{{name}}}"


class _WorkerEnvRefResolver:  # type:ignore[reportUnusedClass]
    def __init__(
        self,
        resolvable_worker_env_vars: Collection[str] | AllowAllWorkerEnvVars,
    ) -> None:
        self._allowed = _snapshot_resolvable_env_vars(resolvable_worker_env_vars)

    def _resolve_ref(self, value: str) -> str:
        def substitute(match: re.Match[str]) -> str:
            name = match.group(1)
            if not _is_resolvable(self._allowed, name):
                return match.group(0)
            return os.environ.get(name, "")

        return _REF_PATTERN.sub(substitute, value)

    def _resolve_domain_secret(self, secret: Mapping[str, Any]) -> dict[str, Any]:
        return {**secret, "value": self._resolve_ref(secret["value"])}

    def _resolve_network_policy(self, network_policy: Any) -> Any:
        policy = cast(MutableMapping[str, Any], network_policy)
        domain_secrets = policy.get("domain_secrets")
        if domain_secrets is None:
            return network_policy
        unresolved = list(domain_secrets)
        # On the code interpreter path pydantic deserializes domain_secrets into a
        # single-pass iterator, so the entries read here go back onto the input.
        policy["domain_secrets"] = unresolved
        return {
            **policy,
            "domain_secrets": [
                self._resolve_domain_secret(secret) for secret in unresolved
            ],
        }

    def resolve_mcp_tool_config(self, tool_config: Mcp) -> Mcp:
        resolved: Mcp = tool_config
        if "authorization" in resolved:
            resolved = {
                **resolved,
                "authorization": self._resolve_ref(resolved["authorization"]),
            }
        headers = resolved.get("headers")
        if headers is not None:
            resolved = {
                **resolved,
                "headers": {
                    name: self._resolve_ref(value) for name, value in headers.items()
                },
            }
        return resolved

    def resolve_shell_tool_environment(
        self,
        environment: ShellToolEnvironment | None,
    ) -> ShellToolEnvironment:
        """An absent environment comes back as the local one ``ShellTool`` normalizes it to."""
        if environment is None:
            return {"type": "local"}
        if environment.get("type") != "container_auto":
            return environment
        auto = cast(ShellToolContainerAutoEnvironment, environment)
        network_policy = auto.get("network_policy")
        if network_policy is None:
            return environment
        return {
            **auto,
            "network_policy": self._resolve_network_policy(network_policy),
        }

    def resolve_code_interpreter_tool_config(
        self,
        tool_config: CodeInterpreter,
    ) -> CodeInterpreter:
        container = tool_config.get("container")
        if not isinstance(container, Mapping):
            return tool_config
        network_policy = container.get("network_policy")
        if network_policy is None:
            return tool_config
        return {
            **tool_config,
            "container": {
                **container,
                "network_policy": self._resolve_network_policy(network_policy),
            },
        }
