"""Deliver an unchanged target to its agent, locally or over the existing HTTP API.

Transport knows neither the failure budget nor how a host/process fault is applied.
An accepted command is not evidence that the action has completed or YDB recovered.
"""

from __future__ import annotations

from typing import Callable

import requests

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand


class AgentCommandTransport:
    def __init__(
        self,
        *,
        is_local_host: Callable[[str], bool],
        start_local: Callable,
        get_app_port: Callable[[], int],
        resolve_http_host: Callable[[str], str],
        post: Callable = requests.post,
        result_local: Callable | None = None,
        resume_remote: bool = False,
        get: Callable = requests.get,
    ) -> None:
        self._is_local_host = is_local_host
        self._start_local = start_local
        self._get_app_port = get_app_port
        self._resolve_http_host = resolve_http_host
        self._post = post
        self._get = get
        self._resume_remote = resume_remote
        self._resumed = set()
        self._result_local = result_local

    def send(self, cmd: DispatchCommand) -> None:
        """Return on acceptance; raise on rejection/delivery failure. Never retry.

        A timeout can occur after acceptance; callers must not infer that the
        target is untouched merely because delivery failed.
        """
        payload = dict(cmd.payload or {})
        payload["host"] = cmd.host
        payload["chaos_target"] = cmd.target.to_dict()
        if self._is_local_host(cmd.host):
            result = self._start_local(cmd.nemesis_type, cmd.action, payload=payload)
        else:
            if cmd.action == "inject" and self._resume_remote and cmd.host not in self._resumed:
                response = self._post(
                    f"http://{self._resolve_http_host(cmd.host)}:{self._get_app_port()}/api/agent/resume",
                    timeout=5,
                )
                response.raise_for_status()
                if response.json().get("status") != "ok":
                    raise RuntimeError("Agent did not resume")
                self._resumed.add(cmd.host)
            response = self._post(
                f"http://{self._resolve_http_host(cmd.host)}:{self._get_app_port()}/api/processes",
                json={
                    "type": cmd.nemesis_type,
                    "action": cmd.action,
                    "payload": payload,
                    "target": cmd.target.to_dict(),
                },
                timeout=5,
            )
            response.raise_for_status()
            result = response.json()
        if not isinstance(result, dict) or result.get("status") != "started":
            raise RuntimeError(f"Agent did not accept {cmd.nemesis_type} on {cmd.host}: {result!r}")

        if cmd.action == "extract":
            execution = result.get("execution")
            if not execution:
                raise RuntimeError("Agent did not provide an execution receipt")

            def completion():
                if self._is_local_host(cmd.host):
                    return self._result_local(execution)["status"]
                response = self._get(
                    f"http://{self._resolve_http_host(cmd.host)}:{self._get_app_port()}/api/executions/{execution}",
                    timeout=5,
                )
                response.raise_for_status()
                return response.json()["status"]
            return completion
