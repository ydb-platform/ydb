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
    ) -> None:
        self._is_local_host = is_local_host
        self._start_local = start_local
        self._get_app_port = get_app_port
        self._resolve_http_host = resolve_http_host
        self._post = post

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
