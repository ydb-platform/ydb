from __future__ import annotations

import json
from typing import (
    Any,
    AsyncIterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

from .multiplexed import multiplexed_result_list, multiplexed_result_stream
from .utils import (
    clean_filters,
    clean_map,
    clean_networks,
    compose_auth_header,
    format_env,
)


class DockerServices:
    def __init__(self, docker):
        self.docker = docker

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, str | Sequence[str]]] = None,
    ) -> List[Mapping]:
        """
        Return a list of services

        Args:
            filters: a dict with a list of filters

        Available filters:
            id=<service id>
            label=<service label>
            mode=["replicated"|"global"]
            name=<service name>
        """

        params = {"filters": clean_filters(filters)}

        response = await self.docker._query_json(
            "services", method="GET", params=params
        )
        return response

    async def create(
        self,
        task_template: Mapping[str, Any],
        *,
        name: Optional[str] = None,
        labels: Optional[Mapping[str, str]] = None,
        mode: Optional[Mapping] = None,
        update_config: Optional[Mapping] = None,
        rollback_config: Optional[Mapping] = None,
        networks: Optional[List] = None,
        endpoint_spec: Optional[Mapping] = None,
        auth: Optional[Union[MutableMapping, str, bytes]] = None,
        registry: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """
        Create a service

        Args:
            task_template: user modifiable task configuration
            name: name of the service
            labels: user-defined key/value metadata
            mode: scheduling mode for the service
            update_config: update strategy of the service
            rollback_config: rollback strategy of the service
            networks: array of network names/IDs to attach the service to
            endpoint_spec: ports to expose
            auth: authentication information, can be a string, dict or bytes
            registry: used when auth is specified, it provides domain/IP of
                the registry without a protocol

        Returns:
            a dict with info of the created service
        """

        if "Image" not in task_template["ContainerSpec"]:
            raise KeyError("Missing mandatory Image key in ContainerSpec")

        if auth and registry is None:
            raise KeyError(
                "When auth is specified you need to specify also the registry"
            )

        # from {"key":"value"} to ["key=value"]
        if "Env" in task_template["ContainerSpec"]:
            task_template["ContainerSpec"]["Env"] = [
                format_env(k, v)
                for k, v in task_template["ContainerSpec"]["Env"].items()
            ]

        headers = None
        if auth:
            headers = {"X-Registry-Auth": compose_auth_header(auth, registry)}

        config = {
            "TaskTemplate": task_template,
            "Name": name,
            "Labels": labels,
            "Mode": mode,
            "UpdateConfig": update_config,
            "RollbackConfig": rollback_config,
            "Networks": clean_networks(networks),
            "EndpointSpec": endpoint_spec,
        }

        data = json.dumps(clean_map(config))

        response = await self.docker._query_json(
            "services/create", method="POST", data=data, headers=headers
        )
        return response

    async def update(
        self,
        service_id: str,
        version: str,
        *,
        image: Optional[str] = None,
        rollback: bool = False,
    ) -> bool:
        """
        Update a service.
        If rollback is True image will be ignored.

        Args:
            service_id: ID or name of the service.
            version: Version of the service that you want to update.
            rollback: Rollback the service to the previous service spec.

        Returns:
            True if successful.
        """
        if image is None and rollback is False:
            raise ValueError("You need to specify an image.")

        inspect_service = await self.inspect(service_id)
        spec = inspect_service["Spec"]

        if image is not None:
            spec["TaskTemplate"]["ContainerSpec"]["Image"] = image

        params = {"version": version}
        if rollback is True:
            params["rollback"] = "previous"

        data = json.dumps(clean_map(spec))

        await self.docker._query_json(
            f"services/{service_id}/update",
            method="POST",
            data=data,
            params=params,
        )
        return True

    async def delete(self, service_id: str) -> bool:
        """
        Remove a service

        Args:
            service_id: ID or name of the service

        Returns:
            True if successful
        """

        async with self.docker._query(f"services/{service_id}", method="DELETE"):
            return True

    async def inspect(self, service_id: str) -> Mapping[str, Any]:
        """
        Inspect a service

        Args:
            service_id: ID or name of the service

        Returns:
            a dict with info about a service
        """

        response = await self.docker._query_json(f"services/{service_id}", method="GET")
        return response

    def logs(
        self,
        service_id: str,
        *,
        details: bool = False,
        follow: bool = False,
        stdout: bool = False,
        stderr: bool = False,
        since: int = 0,
        timestamps: bool = False,
        is_tty: bool = False,
        tail: str = "all",
    ) -> Union[str, AsyncIterator[str]]:
        """
        Retrieve logs of the given service

        Args:
            details: show service context and extra details provided to logs
            follow: return the logs as a stream.
            stdout: return logs from stdout
            stderr: return logs from stderr
            since: return logs since this time, as a UNIX timestamp
            timestamps: add timestamps to every log line
            is_tty: the service has a pseudo-TTY allocated
            tail: only return this number of log lines
                  from the end of the logs, specify as an integer
                  or `all` to output all log lines.


        """
        if stdout is False and stderr is False:
            raise TypeError("Need one of stdout or stderr")

        params = {
            "details": details,
            "follow": follow,
            "stdout": stdout,
            "stderr": stderr,
            "since": since,
            "timestamps": timestamps,
            "tail": tail,
        }
        cm = self.docker._query(
            f"services/{service_id}/logs",
            method="GET",
            params=params,
        )
        if follow:
            return self._logs_stream(cm, is_tty)
        else:
            return self._logs_list(cm, is_tty)

    async def _logs_stream(self, cm, is_tty):
        async with cm as response:
            async for item in multiplexed_result_stream(response, is_tty=is_tty):
                yield item

    async def _logs_list(self, cm, is_tty):
        async with cm as response:
            return await multiplexed_result_list(response, is_tty=is_tty)
