from __future__ import annotations

import json
import shlex
import tarfile
from contextlib import AbstractAsyncContextManager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from aiohttp import ClientResponse, ClientTimeout, ClientWebSocketResponse
from multidict import MultiDict
from yarl import URL

from .exceptions import DockerContainerError, DockerError
from .execs import Exec
from .jsonstream import json_stream_list, json_stream_stream
from .logs import DockerLog
from .multiplexed import multiplexed_result_list, multiplexed_result_stream
from .stream import Stream
from .types import SENTINEL, JSONObject, MutableJSONObject, PortInfo, Sentinel
from .utils import clean_filters, identical, parse_result


if TYPE_CHECKING:
    from .docker import Docker


class DockerContainers:
    def __init__(self, docker: Docker) -> None:
        self.docker = docker

    async def list(self, **kwargs) -> List[DockerContainer]:
        data = await self.docker._query_json(
            "containers/json", method="GET", params=kwargs
        )
        return [DockerContainer(self.docker, **x) for x in data]

    async def create_or_replace(
        self,
        name: str,
        config: JSONObject,
    ) -> DockerContainer:
        container = None

        try:
            container = await self.get(name)
            if not identical(config, container._container):
                running = container._container.get("State", {}).get("Running", False)
                if running:
                    await container.stop()
                await container.delete()
                container = None
        except DockerError:
            pass

        if container is None:
            container = await self.create(config, name=name)

        return container

    async def create(
        self,
        config: JSONObject,
        *,
        name: Optional[str] = None,
    ) -> DockerContainer:
        url = "containers/create"
        encoded_config = json.dumps(config, sort_keys=True).encode("utf-8")
        kwargs = {}
        if name:
            kwargs["name"] = name
        data = await self.docker._query_json(
            url, method="POST", data=encoded_config, params=kwargs
        )
        return DockerContainer(self.docker, id=data["Id"])

    async def run(
        self,
        config: JSONObject,
        *,
        auth: Optional[Union[Mapping, str, bytes]] = None,
        name: Optional[str] = None,
    ) -> DockerContainer:
        """
        Create and start a container.

        If container.start() will raise an error the exception will contain
        a `container_id` attribute with the id of the container.

        Use `auth` for specifying credentials for pulling absent image from
        a private registry.
        """
        try:
            container = await self.create(config, name=name)
        except DockerError as err:
            # image not found, try pulling it
            if err.status == 404 and "Image" in config:
                await self.docker.pull(str(config["Image"]), auth=auth)
                container = await self.create(config, name=name)
            else:
                raise err

        try:
            await container.start()
        except DockerError as err:
            raise DockerContainerError(err.status, err.message, container["id"])

        return container

    async def get(self, container_id: str, **kwargs) -> DockerContainer:
        data = await self.docker._query_json(
            f"containers/{container_id}/json",
            method="GET",
            params=kwargs,
        )
        return DockerContainer(self.docker, **data)

    def container(self, container_id: str, **kwargs) -> DockerContainer:
        data = {"id": container_id}
        data.update(kwargs)
        return DockerContainer(self.docker, **data)

    def exec(self, exec_id: str) -> Exec:
        """Return Exec instance for already created exec object."""
        return Exec(self.docker, exec_id)

    async def prune(
        self,
        *,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Delete stopped containers

        Args:
            filters: Filter expressions to limit which containers are pruned.
                    Available filters:
                    - until: Only remove containers created before given timestamp
                    - label: Only remove containers with (or without, if label!=<key> is used) the specified labels

        Returns:
            Dictionary containing information about deleted containers and space reclaimed
        """
        params = {}
        if filters is not None:
            params["filters"] = clean_filters(filters)

        response = await self.docker._query_json(
            "containers/prune", "POST", params=params
        )
        return response


class DockerContainer:
    _container: Dict[str, Any]
    _id: str

    def __init__(self, docker: Docker, **kwargs) -> None:
        self.docker = docker
        self._container = kwargs
        _id = self._container.get(
            "id", self._container.get("ID", self._container.get("Id"))
        )
        if _id is None:
            raise ValueError(
                "DockerContainer should be initialized with explicit container ID."
            )
        self._id = _id
        self.logs = DockerLog(docker, self)

    @property
    def id(self) -> str:
        return self._id

    @overload
    async def log(
        self,
        *,
        stdout: bool = False,
        stderr: bool = False,
        follow: Literal[False] = False,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
        **kwargs,
    ) -> List[str]: ...

    @overload
    def log(
        self,
        *,
        stdout: bool = False,
        stderr: bool = False,
        follow: Literal[True],
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
        **kwargs,
    ) -> AsyncIterator[str]: ...

    def log(
        self,
        *,
        stdout: bool = False,
        stderr: bool = False,
        follow: bool = False,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
        **kwargs,
    ) -> Any:
        if stdout is False and stderr is False:
            raise TypeError("Need one of stdout or stderr")

        params = {"stdout": stdout, "stderr": stderr, "follow": follow}
        params.update(kwargs)

        # Default to infinite timeout for log operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        cm = self.docker._query(
            f"containers/{self._id}/logs",
            method="GET",
            params=params,
            timeout=timeout_config,
        )
        if follow:
            return self._logs_stream(cm)
        else:
            return self._logs_list(cm)

    async def _logs_stream(
        self, cm: AbstractAsyncContextManager[ClientResponse]
    ) -> AsyncIterator[str]:
        try:
            inspect_info = await self.show()
        except DockerError:
            raise
        is_tty = inspect_info["Config"]["Tty"]

        async with cm as response:
            async for item in multiplexed_result_stream(response, is_tty=is_tty):
                yield item

    async def _logs_list(
        self, cm: AbstractAsyncContextManager[ClientResponse]
    ) -> Sequence[str]:
        try:
            inspect_info = await self.show()
        except DockerError:
            raise
        is_tty = inspect_info["Config"]["Tty"]

        async with cm as response:
            return await multiplexed_result_list(response, is_tty=is_tty)

    async def get_archive(self, path: str) -> tarfile.TarFile:
        async with self.docker._query(
            f"containers/{self._id}/archive",
            method="GET",
            params={"path": path},
        ) as response:
            data = await parse_result(response)
            assert isinstance(data, tarfile.TarFile)
            return data

    async def put_archive(self, path, data):
        async with self.docker._query(
            f"containers/{self._id}/archive",
            method="PUT",
            data=data,
            headers={"content-type": "application/json"},
            params={"path": path},
        ) as response:
            data = await parse_result(response)
            return data

    async def show(self, **kwargs) -> Dict[str, Any]:
        data = await self.docker._query_json(
            f"containers/{self._id}/json", method="GET", params=kwargs
        )
        self._container = data
        return data

    async def stop(
        self,
        *,
        t: int | None = None,
        signal: str | None = None,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> None:
        """Stop the container.

        Args:
            t: Number of seconds to wait for the container to stop before killing it.
                If None, uses the container's configured stop timeout (default 10 seconds).
                This is a Docker API parameter that controls the graceful shutdown period.
            signal: Signal to send to the container (e.g., "SIGTERM", "SIGKILL").
                If None, uses the default SIGTERM signal.
                This parameter may not be supported in older Docker API versions.
            timeout: HTTP request timeout for the stop operation (infinite by default).
                This controls how long to wait for the Docker daemon to respond,
                not the container stop duration.
        """
        params: Dict[str, Any] = {}
        if t is not None:
            params["t"] = t
        if signal is not None:
            params["signal"] = signal

        # Default to infinite timeout for stop operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        async with self.docker._query(
            f"containers/{self._id}/stop",
            method="POST",
            params=params,
            timeout=timeout_config,
        ):
            pass

    async def start(self, **kwargs) -> None:
        async with self.docker._query(
            f"containers/{self._id}/start",
            method="POST",
            headers={"content-type": "application/json"},
            params=kwargs,
        ):
            pass

    async def restart(
        self,
        *,
        t: int | None = None,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> None:
        """Restart the container.

        Args:
            t: Number of seconds to wait for the container to stop before killing it.
                If None, uses the container's configured stop timeout (default 10 seconds).
                This is a Docker API parameter that controls the graceful shutdown period.
            timeout: HTTP request timeout for the restart operation (infinite by default).
                This controls how long to wait for the Docker daemon to respond,
                not the container restart duration.
        """
        params = {}
        if t is not None:
            params["t"] = t

        # Default to infinite timeout for restart operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        async with self.docker._query(
            f"containers/{self._id}/restart",
            method="POST",
            params=params,
            timeout=timeout_config,
        ):
            pass

    async def kill(
        self,
        *,
        signal: str | None = None,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> None:
        """Kill the container by sending a signal.

        Args:
            signal: Signal to send to the container (e.g., "SIGKILL", "SIGTERM", "SIGHUP").
                Can be a signal name (with or without SIG prefix) or a signal number.
                If None, uses the default SIGKILL signal.
            timeout: HTTP request timeout for the kill operation.
        """
        params: Dict[str, Any] = {}
        if signal is not None:
            params["signal"] = signal

        # Use standard timeout resolution
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        async with self.docker._query(
            f"containers/{self._id}/kill",
            method="POST",
            params=params,
            timeout=timeout_config,
        ):
            pass

    async def wait(
        self,
        *,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
        **kwargs,
    ) -> Dict[str, Any]:
        # Default to infinite timeout for wait operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        data = await self.docker._query_json(
            f"containers/{self._id}/wait",
            method="POST",
            params=kwargs,
            timeout=timeout_config,
        )
        return data

    async def delete(
        self,
        *,
        force: bool = False,
        v: bool = False,
        link: bool = False,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> None:
        """Remove the container.

        Args:
            force: If True, kill the container before removing it (using SIGKILL).
                If False, the container must be stopped before it can be removed.
            v: If True, remove anonymous volumes associated with the container.
            link: If True, remove the specified link (legacy networking feature).
            timeout: HTTP request timeout for the delete operation.
        """
        params: Dict[str, Any] = {}
        if force:
            params["force"] = force
        if v:
            params["v"] = v
        if link:
            params["link"] = link

        # Use standard timeout resolution
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        async with self.docker._query(
            f"containers/{self._id}",
            method="DELETE",
            params=params,
            timeout=timeout_config,
        ):
            pass

    async def rename(self, newname) -> None:
        async with self.docker._query(
            f"containers/{self._id}/rename",
            method="POST",
            headers={"content-type": "application/json"},
            params={"name": newname},
        ):
            pass

    async def websocket(self, **params) -> ClientWebSocketResponse:
        if not params:
            params = {"stdin": True, "stdout": True, "stderr": True, "stream": True}
        path = f"containers/{self._id}/attach/ws"
        ws = await self.docker._websocket(path, **params)
        return ws

    def attach(
        self,
        *,
        stdout: bool = False,
        stderr: bool = False,
        stdin: bool = False,
        detach_keys: Optional[str] = None,
        logs: bool = False,
        timeout: ClientTimeout | Sentinel | None = SENTINEL,
    ) -> Stream:
        async def setup() -> Tuple[URL, Optional[bytes], bool]:
            params: MultiDict[Union[str, int]] = MultiDict()
            if detach_keys:
                params.add("detachKeys", detach_keys)
            else:
                params.add("detachKeys", "")
            params.add("logs", int(logs))
            params.add("stdin", int(stdin))
            params.add("stdout", int(stdout))
            params.add("stderr", int(stderr))
            params.add("stream", 1)
            inspect_info = await self.show()
            return (
                URL(f"containers/{self._id}/attach").with_query(params),
                None,
                inspect_info["Config"]["Tty"],
            )

        # Default to infinite timeout for attach operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        return Stream(self.docker, setup, timeout_config)

    async def port(self, private_port: int | str) -> List[PortInfo] | None:
        if "NetworkSettings" not in self._container:
            await self.show()

        private_port = str(private_port)
        h_ports = None

        # Port settings is None when the container is running with
        # network_mode=host.
        port_settings = self._container.get("NetworkSettings", {}).get("Ports")
        if port_settings is None:
            return None

        if "/" in private_port:
            return port_settings.get(private_port)

        h_ports = port_settings.get(private_port + "/tcp")
        if h_ports is None:
            h_ports = port_settings.get(private_port + "/udp")

        return h_ports

    @overload
    def stats(
        self,
        *,
        stream: Literal[True] = True,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> AsyncIterator[Dict[str, Any]]: ...

    @overload
    async def stats(
        self,
        *,
        stream: Literal[False],
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> List[Dict[str, Any]]: ...

    def stats(
        self,
        *,
        stream: bool = True,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> Any:
        # Default to infinite timeout for stats operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        cm = self.docker._query(
            f"containers/{self._id}/stats",
            params={"stream": "1" if stream else "0"},
            timeout=timeout_config,
        )
        if stream:
            return self._stats_stream(cm)
        else:
            return self._stats_list(cm)

    async def _stats_stream(self, cm):
        async with cm as response:
            async for item in json_stream_stream(response):
                yield item

    async def _stats_list(self, cm):
        async with cm as response:
            return await json_stream_list(response)

    async def exec(
        self,
        cmd: Union[str, Sequence[str]],
        stdout: bool = True,
        stderr: bool = True,
        stdin: bool = False,
        tty: bool = False,
        privileged: bool = False,
        user: str = "",  # root by default
        environment: Optional[Union[Mapping[str, str], Sequence[str]]] = None,
        workdir: Optional[str] = None,
        detach_keys: Optional[str] = None,
    ) -> Exec:
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        if environment is None:
            pass
        elif isinstance(environment, Mapping):
            environment = [f"{key}={value}" for key, value in environment.items()]
        else:
            environment = list(environment)
        data = {
            "Container": self._id,
            "Privileged": privileged,
            "Tty": tty,
            "AttachStdin": stdin,
            "AttachStdout": stdout,
            "AttachStderr": stderr,
            "Cmd": cmd,
            "Env": environment,
        }

        if workdir is not None:
            data["WorkingDir"] = workdir
        else:
            data["WorkingDir"] = ""

        if detach_keys:
            data["detachKeys"] = detach_keys
        else:
            data["detachKeys"] = ""

        if user:
            data["User"] = user
        else:
            data["User"] = ""

        data = await self.docker._query_json(
            f"containers/{self._id}/exec", method="POST", data=data
        )
        return Exec(self.docker, data["Id"], tty=tty)

    async def resize(self, *, h: int, w: int) -> None:
        url = URL(f"containers/{self._id}/resize").with_query(h=h, w=w)
        await self.docker._query_json(url, method="POST")

    async def commit(
        self,
        *,
        repository: Optional[str] = None,
        tag: Optional[str] = None,
        message: Optional[str] = None,
        author: Optional[str] = None,
        changes: Optional[Union[str, Sequence[str]]] = None,
        config: Optional[Dict[str, Any]] = None,
        pause: bool = True,
        timeout: float | ClientTimeout | Sentinel | None = SENTINEL,
    ) -> Dict[str, Any]:
        """
        Commit a container to an image. Similar to the ``docker commit``
        command.
        """
        params: MutableJSONObject = {"container": self._id, "pause": pause}
        if repository is not None:
            params["repo"] = repository
        if tag is not None:
            params["tag"] = tag
        if message is not None:
            params["comment"] = message
        if author is not None:
            params["author"] = author
        if changes is not None:
            if not isinstance(changes, str):
                changes = "\n".join(changes)
            params["changes"] = changes

        # Default to infinite timeout for commit operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        return await self.docker._query_json(
            "commit", method="POST", params=params, data=config, timeout=timeout_config
        )

    async def pause(self) -> None:
        async with self.docker._query(f"containers/{self._id}/pause", method="POST"):
            pass

    async def unpause(self) -> None:
        async with self.docker._query(f"containers/{self._id}/unpause", method="POST"):
            pass

    def __getitem__(self, key: str) -> Any:
        return self._container[key]

    def __hasitem__(self, key: str) -> bool:
        return key in self._container
