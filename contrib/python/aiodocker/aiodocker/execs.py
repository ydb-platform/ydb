from __future__ import annotations

import json
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Literal,
    Optional,
    Tuple,
    overload,
)

import aiohttp
from yarl import URL

from .stream import Stream
from .types import SENTINEL, Sentinel


if TYPE_CHECKING:
    from .docker import Docker


# When a Tty is allocated for an "exec" operation, the stdout and stderr are streamed
# straight to the client.
# When a Tty is NOT allocated, then stdout and stderr are multiplexed using the format
# given at
# https://docs.docker.com/engine/api/v1.40/#operation/ContainerAttach under the "Stream
# Format" heading. Note that that documentation is for "docker attach" but the format
# also applies to "docker exec."


class Exec:
    def __init__(self, docker: "Docker", id: str, tty: Optional[bool] = None) -> None:
        self.docker = docker
        self._id = id
        self._tty = tty

    @property
    def id(self) -> str:
        return self._id

    async def inspect(self) -> Dict[str, Any]:
        ret = await self.docker._query_json(f"exec/{self._id}/json")
        assert isinstance(ret["ProcessConfig"], dict)
        self._tty = bool(ret["ProcessConfig"]["tty"])
        return ret

    async def resize(self, *, h: Optional[int] = None, w: Optional[int] = None) -> None:
        dct: Dict[str, int] = {}
        if h is not None:
            dct["h"] = h
        if w is not None:
            dct["w"] = w
        if not dct:
            return
        url = URL(f"exec/{self._id}/resize").with_query(dct)
        async with self.docker._query(url, method="POST"):
            pass

    @overload
    def start(
        self,
        *,
        timeout: float | aiohttp.ClientTimeout | Sentinel | None = SENTINEL,
        detach: Literal[False] = False,
    ) -> Stream:
        pass

    @overload
    async def start(
        self,
        *,
        timeout: float | aiohttp.ClientTimeout | Sentinel | None = SENTINEL,
        detach: Literal[True],
    ) -> bytes:
        pass

    def start(
        self,
        *,
        timeout: float | aiohttp.ClientTimeout | Sentinel | None = SENTINEL,
        detach: bool = False,
    ) -> Any:
        """
        Start this exec instance.

        Args:
            timeout: The timeout for the exec operation (infinite by default).
            detach: Indicates whether we should detach from the command (like the `-d`
                option to `docker exec`).
            tty: Indicates whether a TTY should be allocated (like the `-t` option to
                `docker exec`).
        Returns:
            If `detach` is `True`, this method will return the result of the exec
            process as a binary string.
            If `detach` is False, an `aiohttp.ClientWebSocketResponse` will be returned.
            You can use it to send and receive data the same wa as the response of
            "ws_connect" of aiohttp. If `tty` is `False`, then the messages returned
            from `receive*` will have their `extra` attribute set to 1 if the data was
            from stdout or 2 if from stderr.
        """
        # Default to infinite timeout for exec operations
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        if detach:
            return self._start_detached(
                timeout_config,
                self._tty if self._tty is not None else False,
            )
        else:

            async def setup() -> Tuple[URL, bytes, bool]:
                if self._tty is None:
                    await self.inspect()  # should restore tty
                assert self._tty is not None
                return (
                    URL(f"exec/{self._id}/start"),
                    json.dumps({"Detach": False, "Tty": self._tty}).encode("utf8"),
                    self._tty,
                )

            return Stream(self.docker, setup, timeout_config)

    async def _start_detached(
        self,
        timeout: aiohttp.ClientTimeout,
        tty: bool = False,
    ) -> bytes:
        if self._tty is None:
            await self.inspect()  # should restore tty
        assert self._tty is not None
        async with self.docker._query(
            f"exec/{self._id}/start",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"Detach": True, "Tty": tty}),
            timeout=timeout,
        ) as response:
            result = await response.read()
            await response.release()
            return result
