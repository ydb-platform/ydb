"""Peer transports: how one pile instance talks to another.

Operations: ping, state, prepare, commit, abort, push. Payloads are plain JSON dicts.
`InMemoryNetwork` runs several instances in one event loop for tests. `HttpTransport`
talks to `/peer/*` on the control port of the other instance.
"""
import asyncio
import json
import typing

import aiohttp

PEER_OPS = ("ping", "state", "prepare", "commit", "abort", "push")
GET_OPS = ("ping", "state")


class PeerError(Exception):
    """The peer could not be reached or refused the whole link."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class PeerBlocked(Exception):
    """Raised by a handler when the sender is on the receiver's link block list."""


class Transport:
    async def call(self, src: int, dst: int, op: str, payload: dict, timeout: float) -> dict:
        raise NotImplementedError

    async def close(self) -> None:
        pass


class InMemoryNetwork(Transport):
    """Connects PileInstance objects in one event loop. Drops, delays and kills links."""

    def __init__(self) -> None:
        self.instances: typing.Dict[int, typing.Any] = {}
        self.dead: typing.Set[int] = set()
        self.frozen: typing.Set[int] = set()            # accepts, never answers (SIGSTOP)
        self.cut: typing.Set[typing.Tuple[int, int]] = set()   # (src, dst) dropped
        self.delays: typing.Dict[typing.Tuple[int, int], float] = {}
        self.calls: typing.List[typing.Tuple[int, int, str]] = []

    def register(self, index: int, instance) -> None:
        self.instances[index] = instance
        self.dead.discard(index)

    def kill(self, index: int) -> None:
        self.dead.add(index)

    def revive(self, index: int) -> None:
        self.dead.discard(index)
        self.frozen.discard(index)

    def freeze(self, index: int) -> None:
        self.frozen.add(index)

    def cut_both(self, a: int, b: int) -> None:
        self.cut.add((a, b))
        self.cut.add((b, a))

    def heal(self) -> None:
        self.cut.clear()
        self.delays.clear()

    async def call(self, src: int, dst: int, op: str, payload: dict, timeout: float) -> dict:
        self.calls.append((src, dst, op))
        target = self.instances.get(dst)
        if target is None or dst in self.dead or (src, dst) in self.cut:
            raise PeerError("unreachable")
        body = json.loads(json.dumps(payload))

        async def work() -> dict:
            delay = self.delays.get((src, dst), 0.0)
            if delay:
                await asyncio.sleep(delay)
            if dst in self.frozen:
                await asyncio.Event().wait()
            return await target.handle_peer(op, src, body)

        try:
            reply = await asyncio.wait_for(work(), timeout)
        except asyncio.TimeoutError:
            raise PeerError("timeout")
        except PeerBlocked:
            raise PeerError("blocked")
        return json.loads(json.dumps(reply))


class HttpTransport(Transport):
    def __init__(self, topology) -> None:
        self._topology = topology
        self._session: typing.Optional[aiohttp.ClientSession] = None

    def _client(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True))
        return self._session

    async def call(self, src: int, dst: int, op: str, payload: dict, timeout: float) -> dict:
        topo = self._topology
        url = "%s/peer/%s" % (topo.control_url(dst), op)
        sender = topo.pile_names[src]
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        try:
            if op in GET_OPS:
                request = self._client().get(url, params={"from": sender}, timeout=client_timeout)
            else:
                body = dict(payload)
                body["from"] = sender
                request = self._client().post(url, json=body, timeout=client_timeout)
            async with request as response:
                if response.status == 503:
                    raise PeerError("blocked")
                if response.status != 200:
                    raise PeerError("http %d" % response.status)
                return await response.json()
        except PeerError:
            raise
        except asyncio.TimeoutError:
            raise PeerError("timeout")
        except (aiohttp.ClientError, OSError, ValueError):
            raise PeerError("unreachable")

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
