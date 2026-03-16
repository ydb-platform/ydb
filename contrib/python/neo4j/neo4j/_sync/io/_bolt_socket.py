# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import asyncio
import dataclasses
import logging
import struct
from contextlib import suppress

from ... import _typing as t
from ..._addressing import Address
from ..._async_compat.network import (
    BoltSocketBase,
    NetworkUtil,
)
from ..._exceptions import (
    BoltError,
    BoltProtocolError,
)
from ..._io import (
    BoltProtocolVersion,
    min_timeout,
)
from ...exceptions import (
    DriverError,
    ServiceUnavailable,
)


if t.TYPE_CHECKING:
    from ssl import SSLContext

    from ..._addressing import ResolvedAddress
    from ..._deadline import Deadline


log = logging.getLogger("neo4j.io")


@dataclasses.dataclass
class HandshakeCtx:
    ctx: str
    deadline: Deadline
    local_port: int
    resolved_address: ResolvedAddress
    full_response: bytearray = dataclasses.field(default_factory=bytearray)


@dataclasses.dataclass
class BytesPrinter:
    bytes: bytes | bytearray

    def __str__(self):
        return f"0x{self.bytes.hex().upper()}"


class BoltSocket(BoltSocketBase):
    def _parse_handshake_response_v1(
        self,
        ctx: HandshakeCtx,
        response: bytes,
    ) -> BoltProtocolVersion:
        agreed_version = response[-1], response[-2]
        log.debug(
            "[#%04X]  S: <HANDSHAKE> 0x%06X%02X",
            ctx.local_port,
            agreed_version[1],
            agreed_version[0],
        )
        return BoltProtocolVersion(*agreed_version)

    def _parse_handshake_response_v2(
        self,
        ctx: HandshakeCtx,
        response: bytes,
    ) -> BoltProtocolVersion:
        ctx.ctx = "handshake v2 offerings count"
        num_offerings = self._read_varint(ctx)
        offerings = []
        for i in range(num_offerings):
            ctx.ctx = f"handshake v2 offering {i}"
            offering_response = self._handshake_read(ctx, 4)
            offering = offering_response[-1:-4:-1]
            offerings.append(offering)
        ctx.ctx = "handshake v2 capabilities"
        capabilities_offer = self._read_varint(ctx)

        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug(
                "[#%04X]  S: <HANDSHAKE> %s [%i] %s %s",
                ctx.local_port,
                BytesPrinter(response),
                num_offerings,
                " ".join(
                    f"0x{vx[2]:04X}{vx[1]:02X}{vx[0]:02X}" for vx in offerings
                ),
                BytesPrinter(self._encode_varint(capabilities_offer)),
            )

        supported_versions = sorted(self.Bolt.protocol_handlers.keys())
        chosen_version = BoltProtocolVersion(0, 0)
        for v in supported_versions:
            for offer_major, offer_minor, offer_range in offerings:
                offer_max = (offer_major, offer_minor)
                offer_min = (offer_major, offer_minor - offer_range)
                if offer_min <= v <= offer_max:
                    chosen_version = v
                    break

        ctx.ctx = "handshake v2 chosen version"
        self._handshake_send(
            ctx, bytes((0, 0, chosen_version.minor, chosen_version.major))
        )
        chosen_capabilities = 0
        capabilities = self._encode_varint(chosen_capabilities)
        ctx.ctx = "handshake v2 chosen capabilities"
        log.debug(
            "[#%04X]  C: <HANDSHAKE> 0x%06X%02X %s",
            ctx.local_port,
            chosen_version.minor,
            chosen_version.major,
            BytesPrinter(capabilities),
        )
        self._handshake_send(ctx, b"\x00")

        return chosen_version

    def _read_varint(self, ctx: HandshakeCtx) -> int:
        next_byte = (self._handshake_read(ctx, 1))[0]
        res = next_byte & 0x7F
        i = 0
        while next_byte & 0x80:
            i += 1
            next_byte = (self._handshake_read(ctx, 1))[0]
            res += (next_byte & 0x7F) << (7 * i)
        return res

    @staticmethod
    def _encode_varint(n: int) -> bytearray:
        res = bytearray()
        while n >= 0x80:
            res.append(n & 0x7F | 0x80)
            n >>= 7
        res.append(n)
        return res

    def _handshake_read(self, ctx: HandshakeCtx, n: int) -> bytes:
        original_timeout = self.get_read_timeout()
        self.set_read_timeout(ctx.deadline.to_timeout())
        try:
            response = self.recv(n)
            ctx.full_response.extend(response)
        except OSError as exc:
            raise ServiceUnavailable(
                f"Failed to read {ctx.ctx} from server "
                f"{ctx.resolved_address!r} (deadline {ctx.deadline})"
            ) from exc
        finally:
            self.set_read_timeout(original_timeout)
        data_size = len(response)
        if data_size == 0:
            # If no data is returned after a successful select
            # response, the server has closed the connection
            log.debug("[#%04X]  S: <CLOSE>", ctx.local_port)
            self.close()
            raise ServiceUnavailable(
                f"Connection to {ctx.resolved_address} closed with incomplete "
                f"handshake response"
            )
        if data_size != n:
            # Some garbled data has been received
            log.debug("[#%04X]  S: @*#!", ctx.local_port)
            self.close()
            raise BoltProtocolError(
                f"Expected {ctx.ctx} from {ctx.resolved_address!r}, received "
                f"{response!r} instead (so far {ctx.full_response!r}); "
                "check for incorrect port number",
                address=ctx.resolved_address,
            )

        return response

    def _handshake_send(self, ctx, data, write_timeout=None):
        original_timeout = self.get_write_timeout()
        self.set_write_timeout(
            min_timeout(ctx.deadline.to_timeout(), write_timeout)
        )
        try:
            self.sendall(data)
        except OSError as exc:
            raise ServiceUnavailable(
                f"Failed to write {ctx.ctx} to server "
                f"{ctx.resolved_address!r} (deadline {ctx.deadline})"
            ) from exc
        finally:
            self.set_write_timeout(original_timeout)

    def _handshake(
        self,
        resolved_address: ResolvedAddress,
        deadline: Deadline,
    ) -> BoltProtocolVersion:
        """
        Perform BOLT handshake.

        :param resolved_address:
        :param deadline: Deadline for handshake

        :returns: (version, client_handshake, server_response_data)
        """
        local_port = self.getsockname()[1]

        handshake = self.Bolt.get_handshake()
        if log.getEffectiveLevel() <= logging.DEBUG:
            handshake_bytes: t.Sequence = struct.unpack(">16B", handshake)
            handshake_bytes = [
                handshake[i : i + 4] for i in range(0, len(handshake_bytes), 4)
            ]

            supported_versions = [
                f"0x{vx[0]:02X}{vx[1]:02X}{vx[2]:02X}{vx[3]:02X}"
                for vx in handshake_bytes
            ]

            log.debug(
                "[#%04X]  C: <MAGIC> 0x%08X",
                local_port,
                int.from_bytes(self.Bolt.MAGIC_PREAMBLE, byteorder="big"),
            )
            log.debug(
                "[#%04X]  C: <HANDSHAKE> %s %s %s %s",
                local_port,
                *supported_versions,
            )

        request = self.Bolt.MAGIC_PREAMBLE + handshake

        ctx = HandshakeCtx(
            ctx="handshake opening",
            deadline=deadline,
            local_port=local_port,
            resolved_address=resolved_address,
        )

        self._handshake_send(ctx, request)

        ctx.ctx = "four byte Bolt handshake response"
        response = self._handshake_read(ctx, 4)

        if response == b"HTTP":
            log.debug("[#%04X]  C: <CLOSE> (received b'HTTP')", local_port)
            self.close()
            raise ServiceUnavailable(
                f"Cannot to connect to Bolt service on {resolved_address!r} "
                "(looks like HTTP)"
            )
        elif response[-1] == 0xFF:
            # manifest style handshake
            manifest_version = response[-2]
            if manifest_version == 0x01:
                agreed_version = self._parse_handshake_response_v2(
                    ctx,
                    response,
                )
            else:
                raise BoltProtocolError(
                    "Unsupported Bolt handshake manifest version "
                    f"{manifest_version} received from {resolved_address!r}.",
                    address=resolved_address,
                )
        else:
            agreed_version = self._parse_handshake_response_v1(
                ctx,
                response,
            )

        return agreed_version

    @classmethod
    def connect(
        cls,
        address: Address,
        *,
        tcp_timeout: float | None,
        deadline: Deadline,
        custom_resolver: t.Callable | None,
        ssl_context: SSLContext | None,
        keep_alive: bool,
    ) -> tuple[t.Self, BoltProtocolVersion]:
        """
        Connect and perform a handshake.

        Return a valid Connection object, assuming a protocol version can be
        agreed.
        """
        errors = []
        failed_addresses = []
        # Establish a connection to the host and port specified
        # Catches refused connections see:
        # https://docs.python.org/2/library/errno.html

        resolved_addresses = NetworkUtil.resolve_address(
            Address(address), resolver=custom_resolver
        )
        for resolved_address in resolved_addresses:
            deadline_timeout = deadline.to_timeout()
            if (
                deadline_timeout is not None
                and deadline_timeout <= tcp_timeout
            ):
                tcp_timeout = deadline_timeout
            s = None
            try:
                s = cls._connect_secure(
                    resolved_address,
                    tcp_timeout,
                    deadline,
                    keep_alive,
                    ssl_context,
                )
                agreed_version = s._handshake(resolved_address, deadline)
                return s, agreed_version
            except (BoltError, DriverError, OSError) as error:
                local_port = 0
                if isinstance(s, cls):
                    with suppress(OSError, AttributeError, TypeError):
                        local_port = s.getsockname()[1]
                err_str = error.__class__.__name__
                if str(error):
                    err_str += ": " + str(error)
                log.debug(
                    "[#%04X]  S: <CONNECTION FAILED> %s %s",
                    local_port,
                    resolved_address,
                    err_str,
                )
                if s:
                    cls.close_socket(s)
                errors.append(error)
                failed_addresses.append(resolved_address)
            except asyncio.CancelledError:
                local_port = 0
                if isinstance(s, cls):
                    with suppress(OSError, AttributeError, TypeError):
                        local_port = s.getsockname()[1]
                log.debug(
                    "[#%04X]  C: <CANCELED> %s", local_port, resolved_address
                )
                if s:
                    with suppress(OSError):
                        s.kill()
                raise
            except Exception:
                if s:
                    cls.close_socket(s)
                raise
        address_strs = tuple(map(str, failed_addresses))
        # TODO: 7.0 - when Python 3.11+ is the minimum, use exception groups
        if not errors:
            raise ServiceUnavailable(
                f"Couldn't connect to {address} (resolved to {address_strs})"
            )
        else:
            error_strs = "\n".join(map(str, errors))
            raise ServiceUnavailable(
                f"Couldn't connect to {address} (resolved to {address_strs}):"
                f"\n{error_strs}"
            ) from errors[0]
