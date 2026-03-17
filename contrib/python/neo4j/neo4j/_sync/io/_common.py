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


import asyncio
import inspect
import logging
from contextlib import suppress
from struct import pack as struct_pack

from ..._async_compat.util import Util
from ..._exceptions import SocketDeadlineExceededError
from ..._io import BoltProtocolVersion
from ...exceptions import (
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
)


GQL_ERROR_AWARE_PROTOCOL = BoltProtocolVersion(5, 7)

log = logging.getLogger("neo4j.io")


class Inbox:
    def __init__(self, sock, on_error, unpacker_cls):
        self.on_error = on_error
        self._local_port = sock.getsockname()[1]
        self._socket = sock
        self._buffer = unpacker_cls.new_unpackable_buffer()
        self._unpacker = unpacker_cls(self._buffer)
        self._broken = False

    def _buffer_one_chunk(self):
        assert not self._broken
        try:
            chunk_size = 0
            while True:
                while chunk_size == 0:
                    # Determine the chunk size and skip noop
                    receive_into_buffer(self._socket, self._buffer, 2)
                    chunk_size = self._buffer.pop_u16()
                    if chunk_size == 0:
                        log.debug("[#%04X]  S: <NOOP>", self._local_port)

                receive_into_buffer(
                    self._socket, self._buffer, chunk_size + 2
                )
                chunk_size = self._buffer.pop_u16()

                if chunk_size == 0:
                    # chunk_size was the end marker for the message
                    return
        except (
            OSError,
            SocketDeadlineExceededError,
            asyncio.CancelledError,
        ) as error:
            self._broken = True
            Util.callback(self.on_error, error)
            raise

    def pop(self, hydration_hooks):
        self._buffer_one_chunk()
        try:
            size, tag = self._unpacker.unpack_structure_header()
            fields = [
                self._unpacker.unpack(hydration_hooks) for _ in range(size)
            ]
            return tag, fields
        except Exception as error:
            log.debug(
                "[#%04X]  _: Failed to unpack response: %r",
                self._local_port,
                error,
            )
            self._broken = True
            Util.callback(self.on_error, error)
            raise
        finally:
            # Reset for new message
            self._unpacker.reset()


class Outbox:
    def __init__(self, sock, on_error, packer_cls, max_chunk_size=16384):
        self._max_chunk_size = max_chunk_size
        self._chunked_data = bytearray()
        self._buffer = packer_cls.new_packable_buffer()
        self._packer = packer_cls(self._buffer)
        self.socket = sock
        self.on_error = on_error

    def max_chunk_size(self):
        return self._max_chunk_size

    def _clear(self):
        assert not self._buffer.is_tmp_buffering()
        self._chunked_data = bytearray()
        self._buffer.clear()

    def _chunk_data(self):
        data_len = len(self._buffer.data)
        num_full_chunks, chunk_rest = divmod(data_len, self._max_chunk_size)
        num_chunks = num_full_chunks + bool(chunk_rest)

        with memoryview(self._buffer.data) as data_view:
            header_start = len(self._chunked_data)
            data_start = header_start + 2
            raw_data_start = 0
            for _ in range(num_chunks):
                chunk_size = min(
                    data_len - raw_data_start, self._max_chunk_size
                )
                self._chunked_data[header_start:data_start] = struct_pack(
                    ">H", chunk_size
                )
                self._chunked_data[data_start : (data_start + chunk_size)] = (
                    data_view[raw_data_start : (raw_data_start + chunk_size)]
                )
                header_start += chunk_size + 2
                data_start = header_start + 2
                raw_data_start += chunk_size
        self._buffer.clear()

    def _wrap_message(self):
        assert not self._buffer.is_tmp_buffering()
        self._chunk_data()
        self._chunked_data += b"\x00\x00"

    def append_message(self, tag, fields, dehydration_hooks):
        with self._buffer.tmp_buffer():
            self._packer.pack_struct(tag, fields, dehydration_hooks)
        self._wrap_message()

    def flush(self):
        data = self._chunked_data
        if data:
            try:
                self.socket.sendall(data)
            except (
                OSError,
                SocketDeadlineExceededError,
                asyncio.CancelledError,
            ) as error:
                Util.callback(self.on_error, error)
                return False
            self._clear()
            return True
        return False


class ConnectionErrorHandler:
    """
    Wrapper class for handling connection errors.

    The class will wrap each method to invoke a callback if the method raises
    Neo4jError, SessionExpired, or ServiceUnavailable.
    The error will be re-raised after the callback.

    :param connection the connection object to warp
    :type connection Bolt
    :param on_error the function to be called when a method of
        connection raises of the caught errors.
    :type on_error callable
    """

    def __init__(self, connection, on_error):
        self.__connection = connection
        self.__on_error = on_error

    def __getattr__(self, name):
        connection_attr = getattr(self.__connection, name)
        if not callable(connection_attr):
            return connection_attr

        def outer(func):
            def inner(*args, **kwargs):
                try:
                    func(*args, **kwargs)
                except (Neo4jError, ServiceUnavailable, SessionExpired) as exc:
                    assert not inspect.iscoroutinefunction(self.__on_error)
                    self.__on_error(exc)
                    raise

            return inner

        def outer_async(coroutine_func):
            def inner(*args, **kwargs):
                try:
                    coroutine_func(*args, **kwargs)
                except (
                    Neo4jError,
                    ServiceUnavailable,
                    SessionExpired,
                    asyncio.CancelledError,
                ) as exc:
                    Util.callback(self.__on_error, exc)
                    raise

            return inner

        if inspect.iscoroutinefunction(connection_attr):
            return outer_async(connection_attr)
        return outer(connection_attr)

    def __setattr__(self, name, value):
        if name.startswith("_" + self.__class__.__name__ + "__"):
            super().__setattr__(name, value)
        else:
            setattr(self.__connection, name, value)


class Response:
    """
    Subscriber object for a full response.

    I.e., zero or more detail messages followed by one summary message.
    """

    def __init__(self, connection, message, hydration_hooks, **handlers):
        self.connection = connection
        self.hydration_hooks = hydration_hooks
        self.handlers = handlers
        self.message = message
        self.complete = False

    def on_records(self, records):
        """Handle one or more RECORD messages been received."""
        handler = self.handlers.get("on_records")
        Util.callback(handler, records)

    def on_success(self, metadata):
        """Handle a SUCCESS message been received."""
        handler = self.handlers.get("on_success")
        Util.callback(handler, metadata)

        if not metadata.get("has_more"):
            handler = self.handlers.get("on_summary")
            Util.callback(handler)

    def on_failure(self, metadata):
        """Handle a FAILURE message been received."""
        with suppress(SessionExpired, ServiceUnavailable):
            self.connection.reset()
        handler = self.handlers.get("on_failure")
        Util.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        Util.callback(handler)
        raise self._hydrate_error(metadata)

    def on_ignored(self, metadata=None):
        """Handle an IGNORED message been received."""
        handler = self.handlers.get("on_ignored")
        Util.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        Util.callback(handler)

    def _hydrate_error(self, metadata):
        if self.connection.PROTOCOL_VERSION >= GQL_ERROR_AWARE_PROTOCOL:
            return Neo4jError._hydrate_gql(**metadata)
        else:
            return Neo4jError._hydrate_neo4j(**metadata)


class InitResponse(Response):
    def on_failure(self, metadata):
        # No sense in resetting the connection,
        # the server will have closed it already.
        self.connection.kill()
        handler = self.handlers.get("on_failure")
        Util.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        Util.callback(handler)
        metadata["message"] = metadata.get(
            "message",
            "Connection initialisation failed due to an unknown error",
        )
        raise self._hydrate_error(metadata)


class LogonResponse(InitResponse):
    def on_failure(self, metadata):
        # No sense in resetting the connection,
        # the server will have closed it already.
        self.connection.kill()
        handler = self.handlers.get("on_failure")
        Util.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        Util.callback(handler)
        raise self._hydrate_error(metadata)


class ResetResponse(Response):
    def _unexpected_message(self, response):
        log.warning(
            "[#%04X]  _: <CONNECTION> RESET received %s "
            "(unexpected response) => dropping connection",
            self.connection.local_port,
            response,
        )
        self.connection.close()

    def on_records(self, records):
        self._unexpected_message("RECORD")

    def on_success(self, metadata):
        pass

    def on_failure(self, metadata):
        self._unexpected_message("FAILURE")

    def on_ignored(self, metadata=None):
        self._unexpected_message("IGNORED")


class CommitResponse(Response):
    pass


def receive_into_buffer(sock, buffer, n_bytes):
    end = buffer.used + n_bytes
    if end > len(buffer.data):
        buffer.data += bytearray(end - len(buffer.data))
    with memoryview(buffer.data) as view:
        while buffer.used < end:
            n = sock.recv_into(
                view[buffer.used : end], end - buffer.used
            )
            if n == 0:
                raise OSError("No data")
            buffer.used += n
