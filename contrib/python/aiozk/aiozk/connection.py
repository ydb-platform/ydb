import asyncio
import collections
import logging
import re
import struct
import sys
from contextlib import suppress
from time import time

from aiozk import exc, iterables, protocol


DEFAULT_READ_TIMEOUT = 3

version_regex = re.compile(rb'Zookeeper version: (\d+)\.(\d+)\.(\d+)-.*')

# all requests and responses are prefixed with a 32-bit int denoting size
size_struct = struct.Struct('!i')
# replies are prefixed with an xid, zxid and error code
reply_header_struct = struct.Struct('!iqi')

log = logging.getLogger(__name__)
payload_log = logging.getLogger(__name__ + '.payload')
if payload_log.level == logging.NOTSET:
    payload_log.setLevel(logging.INFO)


class Connection:
    def __init__(self, host, port, watch_handler, read_timeout):
        self.host = host
        self.port = int(port)

        self.reader = None
        self.writer = None
        self.closing = False

        self.version_info = None
        self.start_read_only = None

        self.watch_handler = watch_handler

        self.opcode_xref = {}
        self.host_ip = None

        self.pending = {}
        self.pending_specials = collections.defaultdict(list)

        self.watches = collections.defaultdict(list)

        self.read_timeout = read_timeout or DEFAULT_READ_TIMEOUT
        self.read_loop_task = None

    async def _make_handshake(self):
        self.host_ip = self.writer.transport.get_extra_info('peername')[0]

        log.debug("Sending 'srvr' command to %s:%d", self.host, self.port)
        self.writer.write(b'srvr')

        answer = await self.reader.read()

        version_line = answer.split(b'\n')[0]
        match = version_regex.match(version_line)
        if match is None:
            raise ConnectionError
        self.version_info = tuple(map(int, match.groups()))
        self.start_read_only = bool(b'READ_ONLY' in answer)

        log.debug('Version info: %s', self.version_info)
        log.debug('Read-only mode: %s', self.start_read_only)

        log.debug('Actual connection to server %s:%d', self.host, self.port)

    async def connect(self):
        log.debug('Initial connection to server %s:%d', self.host, self.port)

        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.read_timeout
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f'Connection to {self.host}:{self.port} timed out after {self.read_timeout} seconds')

        try:
            await self._make_handshake()
        finally:
            self.writer.close()

        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def send_connect(self, request):
        # meant to be used before the read_loop starts
        payload_log.debug('[SEND] (initial) %s', request)

        payload = request.serialize()
        payload = size_struct.pack(len(payload)) + payload

        self.writer.write(payload)

        try:
            _, zxid, response = await self.read_response(initial_connect=True)
        except Exception:
            log.exception('Error reading connect response.')
            return

        payload_log.debug('[RECV] (initial) %s', response)
        return zxid, response

    def start_read_loop(self):
        self.read_loop_task = asyncio.create_task(self.read_loop())

    def send(self, request, xid=None):
        loop = asyncio.get_running_loop()
        f = loop.create_future()

        if self.closing:
            f.set_exception(exc.ConnectError(self.host, self.port))
            return f

        if request.special_xid:
            xid = request.special_xid

        payload_log.debug('[SEND] (xid: %s) %s', xid, request)

        payload = request.serialize(xid)
        payload = size_struct.pack(len(payload)) + payload

        self.opcode_xref[xid] = request.opcode

        if xid in protocol.SPECIAL_XIDS:
            self.pending_specials[xid].append(f)
        else:
            self.pending[xid] = f

        try:
            self.writer.write(payload)
        except Exception:
            log.exception('Exception during write')
            self.abort()

        return f

    def pending_count(self):
        return sum(len(futs) for futs in self.pending_specials.values()) + len(self.pending)

    async def read_loop(self):
        """
        Infinite loop that reads messages off of the socket while not closed.

        When a message is received its corresponding pending Future is set
        to have the message as its result.

        This is never used directly and is fired as a separate callback on the
        I/O loop via the `connect()` method.
        """
        while not self.closing or self.pending_count() > 0:
            try:
                xid, zxid, response = await self.read_response()
            except (ConnectionAbortedError, asyncio.CancelledError):
                return
            except Exception:
                log.exception('Error reading response.')
                self.abort()
                return

            payload_log.debug('[RECV] (xid: %s) %s', xid, response)

            if xid == protocol.WATCH_XID:
                self.watch_handler(response)
                continue
            elif xid in protocol.SPECIAL_XIDS:
                f = self.pending_specials[xid].pop()
            else:
                f = self.pending.pop(xid)

            if not f.done():
                if isinstance(response, Exception):
                    f.set_exception(response)
                else:
                    f.set_result((zxid, response))

    async def _read(self, size=-1):
        remaining_size = size
        end_time = time() + self.read_timeout
        payload = []
        while remaining_size and (time() < end_time):
            remaining_time = end_time - time()
            try:
                chunk = await asyncio.wait_for(self.reader.read(remaining_size), timeout=remaining_time)
            except asyncio.TimeoutError:
                continue
            payload.append(chunk)
            remaining_size -= len(chunk)
        if remaining_size:
            raise exc.UnfinishedRead
        return b''.join(payload)

    async def read_response(self, initial_connect=False):
        try:
            raw_size = await self.reader.readexactly(size_struct.size)
        except asyncio.IncompleteReadError:
            raise ConnectionAbortedError
        size = size_struct.unpack(raw_size)[0]

        # connect and close op replies don't contain a reply header
        if initial_connect or self.pending_specials[protocol.CLOSE_XID]:
            raw_payload = await self._read(size)
            response = protocol.ConnectResponse.deserialize(raw_payload)
            return (None, None, response)

        raw_header = await self._read(reply_header_struct.size)
        xid, zxid, error_code = reply_header_struct.unpack_from(raw_header)

        if error_code:
            self.opcode_xref.pop(xid)
            return (xid, zxid, exc.get_response_error(error_code))

        size -= reply_header_struct.size

        raw_payload = await self._read(size)

        if xid == protocol.WATCH_XID:
            response = protocol.WatchEvent.deserialize(raw_payload)
        else:
            opcode = self.opcode_xref.pop(xid)
            response = protocol.response_xref[opcode].deserialize(raw_payload)

        return (xid, zxid, response)

    def abort(self, exception=exc.ConnectError):
        """
        Aborts a connection and puts all pending futures into an error state.

        If ``sys.exc_info()`` is set (i.e. this is being called in an exception
        handler) then pending futures will have that exc info set.  Otherwise
        the given ``exception`` parameter is used (defaults to
        ``ConnectError``).
        """
        log.warning('Aborting connection to %s:%s', self.host, self.port)

        def abort_pending(f):
            exc_info = sys.exc_info()
            # TODO
            log.debug('Abort pending: %s', f)
            if False and any(exc_info):
                f.set_exc_info(exc_info)
            else:
                f.set_exception(exception(self.host, self.port))

        for pending in self.drain_all_pending():
            if pending.done() or pending.cancelled():
                continue
            abort_pending(pending)

    def drain_all_pending(self):
        for special_xid in protocol.SPECIAL_XIDS:
            for f in iterables.drain(self.pending_specials[special_xid]):
                yield f
        for _, f in iterables.drain(self.pending):
            yield f

    async def close(self, timeout):
        if self.closing:
            return
        self.closing = True

        if self.read_loop_task and not self.read_loop_task.done():
            try:
                await asyncio.wait_for(self.read_loop_task, timeout)
            except asyncio.TimeoutError:
                # At this point, read_loop_task.cancel() was already called by
                # wait_for.
                with suppress(asyncio.CancelledError):
                    await self.read_loop_task

        try:
            if self.pending_count() > 0:
                log.warning('Pendings: %s; specials: %s', self.pending, self.pending_specials)
                self.abort(exception=exc.TimeoutError)
        except asyncio.TimeoutError:
            log.warning('ABORT Timeout')
            await self.abort(exception=exc.TimeoutError)
        except Exception as e:
            log.exception('in close: %s', e)
            raise e
        finally:
            log.debug('Closing writer')
            self.writer.close()
            log.debug('Writer closed')
