#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Connection/Session management module."""

import asyncio
import json
import logging
from typing import Awaitable, Callable, Dict, Union, TYPE_CHECKING

from pyee import EventEmitter
import websockets
from websockets.legacy.client import connect as ws_connect

from pyppeteer.errors import NetworkError

if TYPE_CHECKING:
    from typing import Optional  # noqa: F401

logger = logging.getLogger(__name__)
logger_connection = logging.getLogger(__name__ + '.Connection')
logger_session = logging.getLogger(__name__ + '.CDPSession')


class Connection(EventEmitter):
    """Connection management class."""

    def __init__(self, url: str, loop: asyncio.AbstractEventLoop,
                 delay: int = 0) -> None:
        """Make connection.

        :arg str url: WebSocket url to connect devtool.
        :arg int delay: delay to wait before processing received messages.
        """
        super().__init__()
        self._url = url
        self._lastId = 0
        self._callbacks: Dict[int, asyncio.Future] = dict()
        self._delay = delay / 1000
        self._loop = loop
        self._sessions: Dict[str, CDPSession] = dict()
        self.connection: CDPSession
        self._connected = False
        self._ws = ws_connect(self._url, max_size=None, loop=self._loop, ping_interval=None, ping_timeout=None)
        self._recv_fut = self._loop.create_task(self._recv_loop())
        self._closeCallback: Optional[Callable[[], None]] = None

    @property
    def url(self) -> str:
        """Get connected WebSocket url."""
        return self._url

    async def _recv_loop(self) -> None:
        async with self._ws as connection:
            self._connected = True
            self.connection = connection
            while self._connected:
                try:
                    resp = await self.connection.recv()
                    if resp:
                        await self._on_message(resp)
                except (websockets.ConnectionClosed, ConnectionResetError):
                    logger.info('connection closed')
                    break
                await asyncio.sleep(0)
        if self._connected:
            self._loop.create_task(self.dispose())

    async def _async_send(self, msg: str, callback_id: int) -> None:
        while not self._connected:
            await asyncio.sleep(self._delay)
        try:
            await self.connection.send(msg)
        except websockets.ConnectionClosed:
            logger.error('connection unexpectedly closed')
            callback = self._callbacks.get(callback_id, None)
            if callback and not callback.done():
                callback.set_result(None)
                await self.dispose()

    def send(self, method: str, params: dict = None) -> Awaitable:
        """Send message via the connection."""
        # Detect connection availability from the second transmission
        if self._lastId and not self._connected:
            raise ConnectionError('Connection is closed')
        if params is None:
            params = dict()
        self._lastId += 1
        _id = self._lastId
        msg = json.dumps(dict(
            id=_id,
            method=method,
            params=params,
        ))
        logger_connection.debug(f'SEND: {msg}')
        self._loop.create_task(self._async_send(msg, _id))
        callback = self._loop.create_future()
        self._callbacks[_id] = callback
        callback.error: Exception = NetworkError()  # type: ignore
        callback.method: str = method  # type: ignore
        return callback

    def _on_response(self, msg: dict) -> None:
        callback = self._callbacks.pop(msg.get('id', -1))
        if msg.get('error'):
            callback.set_exception(
                _createProtocolError(
                    callback.error,  # type: ignore
                    callback.method,  # type: ignore
                    msg
                )
            )
        else:
            callback.set_result(msg.get('result'))

    def _on_query(self, msg: dict) -> None:
        params = msg.get('params', {})
        method = msg.get('method', '')
        sessionId = params.get('sessionId')
        if method == 'Target.receivedMessageFromTarget':
            session = self._sessions.get(sessionId)
            if session:
                session._on_message(params.get('message'))
        elif method == 'Target.detachedFromTarget':
            session = self._sessions.get(sessionId)
            if session:
                session._on_closed()
                del self._sessions[sessionId]
        else:
            self.emit(method, params)

    def setClosedCallback(self, callback: Callable[[], None]) -> None:
        """Set closed callback."""
        self._closeCallback = callback

    async def _on_message(self, message: str) -> None:
        await asyncio.sleep(self._delay)
        logger_connection.debug(f'RECV: {message}')
        msg = json.loads(message)
        if msg.get('id') in self._callbacks:
            self._on_response(msg)
        else:
            self._on_query(msg)

    async def _on_close(self) -> None:
        if self._closeCallback:
            self._closeCallback()
            self._closeCallback = None

        for cb in self._callbacks.values():
            cb.set_exception(_rewriteError(
                cb.error,  # type: ignore
                f'Protocol error {cb.method}: Target closed.',  # type: ignore
            ))
        self._callbacks.clear()

        for session in self._sessions.values():
            session._on_closed()
        self._sessions.clear()

        # close connection
        if hasattr(self, 'connection'):  # may not have connection
            await self.connection.close()
        if not self._recv_fut.done():
            self._recv_fut.cancel()

    async def dispose(self) -> None:
        """Close all connection."""
        self._connected = False
        await self._on_close()

    async def createSession(self, targetInfo: Dict) -> 'CDPSession':
        """Create new session."""
        resp = await self.send(
            'Target.attachToTarget',
            {'targetId': targetInfo['targetId']}
        )
        sessionId = resp.get('sessionId')
        session = CDPSession(self, targetInfo['type'], sessionId, self._loop)
        self._sessions[sessionId] = session
        return session


class CDPSession(EventEmitter):
    """Chrome Devtools Protocol Session.

    The :class:`CDPSession` instances are used to talk raw Chrome Devtools
    Protocol:

    * protocol methods can be called with :meth:`send` method.
    * protocol events can be subscribed to with :meth:`on` method.

    Documentation on DevTools Protocol can be found
    `here <https://chromedevtools.github.io/devtools-protocol/>`__.
    """

    def __init__(self, connection: Union[Connection, 'CDPSession'],
                 targetType: str, sessionId: str,
                 loop: asyncio.AbstractEventLoop) -> None:
        """Make new session."""
        super().__init__()
        self._lastId = 0
        self._callbacks: Dict[int, asyncio.Future] = {}
        self._connection: Optional[Connection] = connection
        self._targetType = targetType
        self._sessionId = sessionId
        self._sessions: Dict[str, CDPSession] = dict()
        self._loop = loop

    def send(self, method: str, params: dict = None) -> Awaitable:
        """Send message to the connected session.

        :arg str method: Protocol method name.
        :arg dict params: Optional method parameters.
        """
        if not self._connection:
            raise NetworkError(
                f'Protocol Error ({method}): Session closed. Most likely the '
                f'{self._targetType} has been closed.'
            )
        self._lastId += 1
        _id = self._lastId
        msg = json.dumps(dict(id=_id, method=method, params=params))
        logger_session.debug(f'SEND: {msg}')

        callback = self._loop.create_future()
        self._callbacks[_id] = callback
        callback.error: Exception = NetworkError()  # type: ignore
        callback.method: str = method  # type: ignore
        try:
            self._connection.send('Target.sendMessageToTarget', {
                'sessionId': self._sessionId,
                'message': msg,
            })
        except Exception as e:
            # The response from target might have been already dispatched
            if _id in self._callbacks:
                _callback = self._callbacks[_id]
                del self._callbacks[_id]
                _callback.set_exception(_rewriteError(
                    _callback.error,  # type: ignore
                    e.args[0],
                ))
        return callback

    def _on_message(self, msg: str) -> None:  # noqa: C901
        logger_session.debug(f'RECV: {msg}')
        obj = json.loads(msg)
        _id = obj.get('id')
        if _id:
            callback = self._callbacks.get(_id)
            if callback:
                del self._callbacks[_id]
                if obj.get('error'):
                    callback.set_exception(_createProtocolError(
                        callback.error,  # type: ignore
                        callback.method,  # type: ignore
                        obj,
                    ))
                else:
                    result = obj.get('result')
                    if callback and not callback.done():
                        callback.set_result(result)
        else:
            params = obj.get('params', {})
            if obj.get('method') == 'Target.receivedMessageFromTarget':
                session = self._sessions.get(params.get('sessionId'))
                if session:
                    session._on_message(params.get('message'))
            elif obj.get('method') == 'Target.detachFromTarget':
                sessionId = params.get('sessionId')
                session = self._sessions.get(sessionId)
                if session:
                    session._on_closed()
                    del self._sessions[sessionId]
            self.emit(obj.get('method'), obj.get('params'))

    async def detach(self) -> None:
        """Detach session from target.

        Once detached, session won't emit any events and can't be used to send
        messages.
        """
        if not self._connection:
            raise NetworkError('Connection already closed.')
        await self._connection.send('Target.detachFromTarget',
                                    {'sessionId': self._sessionId})

    def _on_closed(self) -> None:
        for cb in self._callbacks.values():
            cb.set_exception(_rewriteError(
                cb.error,  # type: ignore
                f'Protocol error {cb.method}: Target closed.',  # type: ignore
            ))
        self._callbacks.clear()
        self._connection = None

    def _createSession(self, targetType: str, sessionId: str) -> 'CDPSession':
        session = CDPSession(self, targetType, sessionId, self._loop)
        self._sessions[sessionId] = session
        return session


def _createProtocolError(error: Exception, method: str, obj: Dict
                         ) -> Exception:
    message = f'Protocol error ({method}): {obj["error"]["message"]}'
    if 'data' in obj['error']:
        message += f' {obj["error"]["data"]}'
    return _rewriteError(error, message)


def _rewriteError(error: Exception, message: str) -> Exception:
    error.args = (message, )
    return error
