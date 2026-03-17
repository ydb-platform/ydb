import asyncio
import functools
import logging
import socket
import sys
from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    TypeVar,
    Union,
    overload,
)

import pycares

from . import error

__version__ = '3.6.1'

__all__ = ('DNSResolver', 'error')

_T = TypeVar('_T')

WINDOWS_SELECTOR_ERR_MSG = (
    'aiodns needs a SelectorEventLoop on Windows. See more: '
    'https://github.com/aio-libs/aiodns#note-for-windows-users'
)

_LOGGER = logging.getLogger(__name__)

query_type_map = {
    'A': pycares.QUERY_TYPE_A,
    'AAAA': pycares.QUERY_TYPE_AAAA,
    'ANY': pycares.QUERY_TYPE_ANY,
    'CAA': pycares.QUERY_TYPE_CAA,
    'CNAME': pycares.QUERY_TYPE_CNAME,
    'MX': pycares.QUERY_TYPE_MX,
    'NAPTR': pycares.QUERY_TYPE_NAPTR,
    'NS': pycares.QUERY_TYPE_NS,
    'PTR': pycares.QUERY_TYPE_PTR,
    'SOA': pycares.QUERY_TYPE_SOA,
    'SRV': pycares.QUERY_TYPE_SRV,
    'TXT': pycares.QUERY_TYPE_TXT,
}

query_class_map = {
    'IN': pycares.QUERY_CLASS_IN,
    'CHAOS': pycares.QUERY_CLASS_CHAOS,
    'HS': pycares.QUERY_CLASS_HS,
    'NONE': pycares.QUERY_CLASS_NONE,
    'ANY': pycares.QUERY_CLASS_ANY,
}


class DNSResolver:
    def __init__(
        self,
        nameservers: Optional[Sequence[str]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any,
    ) -> None:  # TODO(PY311): Use Unpack for kwargs.
        self._closed = True
        self.loop = loop or asyncio.get_event_loop()
        if TYPE_CHECKING:
            assert self.loop is not None
        kwargs.pop('sock_state_cb', None)
        timeout = kwargs.pop('timeout', None)
        self._timeout = timeout
        self._event_thread, self._channel = self._make_channel(**kwargs)
        if nameservers:
            self.nameservers = nameservers
        self._read_fds: set[int] = set()
        self._write_fds: set[int] = set()
        self._timer: Optional[asyncio.TimerHandle] = None
        self._closed = False

    def _make_channel(self, **kwargs: Any) -> tuple[bool, pycares.Channel]:
        if (
            hasattr(pycares, 'ares_threadsafety')
            and pycares.ares_threadsafety()
        ):
            # pycares is thread safe
            try:
                return True, pycares.Channel(
                    event_thread=True, timeout=self._timeout, **kwargs
                )
            except pycares.AresError as e:
                if sys.platform == 'linux':
                    _LOGGER.warning(
                        'Failed to create DNS resolver channel with automatic '
                        'monitoring of resolver configuration changes. This '
                        'usually means the system ran out of inotify watches. '
                        'Falling back to socket state callback. Consider '
                        'increasing the system inotify watch limit: %s',
                        e,
                    )
                else:
                    _LOGGER.warning(
                        'Failed to create DNS resolver channel with automatic '
                        'monitoring of resolver configuration changes. '
                        'Falling back to socket state callback: %s',
                        e,
                    )
        if sys.platform == 'win32' and not isinstance(
            self.loop, asyncio.SelectorEventLoop
        ):
            try:
                import winloop

                if not isinstance(self.loop, winloop.Loop):
                    raise RuntimeError(WINDOWS_SELECTOR_ERR_MSG)
            except ModuleNotFoundError as ex:
                raise RuntimeError(WINDOWS_SELECTOR_ERR_MSG) from ex
        return False, pycares.Channel(
            sock_state_cb=self._sock_state_cb, timeout=self._timeout, **kwargs
        )

    @property
    def nameservers(self) -> Sequence[str]:
        return self._channel.servers

    @nameservers.setter
    def nameservers(self, value: Iterable[Union[str, bytes]]) -> None:
        self._channel.servers = value

    def _callback(
        self, fut: asyncio.Future[_T], result: _T, errorno: Optional[int]
    ) -> None:
        if fut.cancelled():
            return
        if errorno is not None:
            fut.set_exception(
                error.DNSError(errorno, pycares.errno.strerror(errorno))
            )
        else:
            fut.set_result(result)

    def _get_future_callback(
        self,
    ) -> tuple['asyncio.Future[_T]', Callable[[_T, int], None]]:
        """Return a future and a callback to set the result of the future."""
        cb: Callable[[_T, int], None]
        future: asyncio.Future[_T] = self.loop.create_future()
        if self._event_thread:
            cb = functools.partial(  # type: ignore[assignment]
                self.loop.call_soon_threadsafe,
                self._callback,  # type: ignore[arg-type]
                future,
            )
        else:
            cb = functools.partial(self._callback, future)
        return future, cb

    @overload
    def query(
        self, host: str, qtype: Literal['A'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_a_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['AAAA'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_aaaa_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['CAA'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_caa_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['CNAME'], qclass: Optional[str] = ...
    ) -> asyncio.Future[pycares.ares_query_cname_result]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['MX'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_mx_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['NAPTR'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_naptr_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['NS'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_ns_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['PTR'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_ptr_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['SOA'], qclass: Optional[str] = ...
    ) -> asyncio.Future[pycares.ares_query_soa_result]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['SRV'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_srv_result]]: ...
    @overload
    def query(
        self, host: str, qtype: Literal['TXT'], qclass: Optional[str] = ...
    ) -> asyncio.Future[list[pycares.ares_query_txt_result]]: ...

    def query(
        self, host: str, qtype: str, qclass: Optional[str] = None
    ) -> Union[asyncio.Future[list[Any]], asyncio.Future[Any]]:
        try:
            qtype = query_type_map[qtype]
        except KeyError as e:
            raise ValueError(f'invalid query type: {qtype}') from e
        if qclass is not None:
            try:
                qclass = query_class_map[qclass]
            except KeyError as e:
                raise ValueError(f'invalid query class: {qclass}') from e

        fut: Union[asyncio.Future[list[Any]], asyncio.Future[Any]]
        fut, cb = self._get_future_callback()
        self._channel.query(host, qtype, cb, query_class=qclass)
        return fut

    def gethostbyname(
        self, host: str, family: socket.AddressFamily
    ) -> asyncio.Future[pycares.ares_host_result]:
        fut: asyncio.Future[pycares.ares_host_result]
        fut, cb = self._get_future_callback()
        self._channel.gethostbyname(host, family, cb)
        return fut

    def getaddrinfo(
        self,
        host: str,
        family: socket.AddressFamily = socket.AF_UNSPEC,
        port: Optional[int] = None,
        proto: int = 0,
        type: int = 0,
        flags: int = 0,
    ) -> asyncio.Future[pycares.ares_addrinfo_result]:
        fut: asyncio.Future[pycares.ares_addrinfo_result]
        fut, cb = self._get_future_callback()
        self._channel.getaddrinfo(
            host, port, cb, family=family, type=type, proto=proto, flags=flags
        )
        return fut

    def getnameinfo(
        self,
        sockaddr: Union[tuple[str, int], tuple[str, int, int, int]],
        flags: int = 0,
    ) -> asyncio.Future[pycares.ares_nameinfo_result]:
        fut: asyncio.Future[pycares.ares_nameinfo_result]
        fut, cb = self._get_future_callback()
        self._channel.getnameinfo(sockaddr, flags, cb)
        return fut

    def gethostbyaddr(
        self, name: str
    ) -> asyncio.Future[pycares.ares_host_result]:
        fut: asyncio.Future[pycares.ares_host_result]
        fut, cb = self._get_future_callback()
        self._channel.gethostbyaddr(name, cb)
        return fut

    def cancel(self) -> None:
        self._channel.cancel()

    def _sock_state_cb(self, fd: int, readable: bool, writable: bool) -> None:
        if readable or writable:
            if readable:
                self.loop.add_reader(
                    fd, self._channel.process_fd, fd, pycares.ARES_SOCKET_BAD
                )
                self._read_fds.add(fd)
            if writable:
                self.loop.add_writer(
                    fd, self._channel.process_fd, pycares.ARES_SOCKET_BAD, fd
                )
                self._write_fds.add(fd)
            if self._timer is None:
                self._start_timer()
        else:
            # socket is now closed
            if fd in self._read_fds:
                self._read_fds.discard(fd)
                self.loop.remove_reader(fd)

            if fd in self._write_fds:
                self._write_fds.discard(fd)
                self.loop.remove_writer(fd)

            if (
                not self._read_fds
                and not self._write_fds
                and self._timer is not None
            ):
                self._timer.cancel()
                self._timer = None

    def _timer_cb(self) -> None:
        if self._read_fds or self._write_fds:
            self._channel.process_fd(
                pycares.ARES_SOCKET_BAD, pycares.ARES_SOCKET_BAD
            )
            self._start_timer()
        else:
            self._timer = None

    def _start_timer(self) -> None:
        timeout = self._timeout
        if timeout is None or timeout < 0 or timeout > 1:
            timeout = 1
        elif timeout == 0:
            timeout = 0.1

        self._timer = self.loop.call_later(timeout, self._timer_cb)

    def _cleanup(self) -> None:
        """Cleanup timers and file descriptors when closing resolver."""
        if self._closed:
            return
        # Mark as closed first to prevent double cleanup
        self._closed = True
        # Cancel timer if running
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        # Remove all file descriptors
        for fd in self._read_fds:
            self.loop.remove_reader(fd)
        for fd in self._write_fds:
            self.loop.remove_writer(fd)

        self._read_fds.clear()
        self._write_fds.clear()
        self._channel.close()

    async def close(self) -> None:
        """
        Cleanly close the DNS resolver.

        This should be called to ensure all resources are properly released.
        After calling close(), the resolver should not be used again.
        """
        self._cleanup()

    async def __aenter__(self) -> 'DNSResolver':
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit the async context manager."""
        await self.close()

    def __del__(self) -> None:
        """Handle cleanup when the resolver is garbage collected."""
        # Check if we have a channel to clean up
        # This can happen if an exception occurs during __init__ before
        # _channel is created (e.g., RuntimeError on Windows
        # without proper loop)
        if hasattr(self, '_channel'):
            self._cleanup()
