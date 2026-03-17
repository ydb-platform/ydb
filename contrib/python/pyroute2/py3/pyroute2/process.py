import builtins
import gc
import json
import logging
import multiprocessing
import os
import select
import signal
import socket
import struct
from collections import namedtuple
from typing import Any, Callable, Optional, Union

from pyroute2 import config
from pyroute2.common import USE_DEFAULT_TIMEOUT
from pyroute2.netlink import exceptions as pyroute2_exceptions

log = logging.getLogger(__name__)

ChildProcessReturnValue = namedtuple(
    'ChildProcessReturnValue', ('payload', 'fds')
)
ChildFuncReturnType = Union[None, ChildProcessReturnValue, bytearray, bytes]


def wrapper(
    ctrl: socket.socket,
    func: Callable[..., ChildFuncReturnType],
    argv: list[Any],
) -> None:
    '''Child function wrapper.

    This code will be executed in the child process after running fork().
    The internal data structures might be damaged, so the function `func`
    must be as simple as possible, and use only local variables of simple
    types.

    The garbage collector should be disabled as well to minimize possible
    deadlocks.

    If process doesn't response in time, it will get killed.
    '''
    gc.disable()
    if config.disable_mp_signal:
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
    payload: bytes = b''
    ret_data: bytes = b''
    fds: list[int] = []
    sockets: list[socket.socket] = []
    try:
        ret = func(*argv)
        if isinstance(ret, bytes):
            ret_data = ret
        if isinstance(ret, ChildProcessReturnValue):
            ret_data, sockets = ret
        if isinstance(ret_data, bytearray):
            ret_data = bytes(ret_data)
        if not isinstance(ret_data, bytes):
            raise TypeError('return values not supported')
        payload = struct.pack('B', 2) + ret_data
        if sockets:
            fds = [x.fileno() for x in sockets]
    except Exception as e:
        payload = struct.pack('B', 1) + json.dumps(
            {'exception': e.__class__.__name__, 'options': e.args}
        ).encode('utf-8')
        fds = []
    finally:
        socket.send_fds(ctrl, [payload], fds, len(fds))


class ChildProcess:
    ctrl_r: socket.socket
    ctrl_w: socket.socket

    def __init__(
        self, target: Callable[..., ChildFuncReturnType], args: list[Any]
    ):
        self._mode: str = config.child_process_mode
        self._target: Callable[..., ChildFuncReturnType] = target
        self._args: list[Any] = args
        self._proc: Optional[multiprocessing.Process] = None
        self._running: bool = False
        self._exitcode: Optional[int] = None
        self._pid: Optional[int] = None

    def close(self):
        self.stop()

    def __enter__(self):
        self.run()
        return self

    def __exit__(self, *_):
        self.close()

    @property
    def mode(self):
        return self._mode

    @property
    def pid(self):
        return self._pid

    def communicate(
        self, timeout: int = USE_DEFAULT_TIMEOUT
    ) -> tuple[bytes, list[int]]:
        '''Communicate with the child process.

        Raises:

        * struct.error -- error unpacking response from the child process
        * OSError -- OS level error communicating with the child process
        * TimeoutError -- the child process is alive, but doesn't response
        * RuntimeError -- the child process is dead or not started
        * TypeError -- error loading propagated exception
        '''
        if not self._running:
            raise RuntimeError('child process not started yet')
        if timeout == USE_DEFAULT_TIMEOUT:
            timeout = config.default_communicate_timeout

        poll = select.poll()
        poll.register(self.ctrl_r, select.POLLIN)
        poll_results = poll.poll(timeout * 1000)  # convert to microseconds
        poll.unregister(self.ctrl_r)
        if not poll_results:
            # no data received within timeout
            # 1. the child process is killed
            # 2. the child process is stuck
            #
            # So first check the process, if it is killed, raise
            # RuntimeError, otherwise raise TimeoutError.
            #
            c_pid, w_status = os.waitpid(self.pid, os.WNOHANG)
            if c_pid != 0:
                # process is dead
                self._exitcode = os.waitstatus_to_exitcode(w_status)
                raise RuntimeError(
                    f'child process is dead, status: {self.exitcode}'
                )
            # process is alive, but stuck, kill it
            self.stop(kill=True, reason='no response from the child')
            raise TimeoutError(f'no response from the child pid {self.pid}')

        ret_data = b''
        # raises OSError
        (raw_data, fds, _, _) = socket.recv_fds(self.ctrl_r, 1024, 1)
        # get the return type
        # raises struct.error
        (ret_type,) = struct.unpack('B', raw_data[:1])
        raw_data = raw_data[1:]
        if ret_type == 1:
            # exception
            payload = json.loads(raw_data.decode('utf-8'))
            if not set(payload.keys()) == set(('exception', 'options')):
                raise TypeError('error loading child exception')
            if payload.get('exception') is not None:
                error_class = getattr(builtins, payload['exception'], None)
                if error_class is None:
                    error_class = getattr(
                        pyroute2_exceptions, payload['exception'], None
                    )
                if error_class is None:
                    error_class = Exception
                if not issubclass(error_class, Exception):
                    raise TypeError('error loading child error')
                raise error_class(*payload['options'])
        elif ret_type == 2:
            # raw_data
            ret_data = raw_data
        return ret_data, fds

    def get_data(self, timeout: int = USE_DEFAULT_TIMEOUT) -> bytes:
        return self.communicate(timeout)[0]

    def get_fds(self, timeout: int = USE_DEFAULT_TIMEOUT) -> list[int]:
        return self.communicate(timeout)[1]

    @property
    def proc(self) -> multiprocessing.Process:
        if self._proc is None:
            raise RuntimeError('not started')
        return self._proc

    @proc.setter
    def proc(self, value: Optional[multiprocessing.Process]) -> None:
        self._proc = value

    def _unsupported(self) -> Exception:
        return TypeError('unsupported mode')

    def run(self) -> None:
        if self._running:
            return
        self._running = True
        self.ctrl_r, self.ctrl_w = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_DGRAM
        )
        if self.mode == 'fork':
            self._pid = os.fork()
            if self._pid == 0:
                wrapper(self.ctrl_w, self._target, self._args)
                os._exit(0)
        elif self.mode == 'mp':
            self.proc = multiprocessing.Process(
                target=wrapper, args=[self.ctrl_w, self._target, self._args]
            )
            self.proc.start()
            self._pid = self.proc.pid
        else:
            raise self._unsupported()

    def stop(self, kill: bool = False, reason: Optional[str] = None) -> None:
        if not self._running:
            return
        self._running = False
        self.ctrl_r.close()
        self.ctrl_w.close()
        if config.force_gc:
            gc.collect()
        if self.mode == 'fork':
            try:
                if kill:
                    os.kill(self.pid, signal.SIGKILL)
                else:
                    os.kill(self.pid, signal.SIGTERM)
                _, status = os.waitpid(self.pid, 0)
                self._exitcode = os.waitstatus_to_exitcode(status)
            except ProcessLookupError:
                # the child process has already exited
                pass
        elif self.mode == 'mp':
            if kill:
                self.proc.kill()
            else:
                self.proc.terminate()
            self.proc.join()
        else:
            raise self._unsupported()
        if reason is not None:
            log.warning(reason)

    @property
    def exitcode(self) -> Optional[int]:
        if self.mode == 'fork':
            return self._exitcode
        elif self.mode == 'mp':
            return self.proc.exitcode
        else:
            raise self._unsupported()
