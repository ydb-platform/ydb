import asyncio
import contextlib
import ctypes
import inspect
import logging
import signal
import subprocess
import sys
import time
from collections.abc import AsyncGenerator, Sequence

from testsuite.utils import traceback

SIGNAL_ERRORS: dict[int, str] = {
    signal.SIGSEGV: (
        'Service crashed with {signal_name} signal (segmentation fault)'
    ),
    signal.SIGABRT: 'Service aborted by {signal_name} signal',
}
DEFAULT_SIGNAL_ERROR = 'Service terminated by {signal_name} signal'

_KNOWN_SIGNALS: dict[int, str] = {
    signal.SIGABRT: 'SIGABRT',
    signal.SIGBUS: 'SIGBUS',
    signal.SIGFPE: 'SIGFPE',
    signal.SIGHUP: 'SIGHUP',
    signal.SIGINT: 'SIGINT',
    signal.SIGKILL: 'SIGKILL',
    signal.SIGPIPE: 'SIGPIPE',
    signal.SIGSEGV: 'SIGSEGV',
    signal.SIGTERM: 'SIGTERM',
}
_POLL_TIMEOUT = 0.1

logger = logging.getLogger(__name__)


class BaseError(Exception):
    pass


class HealthCheckError(BaseError):
    pass


class ExitCodeError(BaseError):
    def __init__(self, message: str, exit_code: int) -> None:
        super().__init__(message)
        self.exit_code = exit_code


# TODO: drop py3.6 support and use asyncio.Process instead
class AioReaders:
    def __init__(self, loop=None):
        self._tasks = []
        self._loop = loop

    async def aclose(self):
        if self._tasks:
            await asyncio.wait(self._tasks)

    async def add(self, pipe, handler):
        if not pipe or not handler:
            return
        is_coro = inspect.iscoroutinefunction(handler)
        reader = await _create_pipe_reader(pipe, loop=self._loop)

        async def data_handler():
            async for line in reader:
                if is_coro:
                    await handler(line)
                else:
                    handler(line)

        coro = data_handler()
        self._tasks.append(asyncio.create_task(coro))


@contextlib.asynccontextmanager
async def spawned(
    args: Sequence[str],
    *,
    shutdown_signal: int = signal.SIGINT,
    shutdown_timeout: float = 120,
    subprocess_spawner=None,
    stdout_handler=None,
    stderr_handler=None,
    **kwargs,
) -> AsyncGenerator[subprocess.Popen, None]:
    if stdout_handler:
        kwargs['stdout'] = subprocess.PIPE
    if stderr_handler:
        kwargs['stderr'] = subprocess.PIPE

    kwargs['preexec_fn'] = kwargs.get('preexec_fn', _setup_process)

    logger.debug('Starting process with args %r', args)
    if subprocess_spawner:
        process = subprocess_spawner(args, **kwargs)
    else:
        process = subprocess.Popen(args, **kwargs)
    logging.debug('[%d] Process started', process.pid)

    readers = AioReaders()
    await readers.add(process.stdout, stdout_handler)
    await readers.add(process.stderr, stderr_handler)

    async with contextlib.aclosing(readers):
        async with _shutdown_service(
            process,
            shutdown_signal=shutdown_signal,
            shutdown_timeout=shutdown_timeout,
        ):
            yield process


def exit_code_error(process: subprocess.Popen) -> ExitCodeError:
    retcode = process.returncode
    return ExitCodeError(_exit_code_text(retcode), retcode)


def _exit_code_text(retcode: int):
    if retcode >= 0:
        return f'Service exited with status code {retcode}'
    signal_name = _pretty_signal(-retcode)
    signal_error_fmt = SIGNAL_ERRORS.get(-retcode, DEFAULT_SIGNAL_ERROR)
    return signal_error_fmt.format(signal_name=signal_name)


@contextlib.asynccontextmanager
async def _shutdown_service(*args, **kwargs):
    try:
        yield
    finally:
        await _do_service_shutdown(*args, **kwargs)


async def _do_service_shutdown(process, *, shutdown_signal, shutdown_timeout):
    allowed_exit_codes = (-shutdown_signal, 0)

    retcode = process.poll()
    if retcode is not None:
        logger.info(
            '[%d] Process already finished with code %d', process.pid, retcode
        )
        if retcode not in allowed_exit_codes:
            raise exit_code_error(process)
        return retcode

    try:
        process.send_signal(shutdown_signal)
    except OSError:
        pass
    else:
        logger.info(
            '[%d] Trying to stop process with signal %s',
            process.pid,
            _pretty_signal(shutdown_signal),
        )
        poll_start = time.monotonic()
        while True:
            retcode = process.poll()
            if retcode is not None:
                if retcode not in allowed_exit_codes:
                    raise exit_code_error(process)
                return retcode
            current_time = time.monotonic()
            if current_time - poll_start > shutdown_timeout:
                break
            await asyncio.sleep(_POLL_TIMEOUT)

        logger.warning(
            '[%d] Process did not finished within shutdown timeout %d seconds',
            process.pid,
            shutdown_timeout,
        )

    logger.warning('[%d] Now killing process with signal SIGKILL', process.pid)
    while True:
        retcode = process.poll()
        if retcode is not None:
            raise exit_code_error(process)
        try:
            process.send_signal(signal.SIGKILL)
        except OSError:
            continue
        await asyncio.sleep(_POLL_TIMEOUT)


def _pretty_signal(signum: int) -> str:
    if signum in _KNOWN_SIGNALS:
        return _KNOWN_SIGNALS[signum]
    return str(signum)


async def _create_pipe_reader(pipe, loop=None):
    if loop is None:
        loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    reader_protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: reader_protocol, pipe)
    return reader


# Send SIGKILL to child process on unexpected parent termination
_PR_SET_PDEATHSIG = 1
if sys.platform == 'linux':
    _LIBC = ctypes.CDLL('libc.so.6')
else:
    _LIBC = None


def _setup_process() -> None:
    if _LIBC is not None:
        _LIBC.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)


__tracebackhide__ = traceback.hide(BaseError)
