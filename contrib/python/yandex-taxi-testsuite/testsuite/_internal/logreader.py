import pathlib
import sys
import threading
import typing


class LogFile:
    def __init__(self, path: pathlib.Path):
        self._path = path
        self._position = 0

    @property
    def path(self):
        return self._path

    @property
    def position(self):
        return self._position

    def filesize(self) -> int:
        try:
            return self._path.stat().st_size
        except FileNotFoundError:
            return 0

    def update_position(self):
        self._position = self.filesize()
        return self._position

    def readlines(
        self,
        eof_handler: typing.Callable[[], bool] | None = None,
        limit_position: bool = False,
    ):
        if limit_position:
            max_position = self.filesize()
        else:
            max_position = None
        first_skipped = False
        for line, position in _raw_line_reader(
            self._path,
            self._position,
            eof_handler=eof_handler,
        ):
            self._position = position
            yield line
            if max_position is not None and position >= max_position:
                break


class LiveLogHandler:
    def __init__(self, *, delay: float = 0.05):
        self._threads = {}
        self._exiting = False
        self._delay = delay
        self._condition = threading.Condition()

    def register_logfile(self, path: pathlib.Path, *, formatter):
        if path in self._threads:
            return
        thread = threading.Thread(
            target=self._logreader_thread,
            args=(path,),
            kwargs={'formatter': formatter},
        )
        self._threads[path] = thread
        thread.start()

    def join(self, timeout: float = 10):
        with self._condition:
            self._exiting = True
            self._condition.notify_all()
        for thread in self._threads.values():
            thread.join(timeout=timeout)

    def _logreader_thread(self, path: pathlib.Path, formatter):
        while not path.exists():
            # wait for file to appear
            if self._eof_handler():
                return
        logfile = LogFile(path)
        for line in logfile.readlines(eof_handler=self._eof_handler):
            line = line.rstrip(b'\r\n')
            line = formatter(line)
            if line:
                self._write_logline(line)

    def _write_logline(self, line: str):
        print(line, file=sys.stderr)

    def _eof_handler(self) -> bool:
        with self._condition:
            if self._condition.wait_for(
                lambda: self._exiting, timeout=self._delay
            ):
                return True
        return False


def _raw_line_reader(
    path: pathlib.Path,
    position: int = 0,
    eof_handler: typing.Callable[[], bool] | None = None,
) -> typing.Iterator[tuple[bytes, int]]:
    if not path.exists():
        return

    with path.open('rb') as fp:
        position = fp.seek(position)
        partial = None
        while True:
            for line in fp:
                if partial:
                    line = partial + line
                    partial = None
                if line.endswith(b'\n'):
                    position += len(line)
                    yield line, position
                else:
                    partial = line
            if not eof_handler or eof_handler():
                break
