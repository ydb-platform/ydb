from concurrent.futures import ThreadPoolExecutor
import subprocess
import sys
from typing import IO, Callable

from .io import lsp_read_message, lsp_write_message
from .trace import LspMessage

DEFAULT_TIMEOUT_SECONDS = 4


class LanguageServer:
    def __init__(self, args: list[str]):
        self._args = args
        self._process: subprocess.Popen[bytes] | None = None

    def __enter__(self) -> 'LanguageServer':
        self._process = subprocess.Popen(
            self._args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._process.__exit__(exc_type, exc_value, traceback)

    def __str__(self) -> str:
        return f"LanguageServer({', '.join(self._args[1:])})"

    def stop(self, timeout: int = DEFAULT_TIMEOUT_SECONDS):
        self._process.stdin.close()
        try:
            code = self._process.wait(timeout=timeout)
            assert code == 0
        except subprocess.TimeoutExpired as e:
            self._process.kill()
            raise e

    @property
    def stdin(self) -> IO[bytes]:
        return self._process.stdin

    @property
    def stdout(self) -> IO[bytes]:
        return self._process.stdout

    @property
    def stderr(self) -> IO[bytes]:
        return self._process.stderr

    def send(self, message: LspMessage) -> None:
        lsp_write_message(self.stdin, message)

    def recv(self) -> LspMessage | None:
        return lsp_read_message(self.stdout)


def carefully[T](run: Callable[[LanguageServer], T], server: LanguageServer) -> T:
    caught: Exception | None = None

    def stop(caught):
        try:
            server.stop()
        except Exception as e:
            if caught is None:
                caught = e
            else:
                caught.add_note(str(e))
        finally:
            return caught

    logs: list[str] = []

    def read_logs():
        for line in server.stderr.readlines():
            logs.append(line.decode('utf-8'))

    # NB: avoid stderr buffer overflow (blocking)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(read_logs)

        try:
            return run(server)
        except Exception as e:
            caught = e
        finally:
            caught = stop(caught)

    print(f"{server} stderr:", file=sys.stderr)
    print('\n'.join(logs), file=sys.stderr)
    print("----------------------", file=sys.stderr)

    raise caught
