"""Async transport for the master's existing Unix console protocol."""

import asyncio
from pathlib import Path


class ConsoleError(Exception):
    pass


class CommandError(ConsoleError):
    """A complete ERROR response; the connection can still be used."""


class ConsoleConnection:
    PROMPT = b'\r\n> '
    MAX_RESPONSE = 128 * 1024 * 1024

    def __init__(self, path, timeout=10):
        self.path = str(Path(path).expanduser())
        self.timeout = timeout
        self._reader = self._writer = None
        self._lock = asyncio.Lock()

    async def close(self):
        writer, self._writer, self._reader = self._writer, None, None
        if writer is not None:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=1)
            except (OSError, asyncio.TimeoutError):
                pass

    async def _read(self):
        response = await self._reader.readuntil(self.PROMPT)
        return response[:-len(self.PROMPT)].decode().replace('\r\n', '\n')

    async def _exchange(self, command):
        if self._writer is None:
            self._reader, self._writer = await asyncio.open_unix_connection(self.path, limit=self.MAX_RESPONSE)
            await self._read()  # Greeting includes the initial stats and prompt.
        self._writer.write(command.encode() + b'\n')
        await self._writer.drain()
        return await self._read()

    async def request(self, command):
        if '\n' in command or '\r' in command or len(command.encode()) > 4094:
            raise CommandError('Use one command of at most 4094 UTF-8 bytes')
        if command.strip() in ('quit', 'exit'):
            raise CommandError('Quit is handled locally by the client')
        # Serialize polling and interactive commands; never resend a failed command.
        async with self._lock:
            try:
                response = await asyncio.wait_for(self._exchange(command), self.timeout)
            except asyncio.CancelledError:
                await self.close()
                raise
            except (OSError, EOFError, UnicodeError, asyncio.IncompleteReadError,
                    asyncio.LimitOverrunError, asyncio.TimeoutError) as exc:
                await self.close()
                detail = 'request timed out' if isinstance(exc, asyncio.TimeoutError) else str(exc)
                raise ConsoleError(f'{self.path}: {detail or type(exc).__name__}') from exc
        if response.startswith('ERROR: '):
            raise CommandError(response[7:])
        return response
