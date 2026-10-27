import asyncio
from pathlib import Path
import tempfile
import unittest

from ydb.apps.dstool.tui.connection import ConsoleConnection, ConsoleError, CommandError


class ConsoleConnectionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name) / 'console.sock')
        self.commands = []
        self.connections = 0
        self.handlers = set()
        self.writers = set()
        self.response = lambda command: 'OK'
        self.server = await asyncio.start_unix_server(self.handle, path=self.path)
        self.client = ConsoleConnection(self.path, timeout=2)

    async def asyncTearDown(self):
        await self.client.close()
        self.server.close()
        for task in list(self.handlers):
            task.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)
        await self.server.wait_closed()
        for writer in list(self.writers):
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
        self.temp.cleanup()

    async def handle(self, reader, writer):
        self.connections += 1
        self.handlers.add(asyncio.current_task())
        self.writers.add(writer)
        try:
            writer.write(b'blobsan-service console. help for commands.\r\n{}\r\n> ')
            await writer.drain()
            while line := await reader.readline():
                command = line.decode().strip()
                self.commands.append(command)
                result = self.response(command)
                if asyncio.iscoroutine(result):
                    result = await result
                if result is None:
                    return
                frame = (result.replace('\n', '\r\n') + '\r\n> ').encode()
                for offset in range(0, len(frame), 4093):
                    writer.write(frame[offset:offset + 4093])
                    await writer.drain()
                    await asyncio.sleep(0)
        except (ConnectionError, BrokenPipeError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            finally:
                self.writers.discard(writer)
                self.handlers.discard(asyncio.current_task())

    async def test_fragmented_large_replies_and_empty_result(self):
        large = '1+1+?\n' * 30000
        self.response = lambda command: large if command == 'pools' else '(empty)'
        self.assertEqual(await self.client.request('pools'), large)
        self.assertEqual(await self.client.request('groups brief'), '(empty)')
        self.assertEqual(self.connections, 1)

    async def test_command_error_keeps_connection_and_commands_are_serialized(self):
        self.response = lambda command: 'ERROR: Unknown command' if command == 'bad' else command
        with self.assertRaisesRegex(CommandError, 'Unknown command'):
            await self.client.request('bad')
        self.assertEqual(await asyncio.gather(self.client.request('stats'), self.client.request('pause')),
                         ['stats', 'pause'])
        self.assertEqual(self.connections, 1)

    async def test_disconnect_does_not_retry_command_and_next_poll_reconnects(self):
        self.response = lambda command: None if command == 'pause' else '{}'
        with self.assertRaises(ConsoleError):
            await self.client.request('pause')
        self.assertEqual(await self.client.request('stats'), '{}')
        self.assertEqual(self.commands, ['pause', 'stats'])
        self.assertEqual(self.connections, 2)

    async def test_timeout_does_not_retry_command(self):
        async def response(command):
            if command == 'scan all':
                await asyncio.Event().wait()
            return '{}'
        self.response = response
        self.client.timeout = 0.05
        with self.assertRaisesRegex(ConsoleError, 'timed out'):
            await self.client.request('scan all')
        self.client.timeout = 2
        self.assertEqual(await self.client.request('stats'), '{}')
        self.assertEqual(self.commands, ['scan all', 'stats'])

    async def test_cancellation_does_not_mix_responses(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def response(command):
            if command == 'slow':
                started.set()
                await release.wait()
            return command
        self.response = response
        active = asyncio.create_task(self.client.request('slow'))
        await asyncio.wait_for(started.wait(), 2)
        queued = asyncio.create_task(self.client.request('pause'))
        await asyncio.sleep(0)
        queued.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await queued
        release.set()
        self.assertEqual(await active, 'slow')
        self.assertEqual(await self.client.request('stats'), 'stats')
        self.assertEqual(self.connections, 1)
        release.clear()
        started.clear()
        active = asyncio.create_task(self.client.request('slow'))
        await asyncio.wait_for(started.wait(), 2)
        active.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await active
        self.assertEqual(await self.client.request('stats'), 'stats')
        self.assertEqual(self.connections, 2)
        self.assertNotIn('pause', self.commands)
        release.set()

    async def test_rejects_multiline_oversized_and_quit_before_connecting(self):
        for command in ('pause\nresume', 'pause\rresume', 'я' * 2048, 'quit', 'exit'):
            with self.subTest(command=command[:20]), self.assertRaises(CommandError):
                await self.client.request(command)
        self.assertEqual(self.connections, 0)
