"""Telnet relay server shell implementation."""

# std imports
import asyncio
import logging
from typing import Any, Set, Union, cast

# local
from .client import open_connection
from .accessories import make_reader_task
from .server_shell import readline
from .stream_reader import TelnetReader, TelnetReaderUnicode
from .stream_writer import TelnetWriter, TelnetWriterUnicode

CR, LF, NUL = ("\r", "\n", "\x00")

# std imports

# local


async def relay_shell(
    client_reader: Union[TelnetReader, TelnetReaderUnicode],
    client_writer: Union[TelnetWriter, TelnetWriterUnicode],
) -> None:
    """
    Example telnet relay shell for use with telnetlib3.create_server.

    Run command::

        telnetlib3 --shell telnetlib3.relay_server.relay_shell

    This relay service is very basic, it still needs to somehow forward the TERM
    type and environment variable of value COLORTERM.
    """
    log = logging.getLogger("relay_server")
    _reader = cast(TelnetReaderUnicode, client_reader)
    _writer = cast(TelnetWriterUnicode, client_writer)

    password_prompt = readline(_reader, _writer)
    next(password_prompt)

    _writer.write("Telnet Relay shell ready." + CR + LF + CR + LF)

    client_passcode = "867-5309"
    num_tries = 3
    next_host, next_port = "1984.ws", 23
    passcode = None
    for _ in range(num_tries):
        _writer.write("Passcode: ")
        while passcode is None:
            inp = await _reader.read(1)
            if not inp:
                log.info("EOF from client")
                return
            passcode = password_prompt.send(inp)
        await asyncio.sleep(1)
        _writer.write(CR + LF)
        if passcode == client_passcode:
            log.info("passcode accepted")
            break
        passcode = None

    # wrong passcode after 3 tires
    if passcode is None:
        log.info("passcode failed after %s tries", num_tries)
        _writer.close()
        return

    # connect to another telnet server (next_host, next_port)
    _writer.write(f"Connecting to {next_host}:{next_port} ... ")
    server_reader, server_writer = await open_connection(
        next_host,
        next_port,
        cols=_writer.get_extra_info("cols"),
        rows=_writer.get_extra_info("rows"),
    )
    _writer.write("connected!" + CR + LF)

    done: Set["asyncio.Task[Any]"] = set()
    client_stdin = make_reader_task(_reader)
    server_stdout = make_reader_task(server_reader)
    wait_for = {client_stdin, server_stdout}
    while wait_for:
        done, _ = await asyncio.wait(wait_for, return_when=asyncio.FIRST_COMPLETED)
        while done:
            task = done.pop()
            wait_for.remove(task)
            if task == client_stdin:
                inp = task.result()
                if inp:
                    server_writer.write(inp)
                    client_stdin = make_reader_task(_reader)
                    wait_for.add(client_stdin)
                else:
                    log.info("EOF from client")
                    server_writer.close()
            elif task == server_stdout:
                out = task.result()
                if out:
                    _writer.write(out)
                    server_stdout = make_reader_task(server_reader)
                    wait_for.add(server_stdout)
                else:
                    log.info("EOF from server")
                    _writer.close()
    log.info("No more tasks: relay server complete")
