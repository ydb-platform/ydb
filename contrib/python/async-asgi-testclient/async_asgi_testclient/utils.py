from functools import partial
from multidict import CIMultiDict
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import quote
from urllib.parse import urlencode

import asyncio
import re
import sys


def flatten_headers(headers: Union[Dict, CIMultiDict]) -> List[Tuple]:
    return [(bytes(k.lower(), "utf8"), bytes(v, "utf8")) for k, v in headers.items()]


def make_test_headers_path_and_query_string(
    app: Any,
    path: str,
    headers: Optional[Union[dict, CIMultiDict]] = None,
    query_string: Optional[dict] = None,
) -> Tuple[CIMultiDict, str, bytes]:
    """Make the headers and path with defaults for testing.

    Arguments:
        app: The application to test against.
        path: The path to request. If the query_string argument is not
            defined this argument will be partitioned on a '?' with
            the following part being considered the query_string.
        headers: Initial headers to send.
        query_string: To send as a dictionary, alternatively the
            query_string can be determined from the path.
    """
    if headers is None:
        headers = CIMultiDict()
    elif isinstance(headers, CIMultiDict):
        headers = headers
    elif headers is not None:
        headers = CIMultiDict(headers)
    headers.setdefault("Remote-Addr", "127.0.0.1")
    headers.setdefault("User-Agent", "ASGI-Test-Client")
    headers.setdefault("host", "localhost")

    if "?" in path and query_string is not None:
        raise ValueError("Query string is defined in the path and as an argument")
    if query_string is None:
        path, _, query_string_raw = path.partition("?")
        query_string_raw = quote(query_string_raw, safe="&=")
    else:
        query_string_raw = urlencode(query_string, doseq=True)
    query_string_bytes = query_string_raw.encode("ascii")
    return headers, path, query_string_bytes


def to_relative_path(path: str):
    if path.startswith("/"):
        return path
    return re.sub(r"^[a-zA-Z]+://[^/]+/", "/", path)


async def is_last_one(gen):
    prev_el = None
    async for el in gen:
        prev_el = el
        async for el in gen:
            yield (False, prev_el)
            prev_el = el
        yield (True, prev_el)


class Message:
    def __init__(self, event, reason, task):
        self.event: str = event
        self.reason: str = reason
        self.task: asyncio.Task = task


def create_monitored_task(coro, send):
    future = asyncio.ensure_future(coro)
    future.add_done_callback(partial(_callback, send))
    return future


async def receive(ch, timeout=None):
    fut = set_timeout(ch, timeout)
    msg = await ch.get()
    if not fut.cancelled():
        fut.cancel()
    if isinstance(msg, Message):
        if msg.event == "err":
            raise msg.reason
    return msg


def _callback(send, fut):
    try:
        fut.result()
    except asyncio.CancelledError:
        send(Message("exit", "killed", fut))
        raise
    except Exception as e:
        send(Message("err", e, fut))
    else:
        send(Message("exit", "normal", fut))


async def _send_after(timeout, queue, msg):
    if timeout is None:
        return
    await asyncio.sleep(timeout)
    await queue.put(msg)


def set_timeout(queue, timeout):
    msg = Message("err", asyncio.TimeoutError, current_task())
    return asyncio.ensure_future(_send_after(timeout, queue, msg))


def current_task():
    PY37 = sys.version_info >= (3, 7)
    if PY37:
        return asyncio.current_task()
    else:
        return asyncio.Task.current_task()
