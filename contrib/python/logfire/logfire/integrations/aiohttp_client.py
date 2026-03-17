from __future__ import annotations

from collections.abc import Callable
from typing import Union

from aiohttp.tracing import TraceRequestEndParams, TraceRequestExceptionParams, TraceRequestStartParams
from multidict import CIMultiDict, CIMultiDictProxy
from opentelemetry.trace import Span

AioHttpRequestHeaders = CIMultiDict[str]
AioHttpResponseHeaders = CIMultiDictProxy[str]
RequestHook = Callable[[Span, TraceRequestStartParams], None]
ResponseHook = Callable[[Span, Union[TraceRequestEndParams, TraceRequestExceptionParams]], None]
