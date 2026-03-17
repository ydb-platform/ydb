from aiohttp.tracing import TraceRequestEndParams, TraceRequestExceptionParams, TraceRequestStartParams
from collections.abc import Callable
from multidict import CIMultiDict, CIMultiDictProxy
from opentelemetry.trace import Span

AioHttpRequestHeaders = CIMultiDict[str]
AioHttpResponseHeaders = CIMultiDictProxy[str]
RequestHook = Callable[[Span, TraceRequestStartParams], None]
ResponseHook = Callable[[Span, TraceRequestEndParams | TraceRequestExceptionParams], None]
