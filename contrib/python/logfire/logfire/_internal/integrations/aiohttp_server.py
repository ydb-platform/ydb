import warnings
from typing import Any

try:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        from opentelemetry.instrumentation.aiohttp_server import AioHttpServerInstrumentor

except ImportError:  # pragma: no cover
    raise RuntimeError(
        '`logfire.instrument_aiohttp_server()` requires the `opentelemetry-instrumentation-aiohttp-server` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[aiohttp-server]'"
    )


def instrument_aiohttp_server(**kwargs: Any):
    """Instrument the `aiohttp` module so that spans are automatically created for each server request.

    See the `Logfire.instrument_aiohttp_server` method for details.
    """
    AioHttpServerInstrumentor().instrument(**kwargs)
