from typing import Any

def instrument_aiohttp_server(**kwargs: Any):
    """Instrument the `aiohttp` module so that spans are automatically created for each server request.

    See the `Logfire.instrument_aiohttp_server` method for details.
    """
