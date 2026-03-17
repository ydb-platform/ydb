from typing import Any

def instrument_asyncpg(**kwargs: Any) -> None:
    """Instrument the `asyncpg` module so that spans are automatically created for each query.

    See the `Logfire.instrument_asyncpg` method for details.
    """
