from collections.abc import Iterable
from logfire.integrations.sqlalchemy import CommenterOptions as CommenterOptions
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from typing import Any

def instrument_sqlalchemy(engine: AsyncEngine | Engine | None, engines: Iterable[AsyncEngine | Engine] | None, enable_commenter: bool, commenter_options: CommenterOptions, **kwargs: Any) -> None:
    """Instrument the `sqlalchemy` module so that spans are automatically created for each query.

    See the `Logfire.instrument_sqlalchemy` method for details.
    """
