from __future__ import annotations

import contextlib
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    from logfire.integrations.sqlalchemy import CommenterOptions
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_sqlalchemy()` requires the `opentelemetry-instrumentation-sqlalchemy` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[sqlalchemy]'"
    )

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine


def _convert_to_sync_engine(engine: AsyncEngine | Engine | None) -> Engine | None:
    from sqlalchemy.ext.asyncio import AsyncEngine

    if isinstance(engine, AsyncEngine):
        return engine.sync_engine
    return engine


def instrument_sqlalchemy(
    engine: AsyncEngine | Engine | None,
    engines: Iterable[AsyncEngine | Engine] | None,
    enable_commenter: bool,
    commenter_options: CommenterOptions,
    **kwargs: Any,
) -> None:
    """Instrument the `sqlalchemy` module so that spans are automatically created for each query.

    See the `Logfire.instrument_sqlalchemy` method for details.
    """
    with contextlib.suppress(ImportError):
        engine = _convert_to_sync_engine(engine)

        if engines is not None:
            engines = [_convert_to_sync_engine(engine_entry) for engine_entry in engines]  # type: ignore

    return SQLAlchemyInstrumentor().instrument(
        engine=engine, engines=engines, enable_commenter=enable_commenter, commenter_options=commenter_options, **kwargs
    )
