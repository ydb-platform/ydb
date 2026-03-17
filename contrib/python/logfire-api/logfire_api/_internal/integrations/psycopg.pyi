from logfire import Logfire as Logfire
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from psycopg import AsyncConnection, Connection
from psycopg2._psycopg import connection as Psycopg2Connection
from types import ModuleType
from typing import Any, Literal
from typing_extensions import TypeVar

PsycopgConnection = TypeVar('PsycopgConnection', Connection[Any], AsyncConnection[Any], Psycopg2Connection)
Instrumentor = PsycopgInstrumentor | Psycopg2Instrumentor
PACKAGE_NAMES: tuple[Literal['psycopg'], Literal['psycopg2']]

def instrument_psycopg(logfire_instance: Logfire, conn_or_module: ModuleType | Literal['psycopg', 'psycopg2'] | None | PsycopgConnection | Psycopg2Connection, **kwargs: Any) -> None:
    """Instrument a `psycopg` connection or module so that spans are automatically created for each query.

    See the `Logfire.instrument_psycopg` method for details.
    """
def check_version(name: str, version: str, instrumentor: Instrumentor) -> bool: ...
