from __future__ import annotations

import contextlib
import importlib
from importlib.util import find_spec
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from packaging.requirements import Requirement

from logfire import Logfire

if TYPE_CHECKING:  # pragma: no cover
    from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
    from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
    from psycopg import AsyncConnection, Connection
    from psycopg2._psycopg import connection as Psycopg2Connection
    from typing_extensions import TypeVar

    PsycopgConnection = TypeVar('PsycopgConnection', Connection[Any], AsyncConnection[Any], Psycopg2Connection)

    Instrumentor = PsycopgInstrumentor | Psycopg2Instrumentor


PACKAGE_NAMES: tuple[Literal['psycopg'], Literal['psycopg2']] = ('psycopg', 'psycopg2')


def instrument_psycopg(
    logfire_instance: Logfire,
    conn_or_module: ModuleType | Literal['psycopg', 'psycopg2'] | None | PsycopgConnection | Psycopg2Connection,
    **kwargs: Any,
) -> None:
    """Instrument a `psycopg` connection or module so that spans are automatically created for each query.

    See the `Logfire.instrument_psycopg` method for details.
    """
    if conn_or_module is None:
        # By default, instrument whichever libraries are installed.
        for package in PACKAGE_NAMES:
            if find_spec(package):  # pragma: no branch
                instrument_psycopg(logfire_instance, package, **kwargs)
        return
    elif conn_or_module in PACKAGE_NAMES:
        assert isinstance(conn_or_module, str)
        _instrument_psycopg(logfire_instance, name=conn_or_module, **kwargs)
        return
    elif isinstance(conn_or_module, ModuleType):
        module_name = cast(Literal['psycopg', 'psycopg2'], conn_or_module.__name__)
        instrument_psycopg(logfire_instance, module_name, **kwargs)
        return
    else:
        # Given an object that's not a module or string,
        # and whose class (or an ancestor) is defined in one of the packages, assume it's a connection object.
        for cls in conn_or_module.__class__.__mro__:
            package = cls.__module__.split('.')[0]
            if package in PACKAGE_NAMES:
                if kwargs:
                    raise TypeError(
                        f'Extra keyword arguments are only supported when instrumenting the {package} module, not a connection.'
                    )
                _instrument_psycopg(logfire_instance, package, conn_or_module, **kwargs)
                return

    raise ValueError(f"Don't know how to instrument {conn_or_module!r}")


def _instrument_psycopg(
    logfire_instance: Logfire,
    name: Literal['psycopg', 'psycopg2'],
    conn: Any = None,
    **kwargs: Any,
) -> None:
    instrumentor = _get_instrumentor(name)
    if conn is None:
        # OTEL looks at the installed packages to determine if the correct dependencies are installed.
        # This means that if a user installs `psycopg-binary` (which is commonly recommended)
        # then they can `import psycopg` but OTEL doesn't recognise this correctly.
        # So we implement an alternative strategy, which is to import `psycopg(2)` and check `__version__`.
        # If it matches, we can tell OTEL to skip the check so that it still works and doesn't log a warning.
        mod = importlib.import_module(name)
        skip_dep_check = check_version(name, mod.__version__, instrumentor)

        if kwargs.get('enable_commenter') and name == 'psycopg':
            import psycopg.pq

            # OTEL looks for __libpq_version__ which only exists in psycopg2.
            mod.__libpq_version__ = psycopg.pq.version()  # type: ignore

        instrumentor.instrument(
            skip_dep_check=skip_dep_check,
            **{
                'tracer_provider': logfire_instance.config.get_tracer_provider(),
                'meter_provider': logfire_instance.config.get_meter_provider(),
                **kwargs,
            },
        )
    else:
        # instrument_connection doesn't have a skip_dep_check argument.
        instrumentor.instrument_connection(conn, tracer_provider=logfire_instance.config.get_tracer_provider())  # type: ignore[reportUnknownMemberType]


def check_version(name: str, version: str, instrumentor: Instrumentor) -> bool:
    with contextlib.suppress(Exception):  # it's not worth raising an exception if this fails somehow.
        for dep in instrumentor.instrumentation_dependencies():
            req = Requirement(dep)  # dep is a string like 'psycopg >= 3.1.0'
            # The module __version__ can be something like '2.9.9 (dt dec pq3 ext lo64)', hence the split.
            if req.name == name and req.specifier.contains(version.split()[0]):
                return True
    return False


@overload
def _get_instrumentor(name: Literal['psycopg']) -> PsycopgInstrumentor: ...


@overload
def _get_instrumentor(name: Literal['psycopg2']) -> Psycopg2Instrumentor: ...


def _get_instrumentor(name: str) -> Instrumentor:
    try:
        instrumentor_module = importlib.import_module(f'opentelemetry.instrumentation.{name}')
    except ImportError:
        raise ImportError(f"Run `pip install 'logfire[{name}]'` to install {name} instrumentation.")

    return getattr(instrumentor_module, f'{name.capitalize()}Instrumentor')()
