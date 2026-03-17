# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Instrument `sqlalchemy`_ to report SQL queries.

There are two options for instrumenting code. The first option is to use
the ``opentelemetry-instrument`` executable which will automatically
instrument your SQLAlchemy engine. The second is to programmatically enable
instrumentation via the following code:

.. _sqlalchemy: https://pypi.org/project/sqlalchemy/

Usage
-----
.. code:: python

    from sqlalchemy import create_engine

    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    import sqlalchemy

    engine = create_engine("sqlite:///:memory:")
    SQLAlchemyInstrumentor().instrument(
        engine=engine,
    )

.. code:: python

    # of the async variant of SQLAlchemy

    from sqlalchemy.ext.asyncio import create_async_engine

    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    import sqlalchemy

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    SQLAlchemyInstrumentor().instrument(
        engine=engine.sync_engine
    )

Configuration
-------------

SQLCommenter
************
You can optionally enable sqlcommenter which enriches the query with contextual
information. Queries made after setting up trace integration with sqlcommenter
enabled will have configurable key-value pairs appended to them, e.g.
``"select * from auth_users; /*traceparent=00-01234567-abcd-01*/"``. This
supports context propagation between database client and server when database log
records are enabled. For more information, see:

* `Semantic Conventions - Database Spans <https://github.com/open-telemetry/semantic-conventions/blob/main/docs/database/database-spans.md#sql-commenter>`_
* `sqlcommenter <https://google.github.io/sqlcommenter/>`_

.. code:: python

    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    SQLAlchemyInstrumentor().instrument(enable_commenter=True)

SQLCommenter with commenter_options
***********************************
The key-value pairs appended to the query can be configured using
``commenter_options``. When sqlcommenter is enabled, all available KVs/tags
are calculated by default. ``commenter_options`` supports *opting out*
of specific KVs.

.. code:: python

    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    # Opts into sqlcomment for SQLAlchemy trace integration.
    # Opts out of tags for db_driver, db_framework.
    SQLAlchemyInstrumentor().instrument(
        enable_commenter=True,
        commenter_options={
            "db_driver": False,
            "db_framework": False,
        }
    )

Available commenter_options
###########################

The following sqlcomment key-values can be opted out of through ``commenter_options``:

+---------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| Commenter Option          | Description                                               | Example                                                                   |
+===========================+===========================================================+===========================================================================+
| ``db_driver``             | Database driver name.                                     | ``db_driver='psycopg2'``                                                  |
+---------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``db_framework``          | Database framework name with version.                     | ``db_framework='sqlalchemy:1.4.0'``                                       |
+---------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``opentelemetry_values``  | OpenTelemetry context as traceparent at time of query.    | ``traceparent='00-03afa25236b8cd948fa853d67038ac79-405ff022e8247c46-01'`` |
+---------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+

SQLComment in span attribute
****************************
If sqlcommenter is enabled, you can opt into the inclusion of sqlcomment in
the query span ``db.statement`` attribute for your needs. If ``commenter_options``
have been set, the span attribute comment will also be configured by this
setting.

.. code:: python

    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    # Opts into sqlcomment for SQLAlchemy trace integration.
    # Opts into sqlcomment for `db.statement` span attribute.
    SQLAlchemyInstrumentor().instrument(
        enable_commenter=True,
        commenter_options={},
        enable_attribute_commenter=True,
    )

Warning:
    Capture of sqlcomment in ``db.statement`` may have high cardinality without platform normalization. See `Semantic Conventions for database spans <https://opentelemetry.io/docs/specs/semconv/database/database-spans/#generating-a-summary-of-the-query-text>`_ for more information.

API
---
"""

from collections.abc import Sequence
from typing import Collection

import sqlalchemy
from packaging.version import parse as parse_version
from sqlalchemy.engine.base import Engine
from wrapt import wrap_function_wrapper as _w

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.sqlalchemy.engine import (
    EngineTracer,
    _wrap_connect,
    _wrap_create_async_engine,
    _wrap_create_engine,
)
from opentelemetry.instrumentation.sqlalchemy.package import _instruments
from opentelemetry.instrumentation.sqlalchemy.version import __version__
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import get_meter
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.trace import get_tracer


class SQLAlchemyInstrumentor(BaseInstrumentor):
    """An instrumentor for SQLAlchemy
    See `BaseInstrumentor`
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        """Instruments SQLAlchemy engine creation methods and the engine
        if passed as an argument.

        Args:
            **kwargs: Optional arguments
                ``engine``: a SQLAlchemy engine instance
                ``engines``: a list of SQLAlchemy engine instances
                ``tracer_provider``: a TracerProvider, defaults to global
                ``meter_provider``: a MeterProvider, defaults to global
                ``enable_commenter``: bool to enable sqlcommenter, defaults to False
                ``commenter_options``: dict of sqlcommenter config, defaults to {}
                ``enable_attribute_commenter``: bool to enable sqlcomment addition to span attribute, defaults to False. Must also set `enable_commenter`.

        Returns:
            An instrumented engine if passed in as an argument or list of instrumented engines, None otherwise.
        """
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        meter_provider = kwargs.get("meter_provider")
        meter = get_meter(
            __name__,
            __version__,
            meter_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        connections_usage = meter.create_up_down_counter(
            name=MetricInstruments.DB_CLIENT_CONNECTIONS_USAGE,
            unit="connections",
            description="The number of connections that are currently in state described by the state attribute.",
        )

        enable_commenter = kwargs.get("enable_commenter", False)
        commenter_options = kwargs.get("commenter_options", {})
        enable_attribute_commenter = kwargs.get(
            "enable_attribute_commenter", False
        )

        _w(
            "sqlalchemy",
            "create_engine",
            _wrap_create_engine(
                tracer,
                connections_usage,
                enable_commenter,
                commenter_options,
                enable_attribute_commenter,
            ),
        )
        _w(
            "sqlalchemy.engine",
            "create_engine",
            _wrap_create_engine(
                tracer,
                connections_usage,
                enable_commenter,
                commenter_options,
                enable_attribute_commenter,
            ),
        )
        # sqlalchemy.engine.create is not present in earlier versions of sqlalchemy (which we support)
        if parse_version(sqlalchemy.__version__).release >= (1, 4):
            _w(
                "sqlalchemy.engine.create",
                "create_engine",
                _wrap_create_engine(
                    tracer,
                    connections_usage,
                    enable_commenter,
                    commenter_options,
                    enable_attribute_commenter,
                ),
            )
        _w(
            "sqlalchemy.engine.base",
            "Engine.connect",
            _wrap_connect(tracer),
        )
        if parse_version(sqlalchemy.__version__).release >= (1, 4):
            _w(
                "sqlalchemy.ext.asyncio",
                "create_async_engine",
                _wrap_create_async_engine(
                    tracer,
                    connections_usage,
                    enable_commenter,
                    commenter_options,
                    enable_attribute_commenter,
                ),
            )
        if kwargs.get("engine") is not None:
            return EngineTracer(
                tracer,
                kwargs.get("engine"),
                connections_usage,
                kwargs.get("enable_commenter", False),
                kwargs.get("commenter_options", {}),
                kwargs.get("enable_attribute_commenter", False),
            )
        if kwargs.get("engines") is not None and isinstance(
            kwargs.get("engines"), Sequence
        ):
            return [
                EngineTracer(
                    tracer,
                    engine,
                    connections_usage,
                    kwargs.get("enable_commenter", False),
                    kwargs.get("commenter_options", {}),
                    kwargs.get("enable_attribute_commenter", False),
                )
                for engine in kwargs.get("engines")
            ]

        return None

    def _uninstrument(self, **kwargs):
        unwrap(sqlalchemy, "create_engine")
        unwrap(sqlalchemy.engine, "create_engine")
        if parse_version(sqlalchemy.__version__).release >= (1, 4):
            unwrap(sqlalchemy.engine.create, "create_engine")
        unwrap(Engine, "connect")
        if parse_version(sqlalchemy.__version__).release >= (1, 4):
            unwrap(sqlalchemy.ext.asyncio, "create_async_engine")
        EngineTracer.remove_all_event_listeners()
