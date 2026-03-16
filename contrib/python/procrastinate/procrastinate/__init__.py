from __future__ import annotations

from procrastinate import metadata as _metadata_module
from procrastinate.app import App
from procrastinate.blueprints import Blueprint
from procrastinate.connector import BaseConnector
from procrastinate.job_context import JobContext
from procrastinate.psycopg_connector import PsycopgConnector
from procrastinate.retry import BaseRetryStrategy, RetryDecision, RetryStrategy
from procrastinate.sync_psycopg_connector import SyncPsycopgConnector
from procrastinate.utils import MovedElsewhere as _MovedElsewhere

AiopgConnector = _MovedElsewhere(
    name="AiopgConnector",
    new_location="procrastinate.contrib.aiopg.AiopgConnector",
)
Psycopg2Connector = _MovedElsewhere(
    name="Psycopg2Connector",
    new_location="procrastinate.contrib.psycopg2.Psycopg2Connector",
)

__all__ = [
    "App",
    "BaseConnector",
    "BaseRetryStrategy",
    "Blueprint",
    "JobContext",
    "PsycopgConnector",
    "RetryDecision",
    "RetryStrategy",
    "SyncPsycopgConnector",
]


_metadata = _metadata_module.extract_metadata()
__author__ = _metadata["author"]
__author_email__ = _metadata["email"]
__license__ = _metadata["license"]
__url__ = _metadata["url"]
__version__ = _metadata["version"]
