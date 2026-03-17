# ----------------------------------------------------------------------
# Version API
#
# We provide three symbols:
# - duckdb.__version__: The version of this package
# - duckdb.__duckdb_version__: The version of duckdb that is bundled
# - duckdb.version(): A human-readable version string containing both of the above
# ----------------------------------------------------------------------
from importlib.metadata import version as _dist_version

import _duckdb

__version__: str = _dist_version("duckdb")
"""Version of the DuckDB Python Package."""

__duckdb_version__: str = _duckdb.__version__
"""Version of DuckDB that is bundled."""


def version() -> str:
    """Human-friendly formatted version string of both the distribution package and the bundled DuckDB engine."""
    return f"{__version__} (with duckdb {_duckdb.__version__})"
