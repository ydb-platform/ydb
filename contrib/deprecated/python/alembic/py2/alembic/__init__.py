import sys

from . import context  # noqa
from . import op  # noqa
from .runtime import environment
from .runtime import migration

__version__ = "1.6.5"

sys.modules["alembic.migration"] = migration
sys.modules["alembic.environment"] = environment
