"""
This module provides backward compatibility
"""
# _token_map is needed by sqlalchemy_pytds connector
from .tds_session import (
    _token_map,  # noqa: F401 # _token_map is needed by sqlalchemy_pytds connector
)
from . import tds_base  # noqa: F401 # this is needed by sqlalchemy_pytds connector
