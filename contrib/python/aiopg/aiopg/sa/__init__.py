"""Optional support for sqlalchemy.sql dynamic query generation."""
from .connection import SAConnection
from .engine import Engine, create_engine
from .exc import (
    ArgumentError,
    Error,
    InvalidRequestError,
    NoSuchColumnError,
    ResourceClosedError,
)

__all__ = (
    "create_engine",
    "SAConnection",
    "Error",
    "ArgumentError",
    "InvalidRequestError",
    "NoSuchColumnError",
    "ResourceClosedError",
    "Engine",
)


(
    SAConnection,
    Error,
    ArgumentError,
    InvalidRequestError,
    NoSuchColumnError,
    ResourceClosedError,
    create_engine,
    Engine,
)
