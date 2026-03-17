from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, no_type_check

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.base import UUID
from sqlalchemy.sql.sqltypes import CHAR
from sqlalchemy.sql.type_api import TypeDecorator

# Use the following as the value of server_default for primary keys of type GUID
GUID_SERVER_DEFAULT_POSTGRESQL = sa.DefaultClause(sa.text("gen_random_uuid()"))
GUID_DEFAULT_SQLITE = uuid.uuid4

if TYPE_CHECKING:
    UUIDTypeDecorator = TypeDecorator[uuid.UUID]
else:
    UUIDTypeDecorator = TypeDecorator


class GUID(UUIDTypeDecorator):
    """
    Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses CHAR(32), storing as stringified hex values.

    Taken from SQLAlchemy docs: https://docs.sqlalchemy.org/en/13/core/custom_types.html#backend-agnostic-guid-type
    """

    impl = CHAR
    cache_ok = True

    @no_type_check
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @no_type_check
    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":  # pragma: no cover
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    @no_type_check
    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == "postgresql":  # pragma: no cover
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    @no_type_check
    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):  # pragma: no branch
                value = uuid.UUID(value)
            return value


def setup_guids_postgresql(engine: sa.engine.Engine) -> None:  # pragma: no cover
    """
    Set up UUID generation using the pgcrypto extension for postgres

    This query only needs to be executed once when the database is created
    """
    engine.execute('create EXTENSION if not EXISTS "pgcrypto"')
