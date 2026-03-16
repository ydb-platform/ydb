import uuid

from sqlalchemy import types, util
from sqlalchemy.dialects import mssql, postgresql

from ..compat import get_sqlalchemy_version
from .scalar_coercible import ScalarCoercible

sqlalchemy_version = get_sqlalchemy_version()


class UUIDType(ScalarCoercible, types.TypeDecorator):
    """
    Stores a UUID in the database natively when it can and falls back to
    a BINARY(16) or a CHAR(32) when it can't.

    ::

        from sqlalchemy_utils import UUIDType
        import uuid

        class User(Base):
            __tablename__ = 'user'

            # Pass `binary=False` to fallback to CHAR instead of BINARY
            id = sa.Column(
                UUIDType(binary=False),
                primary_key=True,
                default=uuid.uuid4
            )
    """
    impl = types.BINARY(16)

    python_type = uuid.UUID

    cache_ok = True

    def __init__(self, binary=True, native=True):
        """
        :param binary: Whether to use a BINARY(16) or CHAR(32) fallback.
        """
        self.binary = binary
        self.native = native

    def __repr__(self):
        return util.generic_repr(self)

    def load_dialect_impl(self, dialect):
        if self.native and dialect.name in ('postgresql', 'cockroachdb'):
            # Use the native UUID type.
            return dialect.type_descriptor(postgresql.UUID())

        if dialect.name == 'mssql' and self.native:
            # Use the native UNIQUEIDENTIFIER type.
            return dialect.type_descriptor(mssql.UNIQUEIDENTIFIER())

        else:
            # Fallback to either a BINARY or a CHAR.
            kind = self.impl if self.binary else types.CHAR(32)
            return dialect.type_descriptor(kind)

    @staticmethod
    def _coerce(value):
        if value and not isinstance(value, uuid.UUID):
            try:
                value = uuid.UUID(value)

            except (TypeError, ValueError):
                value = uuid.UUID(bytes=value)

        return value

    # sqlalchemy >= 1.4.30 quotes UUID's automatically.
    # It is only necessary to quote UUID's in sqlalchemy < 1.4.30.
    if sqlalchemy_version < (1, 4, 30):
        def process_literal_param(self, value, dialect):
            return f"'{value}'" if value else value
    else:
        def process_literal_param(self, value, dialect):
            return value

    def process_bind_param(self, value, dialect):
        if value is None:
            return value

        if not isinstance(value, uuid.UUID):
            value = self._coerce(value)

        if self.native and dialect.name in (
            'postgresql',
            'mssql',
            'cockroachdb'
        ):
            return str(value)

        return value.bytes if self.binary else value.hex

    def process_result_value(self, value, dialect):
        if value is None:
            return value

        if self.native and dialect.name in (
            'postgresql',
            'mssql',
            'cockroachdb'
        ):
            if isinstance(value, uuid.UUID):
                # Some drivers convert PostgreSQL's uuid values to
                # Python's uuid.UUID objects by themselves
                return value
            return uuid.UUID(value)

        return uuid.UUID(bytes=value) if self.binary else uuid.UUID(value)
