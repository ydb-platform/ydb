class MarshmallowSQLAlchemyError(Exception):
    """Base exception class from which all exceptions related to
    marshmallow-sqlalchemy inherit.
    """


class ModelConversionError(MarshmallowSQLAlchemyError):
    """Raised when an error occurs in converting a SQLAlchemy construct
    to a marshmallow object.
    """


class IncorrectSchemaTypeError(ModelConversionError):
    """Raised when a ``SQLAlchemyAutoField`` is bound to ``Schema`` that
    is not an instance of ``SQLAlchemySchema``.
    """
