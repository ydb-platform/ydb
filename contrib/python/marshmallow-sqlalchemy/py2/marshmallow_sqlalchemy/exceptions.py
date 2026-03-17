# -*- coding: utf-8 -*-


class MarshmallowSQLAlchemyError(Exception):
    """Base exception class from which all exceptions related to
    marshmallow-sqlalchemy inherit.
    """

    pass


class ModelConversionError(MarshmallowSQLAlchemyError):
    """Raised when an error occurs in converting a SQLAlchemy construct
    to a marshmallow object.
    """

    pass
