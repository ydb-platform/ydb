# -*- coding: utf-8 -*-


class MarshmallowMongoengineError(Exception):
    """Base exception class from which all exceptions related to
    marshmallow-mongoengine inherit.
    """

    pass


class ModelConversionError(MarshmallowMongoengineError):
    """Raised when an error occurs in converting a Mongoengine construct
    to a marshmallow object.
    """

    pass
