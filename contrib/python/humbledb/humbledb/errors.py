""" """


class NoConnection(RuntimeError):
    """Raised when a connection is needed."""


class NestedConnection(RuntimeError):
    """Raised when trying to nest the same connection within itself."""


class MissingConfig(RuntimeError):
    """Raised when configuartion is not configured correctly at runtime."""


class DatabaseMismatch(RuntimeError):
    """
    Raised when a user tries to use a document with a connection and the
    databases don't match.

    """


def _import_pymongo_errors():
    """Tries to add all the pymongo exceptions to this module's namespace."""
    import pymongo.errors

    _pymongo_errors = [
        "AutoReconnect",
        "BSONError",
        "CertificateError",
        "CollectionInvalid",
        "ConfigurationError",
        "ConnectionFailure",
        "DuplicateKeyError",
        "InvalidBSON",
        "InvalidDocument",
        "InvalidId",
        "InvalidName",
        "InvalidOperation",
        "InvalidStringData",
        "InvalidURI",
        "OperationFailure",
        "PyMongoError",
        "TimeoutError",
        "UnsupportedOption",
    ]

    for name in _pymongo_errors:
        try:
            globals()[name] = getattr(pymongo.errors, name)
        except AttributeError:
            pass


# Call the import helper and remove it
_import_pymongo_errors()
del _import_pymongo_errors
