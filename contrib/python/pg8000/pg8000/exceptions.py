class Error(Exception):
    """Generic exception that is the base exception of all other error
    exceptions.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    pass


class InterfaceError(Error):
    """Generic exception raised for errors that are related to the database
    interface rather than the database itself.  For example, if the interface
    attempts to use an SSL connection but the server refuses, an InterfaceError
    will be raised.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    pass


class DatabaseError(Error):
    """Generic exception raised for errors that are related to the database.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    pass
