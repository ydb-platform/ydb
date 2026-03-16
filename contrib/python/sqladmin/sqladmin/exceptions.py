class SQLAdminException(Exception):
    pass


class InvalidModelError(SQLAdminException):
    pass


class NoConverterFound(SQLAdminException):
    pass
