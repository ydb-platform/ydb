from testsuite.utils import traceback


class BaseError(Exception):
    pass


class PostgresqlError(BaseError):
    pass


class NameCannotBeShortend(BaseError):
    pass


__tracebackhide__ = traceback.hide(BaseError)
