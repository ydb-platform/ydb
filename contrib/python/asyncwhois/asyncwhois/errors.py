class WhoIsError(Exception):
    pass


class QueryError(WhoIsError):
    pass


class NotFoundError(WhoIsError):
    pass


class GeneralError(WhoIsError):
    pass
