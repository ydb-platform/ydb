class WhodapError(Exception):
    ...


class NotFoundError(WhodapError):
    ...


class RateLimitError(WhodapError):
    ...


class MalformedQueryError(WhodapError):
    ...


class BadStatusCode(WhodapError):
    ...


class RDAPConformanceException(WhodapError):
    ...
