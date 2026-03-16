import sys

if sys.version_info >= (3, 11):  # pragma: no cover
    from enum import StrEnum
else:  # pragma: no cover
    from enum import Enum

    class StrEnum(str, Enum):
        pass


class HeaderKeys(StrEnum):
    api_key = "X-API-Key"
    correlation_id = "X-Correlation-ID"
    request_id = "X-Request-ID"
    date = "Date"
    forwarded_for = "X-Forwarded-For"
    user_agent = "User-Agent"
