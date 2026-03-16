from enum import Enum


class RDAPStatusCodes(int, Enum):
    """
    Status Code meanings for the RDAP protocol
    """

    POSITIVE_ANSWER_200 = 200
    NEGATIVE_ANSWER_404 = 404
    MALFORMED_QUERY_400 = 400
    RATE_LIMIT_429 = 429
    REDIRECT_302 = 302
