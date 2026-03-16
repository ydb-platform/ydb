from .primitives import Bool, Buffer, Int, Long
from .request import Request
from .response import Response


class ConnectRequest(Request):
    """ """

    parts = (
        ('protocol_version', Int),
        ('last_seen_zxid', Long),
        ('timeout', Int),
        ('session_id', Long),
        ('password', Buffer),
        ('read_only', Bool),
    )


class ConnectResponse(Response):
    """ """

    parts = (
        ('protocol_version', Int),
        ('timeout', Int),
        ('session_id', Long),
        ('password', Buffer),
    )
