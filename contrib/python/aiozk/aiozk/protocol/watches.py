from .primitives import Int, Long, UString, Vector
from .request import Request
from .response import Response


WATCH_XID = -1


class WatchEvent(Response):
    """ """

    CREATED = 1
    DELETED = 2
    DATA_CHANGED = 3
    CHILDREN_CHANGED = 4

    DISCONNECTED = 0
    CONNECTED = 3
    AUTH_FAILED = 4
    CONNECTED_READ_ONLY = 5
    SASL_AUTHENTICATED = 6
    SESSION_EXPIRED = -112

    parts = (
        ('type', Int),
        ('state', Int),
        ('path', UString),
    )


class SetWatchesRequest(Request):
    """ """

    opcode = 101

    parts = (
        ('relative_zxid', Long),
        ('data_watches', Vector.of(UString)),
        ('exist_watches', Vector.of(UString)),
        ('child_watches', Vector.of(UString)),
    )


class SetWatchesResponse(Response):
    """ """

    opcode = 101

    parts = ()


class CheckWatchesRequest(Request):
    """ """

    opcode = 17

    parts = (
        ('path', UString),
        ('type', Int),
    )


class CheckWatchesResponse(Response):
    """ """

    opcode = 17

    parts = ()


class RemoveWatchesRequest(Request):
    """ """

    opcode = 18

    parts = (
        ('path', UString),
        ('type', Int),
    )


class RemoveWatchesResponse(Response):
    """ """

    opcode = 18

    parts = ()
