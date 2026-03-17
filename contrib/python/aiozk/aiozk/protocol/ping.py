from .request import Request
from .response import Response


PING_XID = -2


class PingRequest(Request):
    """ """

    opcode = 11
    special_xid = PING_XID

    parts = ()


class PingResponse(Response):
    """ """

    opcode = 11

    parts = ()
