from .request import Request
from .response import Response


CLOSE_XID = None


class CloseRequest(Request):
    """ """

    opcode = -11
    special_xid = CLOSE_XID

    parts = ()


class CloseResponse(Response):
    """ """

    opcode = -11

    parts = ()
