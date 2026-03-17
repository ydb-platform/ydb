from .primitives import Buffer, Int, UString
from .request import Request
from .response import Response


AUTH_XID = -4


class AuthRequest(Request):
    """ """

    opcode = 100
    special_xid = AUTH_XID

    parts = (
        ('type', Int),
        ('scheme', UString),
        ('auth', Buffer),
    )


class AuthResponse(Response):
    """ """

    opcode = 100

    parts = ()
