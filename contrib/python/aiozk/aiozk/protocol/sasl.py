from .primitives import Buffer
from .request import Request
from .response import Response


class SASLRequest(Request):
    """ """

    opcode = 102

    parts = ('token', Buffer)


class SASLResponse(Response):
    """ """

    opcode = 102

    parts = ('token', Buffer)
