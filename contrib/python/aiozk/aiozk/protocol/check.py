from .primitives import Int, UString
from .request import Request
from .response import Response


class CheckVersionRequest(Request):
    """ """

    opcode = 13

    parts = (
        ('path', UString),
        ('version', Int),
    )


class CheckVersionResponse(Response):
    """ """

    opcode = 13

    parts = ()
