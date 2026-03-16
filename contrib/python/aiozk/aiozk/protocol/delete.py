from .primitives import Int, UString
from .request import Request
from .response import Response


class DeleteRequest(Request):
    """ """

    opcode = 2

    writes_data = True

    parts = (
        ('path', UString),
        ('version', Int),
    )


class DeleteResponse(Response):
    """ """

    opcode = 2

    parts = ()
