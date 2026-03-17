from .primitives import Bool, UString
from .request import Request
from .response import Response
from .stat import Stat


class ExistsRequest(Request):
    """ """

    opcode = 3

    parts = (
        ('path', UString),
        ('watch', Bool),
    )


class ExistsResponse(Response):
    """ """

    opcode = 3

    parts = (('stat', Stat),)
