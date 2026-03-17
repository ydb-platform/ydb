from .primitives import Long, UString
from .request import Request
from .response import Response
from .stat import Stat


class ReconfigRequest(Request):
    """ """

    opcode = 16

    parts = (
        ('joining_servers', UString),
        ('leaving_servers', UString),
        ('new_members', UString),
        ('current_config_id', Long),
    )


class ReconfigResponse(Response):
    """ """

    opcode = 16

    parts = (('stat', Stat),)
