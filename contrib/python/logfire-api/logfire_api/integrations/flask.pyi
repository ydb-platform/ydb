from _typeshed import Incomplete
from typing import TypedDict
from wsgiref.types import WSGIEnvironment as WSGIEnvironment

RequestHook: Incomplete
ResponseHook: Incomplete

class CommenterOptions(TypedDict, total=False):
    """The `commenter_options` parameter for `instrument_flask`."""
    framework: bool
    route: bool
    controller: bool
