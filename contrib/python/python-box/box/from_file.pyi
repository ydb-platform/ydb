from box.box import Box as Box
from box.box_list import BoxList as BoxList
from os import PathLike
from typing import Any

def box_from_file(
    file: str | PathLike,
    file_type: str = ...,
    encoding: str = ...,
    errors: str = ...,
    **kwargs: Any,
) -> Box | BoxList: ...
def box_from_string(
    content: str,
    string_type: str = ...,
) -> Box | BoxList: ...
