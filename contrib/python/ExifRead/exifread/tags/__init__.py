"""
Tag definitions
"""

from typing import Callable, Dict, List, Tuple, Union

DEFAULT_STOP_TAG = "UNDEF"


# To ignore when quick processing
IGNORE_TAGS: List[int] = [
    0x02BC,  # XPM
    0x927C,  # MakerNote Tags
    0x9286,  # user comment
]

SubIfdTagDictValue = Tuple[str, Union[dict, Callable, None]]
SubIfdTagDict = Dict[int, SubIfdTagDictValue]
SubIfdInfoTuple = Tuple[str, SubIfdTagDict]

IfdDictValue = Union[Tuple[str, SubIfdInfoTuple], SubIfdTagDictValue]
IfdTagDict = Dict[int, IfdDictValue]
