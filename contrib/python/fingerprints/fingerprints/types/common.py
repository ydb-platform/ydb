import os
from typing import TypedDict, List

TYPES_PATH = os.path.join(os.path.dirname(__file__), "types.yml")


class TypeEntry(TypedDict):
    main: str
    forms: List[str]


class TypesList(TypedDict):
    types: List[TypeEntry]
