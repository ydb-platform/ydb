from typing import Any, List, Mapping, TypedDict

DocumentType = Mapping[str, Any]


class BuildInfo(TypedDict):
    ok: float
    version: str
    versionArray: List[int]
