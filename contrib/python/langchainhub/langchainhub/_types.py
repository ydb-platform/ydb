from typing import Any, Dict, List
from typing_extensions import TypedDict


class Repo(TypedDict, total=False):
    owner: str
    repo: str
    commit_hash: str
    manifest: Dict[str, Any]
    examples: List[dict]
