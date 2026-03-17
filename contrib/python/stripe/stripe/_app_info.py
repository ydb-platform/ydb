from typing import Optional
from typing_extensions import TypedDict


class AppInfo(TypedDict):
    name: str
    partner_id: Optional[str]
    url: Optional[str]
    version: Optional[str]
