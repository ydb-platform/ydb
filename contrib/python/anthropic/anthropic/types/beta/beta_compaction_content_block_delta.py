# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import Optional
from typing_extensions import Literal

from ..._models import BaseModel

__all__ = ["BetaCompactionContentBlockDelta"]


class BetaCompactionContentBlockDelta(BaseModel):
    content: Optional[str] = None

    type: Literal["compaction_delta"]
