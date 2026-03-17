from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from .. import Source, fo_o


class ChocolatE(BaseModel):
    flavour_name: Optional[str] = Field(None, alias='flavour-name')
    sourc_e: Optional[Source] = Field(None, alias='sourc-e')
    coco_a: Optional[fo_o.CocoA] = Field(None, alias='coco-a')
