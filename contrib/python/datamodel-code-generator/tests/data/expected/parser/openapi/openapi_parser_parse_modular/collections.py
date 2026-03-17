from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import AnyUrl, BaseModel

from . import models


class Pets(BaseModel):
    __root__: List[models.Pet]


class Users(BaseModel):
    __root__: List[models.User]


class Rules(BaseModel):
    __root__: List[str]


class Stage(Enum):
    test = 'test'
    dev = 'dev'
    stg = 'stg'
    prod = 'prod'


class Api(BaseModel):
    apiKey: Optional[str] = None
    apiVersionNumber: Optional[str] = None
    apiUrl: Optional[AnyUrl] = None
    apiDocumentationUrl: Optional[AnyUrl] = None
    stage: Optional[Stage] = None


class Apis(BaseModel):
    __root__: List[Api]
