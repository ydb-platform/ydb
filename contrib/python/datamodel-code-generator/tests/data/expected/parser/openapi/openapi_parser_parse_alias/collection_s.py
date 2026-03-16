from __future__ import annotations

from typing import List, Optional

from pydantic import AnyUrl, BaseModel, Field

from . import model_s


class PetS(BaseModel):
    __root__: List[model_s.PeT]


class UserS(BaseModel):
    __root__: List[model_s.UseR]


class RuleS(BaseModel):
    __root__: List[str]


class Api(BaseModel):
    apiKey: Optional[str] = Field(
        None, description='To be used as a dataset parameter value'
    )
    apiVersionNumber: Optional[str] = Field(
        None, description='To be used as a version parameter value'
    )
    apiUrl: Optional[AnyUrl] = Field(
        None, description="The URL describing the dataset's fields"
    )
    apiDocumentationUrl: Optional[AnyUrl] = Field(
        None, description='A URL to the API console for each API'
    )


class ApiS(BaseModel):
    __root__: List[Api]
