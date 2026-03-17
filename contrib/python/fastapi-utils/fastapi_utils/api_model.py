from __future__ import annotations

from functools import partial

import pydantic
from pydantic import BaseModel

from .camelcase import snake2camel

PYDANTIC_VERSION = pydantic.VERSION

if PYDANTIC_VERSION[0] == "2":
    from pydantic import ConfigDict
else:
    from pydantic import BaseConfig


class APIModel(BaseModel):
    """
    Intended for use as a base class for externally-facing models.

    Any models that inherit from this class will:
    * accept fields using snake_case or camelCase keys
    * use camelCase keys in the generated OpenAPI spec
    * have orm_mode on by default
        * Because of this, FastAPI will automatically attempt to parse returned orm instances into the model
    """

    if PYDANTIC_VERSION[0] == "2":
        model_config = ConfigDict(
            from_attributes=True, populate_by_name=True, alias_generator=partial(snake2camel, start_lower=True)
        )
    else:

        class Config(BaseConfig):
            orm_mode = True
            allow_population_by_field_name = True
            alias_generator = partial(snake2camel, start_lower=True)


class APIMessage(APIModel):
    """
    A lightweight utility class intended for use with simple message-returning endpoints.
    """

    detail: str
