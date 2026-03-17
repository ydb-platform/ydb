import re

import bson
import pydantic
from typing_extensions import Annotated

from beanie.odm.utils.pydantic import IS_PYDANTIC_V2


def _to_bson_regex(v):
    return v.try_compile() if isinstance(v, bson.Regex) else v


if IS_PYDANTIC_V2:
    Pattern = Annotated[
        re.Pattern,
        pydantic.BeforeValidator(
            lambda v: v.try_compile() if isinstance(v, bson.Regex) else v
        ),
    ]
else:

    class Pattern(bson.Regex):  # type: ignore[no-redef]
        @classmethod
        def __get_validators__(cls):
            yield _to_bson_regex
