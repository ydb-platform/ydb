from typing import Any

import bson
import pydantic
from typing_extensions import Annotated

from beanie.odm.utils.pydantic import IS_PYDANTIC_V2


def _to_bson_binary(value: Any) -> bson.Binary:
    return value if isinstance(value, bson.Binary) else bson.Binary(value)


if IS_PYDANTIC_V2:
    BsonBinary = Annotated[
        bson.Binary, pydantic.PlainValidator(_to_bson_binary)
    ]
else:

    class BsonBinary(bson.Binary):  # type: ignore[no-redef]
        @classmethod
        def __get_validators__(cls):
            yield _to_bson_binary
