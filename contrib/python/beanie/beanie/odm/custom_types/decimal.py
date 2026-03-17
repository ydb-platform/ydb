import decimal

import bson
import pydantic
from typing_extensions import Annotated

DecimalAnnotation = Annotated[
    decimal.Decimal,
    pydantic.BeforeValidator(
        lambda v: v.to_decimal() if isinstance(v, bson.Decimal128) else v
    ),
]
