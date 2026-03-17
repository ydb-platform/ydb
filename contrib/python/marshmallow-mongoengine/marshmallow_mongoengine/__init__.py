# -*- coding: utf-8 -*-
from __future__ import absolute_import

try:
    import marshmallow
except ImportError:
    raise ImportError("marshmallow is not installed.")
else:
    del marshmallow

from marshmallow_mongoengine.schema import (
    SchemaOpts,
    ModelSchema,
)

from marshmallow_mongoengine.conversion.fields import (
    register_field,
    register_field_builder,
)

from marshmallow_mongoengine.convert import (
    ModelConverter,
    fields_for_model,
    convert_field,
    field_for,
)
from marshmallow_mongoengine.exceptions import ModelConversionError

__version__ = "0.31.2"
__license__ = "MIT"

__all__ = [
    "ModelSchema",
    "SchemaOpts",
    "ModelConverter",
    "fields_for_model",
    "property2field",
    "column2field",
    "ModelConversionError",
    "convert_field",
    "field_for",
    "fields",
    "register_field_builder",
    "register_field",
]
