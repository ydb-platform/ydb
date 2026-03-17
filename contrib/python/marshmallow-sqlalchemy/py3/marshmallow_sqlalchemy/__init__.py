from .schema import (
    TableSchema,
    TableSchemaOpts,
    ModelSchema,
    ModelSchemaOpts,
    SQLAlchemySchema,
    SQLAlchemyAutoSchema,
    SQLAlchemySchemaOpts,
    SQLAlchemyAutoSchemaOpts,
    auto_field,
)

from .convert import (
    ModelConverter,
    fields_for_model,
    property2field,
    column2field,
    field_for,
)
from .exceptions import ModelConversionError

__version__ = "0.25.0"
__all__ = [
    "SQLAlchemySchema",
    "SQLAlchemyAutoSchema",
    "SQLAlchemySchemaOpts",
    "SQLAlchemyAutoSchemaOpts",
    "auto_field",
    "ModelConverter",
    "fields_for_model",
    "property2field",
    "column2field",
    "ModelConversionError",
    "field_for",
    # Deprecated API
    "TableSchema",
    "TableSchemaOpts",
    "ModelSchema",
    "ModelSchemaOpts",
]
