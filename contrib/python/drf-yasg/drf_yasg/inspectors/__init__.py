from ..app_settings import swagger_settings
from .base import (
    BaseInspector,
    FieldInspector,
    FilterInspector,
    NotHandled,
    PaginatorInspector,
    SerializerInspector,
    ViewInspector,
)
from .field import (
    CamelCaseJSONFilter,
    ChoiceFieldInspector,
    DictFieldInspector,
    FileFieldInspector,
    HiddenFieldInspector,
    InlineSerializerInspector,
    JSONFieldInspector,
    RecursiveFieldInspector,
    ReferencingSerializerInspector,
    RelatedFieldInspector,
    SerializerMethodFieldInspector,
    SimpleFieldInspector,
    StringDefaultFieldInspector,
)
from .query import (
    CoreAPICompatInspector,
    DjangoRestResponsePagination,
    DrfAPICompatInspector,
)
from .view import SwaggerAutoSchema

# these settings must be accessed only after defining/importing all the classes in this
# module to avoid ImportErrors
ViewInspector.field_inspectors = swagger_settings.DEFAULT_FIELD_INSPECTORS
ViewInspector.filter_inspectors = swagger_settings.DEFAULT_FILTER_INSPECTORS
ViewInspector.paginator_inspectors = swagger_settings.DEFAULT_PAGINATOR_INSPECTORS

__all__ = [
    # base inspectors
    "BaseInspector",
    "FilterInspector",
    "PaginatorInspector",
    "FieldInspector",
    "SerializerInspector",
    "ViewInspector",
    # filter and pagination inspectors
    "DrfAPICompatInspector",
    "CoreAPICompatInspector",
    "DjangoRestResponsePagination",
    # field inspectors
    "InlineSerializerInspector",
    "RecursiveFieldInspector",
    "ReferencingSerializerInspector",
    "RelatedFieldInspector",
    "SimpleFieldInspector",
    "FileFieldInspector",
    "ChoiceFieldInspector",
    "DictFieldInspector",
    "JSONFieldInspector",
    "StringDefaultFieldInspector",
    "CamelCaseJSONFilter",
    "HiddenFieldInspector",
    "SerializerMethodFieldInspector",
    # view inspectors
    "SwaggerAutoSchema",
    # module constants
    "NotHandled",
]
