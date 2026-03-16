from functools import wraps

try:
    import coreschema
except ImportError:
    coreschema = None

from .. import openapi
from ..utils import force_real_str
from .base import FilterInspector, NotHandled, PaginatorInspector


def ignore_assert_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AssertionError:
            return NotHandled

    return wrapper


class DrfAPICompatInspector(PaginatorInspector, FilterInspector):
    def param_to_schema(self, param):
        return openapi.Parameter(
            name=param["name"],
            in_=param["in"],
            description=param.get("description"),
            required=param.get("required", False),
            **param["schema"],
        )

    def get_paginator_parameters(self, paginator):
        if hasattr(paginator, "get_schema_operation_parameters"):
            return list(
                map(
                    self.param_to_schema,
                    paginator.get_schema_operation_parameters(self.view),
                )
            )
        return NotHandled

    def get_filter_parameters(self, filter_backend):
        if hasattr(filter_backend, "get_schema_operation_parameters"):
            return list(
                map(
                    self.param_to_schema,
                    filter_backend.get_schema_operation_parameters(self.view),
                )
            )

        if hasattr(filter_backend, "get_filterset_class"):
            return self.get_filter_backend_params(filter_backend)

        return NotHandled

    def get_filter_backend_params(self, filter_backend):
        filterset_class = filter_backend.get_filterset_class(
            self.view,
            self.view.queryset,
        )

        if filterset_class is None:
            return NotHandled

        return [
            openapi.Parameter(
                name=field_name,
                in_=openapi.IN_QUERY,
                required=field.extra.get("required", False),
                type=openapi.TYPE_STRING,
                description=str(field.label) if field.label else "",
            )
            for field_name, field in filterset_class.base_filters.items()
        ]


class CoreAPICompatInspector(PaginatorInspector, FilterInspector):
    """Converts ``coreapi.Field``\\ s to :class:`.openapi.Parameter`\\ s for filters and
    paginators that implement a ``get_schema_fields`` method.
    """

    @ignore_assert_decorator
    def get_paginator_parameters(self, paginator):
        fields = []
        if hasattr(paginator, "get_schema_fields"):
            fields = paginator.get_schema_fields(self.view)

        return [self.coreapi_field_to_parameter(field) for field in fields]

    @ignore_assert_decorator
    def get_filter_parameters(self, filter_backend):
        fields = []
        if hasattr(filter_backend, "get_schema_fields"):
            fields = filter_backend.get_schema_fields(self.view)
        return [self.coreapi_field_to_parameter(field) for field in fields]

    def coreapi_field_to_parameter(self, field):
        """Convert an instance of `coreapi.Field` to a swagger :class:`.Parameter`
        object.

        :param coreapi.Field field:
        :rtype: openapi.Parameter
        """
        location_to_in = {
            "query": openapi.IN_QUERY,
            "path": openapi.IN_PATH,
            "form": openapi.IN_FORM,
            "body": openapi.IN_FORM,
        }
        coreapi_types = {
            coreschema.Integer: openapi.TYPE_INTEGER,
            coreschema.Number: openapi.TYPE_NUMBER,
            coreschema.String: openapi.TYPE_STRING,
            coreschema.Boolean: openapi.TYPE_BOOLEAN,
        }

        coreschema_attrs = ["format", "pattern", "enum", "min_length", "max_length"]
        schema = field.schema
        return openapi.Parameter(
            name=field.name,
            in_=location_to_in[field.location],
            required=field.required,
            description=force_real_str(schema.description) if schema else None,
            type=coreapi_types.get(type(schema), openapi.TYPE_STRING),
            **{attr: getattr(schema, attr, None) for attr in coreschema_attrs},
        )


class DjangoRestResponsePagination(PaginatorInspector):
    """Provides response schema pagination wrapping for django-rest-framework'
    LimitOffsetPagination, PageNumberPagination and CursorPagination
    """

    def fix_paginated_property(self, key: str, value: dict):
        # Need to remove useless params from schema
        value.pop("example", None)
        if "nullable" in value:
            value["x-nullable"] = value.pop("nullable")
        if key in {"next", "previous"} and "format" not in value:
            value["format"] = "uri"
        return openapi.Schema(**value)

    def get_paginated_response(self, paginator, response_schema):
        if hasattr(paginator, "get_paginated_response_schema"):
            paginator_schema = paginator.get_paginated_response_schema(response_schema)
            if paginator_schema["type"] == openapi.TYPE_OBJECT:
                properties = {
                    k: self.fix_paginated_property(k, v)
                    for k, v in paginator_schema.pop("properties").items()
                }
                if "required" not in paginator_schema:
                    paginator_schema.setdefault("required", [])
                    for prop in ("count", "results"):
                        if prop in properties:
                            paginator_schema["required"].append(prop)
                return openapi.Schema(**paginator_schema, properties=properties)
            else:
                return openapi.Schema(**paginator_schema)

        return response_schema
