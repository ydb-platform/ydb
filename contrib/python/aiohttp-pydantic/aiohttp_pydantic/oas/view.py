import typing
import warnings
from inspect import getdoc, signature
from itertools import count
from typing import List, Optional, Type, get_type_hints

from aiohttp import hdrs
from aiohttp.web import Response, json_response, View
from aiohttp.web_app import Application
from pydantic import RootModel
from pydantic.fields import FieldInfo

from . import pydantic_schema_to_oas
from ..injectors import _parse_func_signature
from ..uploaded_file import UploadedFile
from ..utils import is_pydantic_base_model, robuste_issubclass
from ..view import PydanticView, is_pydantic_view
from . import docstring_parser
from .definition import (
    AIOHTTP_HAS_APP_KEY,
    key_apps_to_expose,
    key_index_template,
    key_title_spec,
    key_version_spec,
    key_security,
    key_swagger_ui_version,
    key_display_configurations,
)
from .struct import OpenApiSpec3, OperationObject, PathItem
from .typing import is_status_code_type

_APP_KEY_NOT_SET = object()


class _OASResponseBuilder:
    """
    Parse the type annotated as returned by a function and
    generate the OAS operation response.
    """

    def __init__(
        self,
        oas: OpenApiSpec3,
        oas_operation,
        status_code_descriptions,
        pydantic_to_oas,
    ):
        self._oas_operation = oas_operation
        self._oas = oas
        self._status_code_descriptions = status_code_descriptions
        self._to_oas = pydantic_to_oas

    def _handle_pydantic_base_model(self, obj):
        if is_pydantic_base_model(obj):
            return _extract_and_register_pydantic_schema(
                obj=obj,
                oas=self._oas,
                to_oas_func=self._to_oas,
            )
        return {}

    def _handle_list(self, obj):
        if typing.get_origin(obj) is list:
            return {
                "type": "array",
                "items": self._handle_pydantic_base_model(typing.get_args(obj)[0]),
            }
        return self._handle_pydantic_base_model(obj)

    def _handle_status_code_type(self, obj):
        if is_status_code_type(typing.get_origin(obj)):
            status_code = typing.get_origin(obj).__name__
            if status_code != "default":
                status_code = status_code[1:]
            self._oas_operation.responses[status_code].content = {
                "application/json": {
                    "schema": self._handle_list(typing.get_args(obj)[0])
                }
            }
            desc = self._status_code_descriptions.get(status_code)
            if desc:
                self._oas_operation.responses[status_code].description = desc

        elif is_status_code_type(obj):
            status_code = obj.__name__
            if status_code != "default":
                status_code = status_code[1:]
            self._oas_operation.responses[status_code].content = {}
            desc = self._status_code_descriptions.get(status_code)
            if desc:
                self._oas_operation.responses[status_code].description = desc

    def _handle_union(self, obj):
        if typing.get_origin(obj) is typing.Union:
            for arg in typing.get_args(obj):
                self._handle_status_code_type(arg)
        self._handle_status_code_type(obj)

    def build(self, obj):
        self._handle_union(obj)


def _extract_and_register_pydantic_schema(obj, oas: OpenApiSpec3, to_oas_func):
    """
    Extracts a Pydantic model schema, converts it to OAS, registers all sub-schemas,
    and returns a {"$ref": "..."} pointing to the main model schema.
    """
    schema = obj.model_json_schema(ref_template="#/components/schemas/{model}").copy()
    to_oas_func(schema)
    if def_sub_schemas := schema.pop("$defs", None):
        for sub_schema in def_sub_schemas.values():
            to_oas_func(sub_schema)
        oas.components.schemas.update(def_sub_schemas)

    model_name = schema.get("title", obj.__name__)
    oas.components.schemas[model_name] = schema

    return {"$ref": f"#/components/schemas/{model_name}"}


def _add_http_method_to_oas(
    oas: OpenApiSpec3, oas_path: PathItem, http_method: str, handler
):
    to_oas = pydantic_schema_to_oas.translater(oas.spec.get("version", "3.0.0"))
    http_method = http_method.lower()
    oas_operation: OperationObject = getattr(oas_path, http_method)
    first_param = next(iter(signature(handler).parameters), "")
    if first_param in ("self", "request"):
        ignore_params = (first_param,)
    else:
        ignore_params = ()
    path_args, body_args, qs_args, header_args, defaults = _parse_func_signature(
        handler, unpack_group=True, ignore_params=ignore_params
    )
    description = getdoc(handler)
    if description:
        oas_operation.description = docstring_parser.operation(description)
        oas_operation.tags = docstring_parser.tags(description)
        oas_operation.security = docstring_parser.security(description)
        oas_operation.deprecated = docstring_parser.deprecated(description)
        status_code_descriptions = docstring_parser.status_code(description)
    else:
        status_code_descriptions = {}

    if body_args:
        multipart = any(
            robuste_issubclass(type_, UploadedFile) for type_ in body_args.values()
        )
        if multipart:
            # requestBody:
            #   content:
            #     multipart/form-data:
            #       schema:
            #         type: object
            #         properties:
            #           orderId:
            #             type: integer
            #           userId:
            #             type: integer
            #           fileName:
            #             type: string
            #             format: binary

            properties = {}
            for name, type_ in body_args.items():
                if robuste_issubclass(type_, UploadedFile):
                    properties[name] = {"type": "string", "format": "binary"}
                else:
                    body_schema = type_.model_json_schema(
                        ref_template="#/components/schemas/{model}"
                    ).copy()
                    to_oas(body_schema)
                    properties[name] = body_schema
                    if def_sub_schemas := body_schema.pop("$defs", None):
                        for sub_schema in def_sub_schemas.values():
                            to_oas(sub_schema)
                        oas.components.schemas.update(def_sub_schemas)

            oas_operation.request_body.content = {
                "multipart/form-data": {
                    "schema": {"type": "object", "properties": properties}
                }
            }

        else:
            obj = next(iter(body_args.values()))
            body_ref = _extract_and_register_pydantic_schema(
                obj=obj,
                oas=oas,
                to_oas_func=to_oas,
            )
            oas_operation.request_body.content = {
                "application/json": {"schema": body_ref}
            }

    indexes = count()
    for args_location, args in (
        ("path", path_args.items()),
        ("query", qs_args.items()),
        ("header", header_args.items()),
    ):

        for name, type_ in args:
            i = next(indexes)
            oas_operation.parameters[i].in_ = args_location
            oas_operation.parameters[i].name = name

            attrs = {"__annotations__": {"root": type_}}
            oas_operation.parameters[i].required = True
            if name in defaults:
                if defaults[name] is not None:
                    attrs["root"] = defaults[name]

                if not (
                    isinstance(defaults[name], FieldInfo)
                    and defaults[name].is_required()
                ):
                    oas_operation.parameters[i].required = False

            attr_schema = type(name, (RootModel,), attrs).model_json_schema(
                ref_template="#/components/schemas/{model}"
            )
            to_oas(attr_schema)
            if def_sub_schemas := attr_schema.pop("$defs", None):
                for sub_schema in def_sub_schemas.values():
                    to_oas(sub_schema)
                oas.components.schemas.update(def_sub_schemas)
            # update description
            if "description" in attr_schema:
                oas_operation.parameters[i].description = attr_schema.pop("description")
            oas_operation.parameters[i].schema = attr_schema

    return_type = get_type_hints(handler).get("return")
    if return_type is not None:
        _OASResponseBuilder(oas, oas_operation, status_code_descriptions, to_oas).build(
            return_type
        )
    else:
        oas_operation.responses["200"].description = ""


def _is_aiohttp_view(obj):
    """
    Return True if obj is a aiohttp View subclass else False.
    """
    try:
        return issubclass(obj, View)
    except TypeError:
        return False


def generate_oas(
    apps: List[Application],
    version_spec: Optional[str] = None,
    title_spec: Optional[str] = None,
    security: Optional[dict] = None,
) -> dict:
    """
    Generate and return Open Api Specification from PydanticView in application.
    """
    oas = OpenApiSpec3()

    if version_spec is not None:
        oas.info.version = version_spec

    if title_spec is not None:
        oas.info.title = title_spec

    for app in apps:
        for resources in app.router.resources():
            for resource_route in resources:
                # FUTURE: remove is_pydantic_view test when PydanticView use
                # decorator.inject_param.in_method under the hood
                if is_pydantic_view(resource_route.handler):
                    view: Type[PydanticView] = resource_route.handler
                    info = resource_route.get_info()
                    path = oas.paths[info.get("path", info.get("formatter"))]
                    if resource_route.method == "*":
                        for method_name in view.allowed_methods:
                            handler = getattr(view, method_name.lower())
                            _add_http_method_to_oas(oas, path, method_name, handler)
                    else:
                        handler = getattr(view, resource_route.method.lower())
                        _add_http_method_to_oas(
                            oas, path, resource_route.method, handler
                        )
                elif _is_aiohttp_view(resource_route.handler):
                    view: View = resource_route.handler
                    info = resource_route.get_info()
                    path = oas.paths[info.get("path", info.get("formatter"))]
                    if resource_route.method == "*":
                        for method_name in hdrs.METH_ALL:
                            handler = getattr(view, method_name.lower(), None)
                            if handler is not None and getattr(
                                handler, "is_aiohttp_pydantic_handler", False
                            ):
                                _add_http_method_to_oas(oas, path, method_name, handler)

                elif getattr(
                    resource_route.handler, "is_aiohttp_pydantic_handler", False
                ):
                    info = resource_route.get_info()
                    path = oas.paths[info.get("path", info.get("formatter"))]
                    _add_http_method_to_oas(
                        oas, path, resource_route.method, resource_route.handler
                    )

    if security:
        oas.components.security_schemes.update(security)

    return oas.spec


def _app_key_or_string(app, app_key, str_key):
    if app_key in app:
        return app[app_key]
    if AIOHTTP_HAS_APP_KEY:
        key_name = "key_" + str_key.replace(" ", "_")
        warnings.warn(
            f"Use from aiohttp_pydantic.oas.definition import {key_name}; app[{key_name}] = ... "
            f"instead of app[{str_key}] = ...",
            DeprecationWarning,
            stacklevel=4,
        )
    return app[str_key]


async def get_oas(request):
    """
    View to generate the Open Api Specification from PydanticView in application.
    """
    apps = _app_key_or_string(request.app, key_apps_to_expose, "apps to expose")
    version_spec = _app_key_or_string(request.app, key_version_spec, "version spec")
    title_spec = _app_key_or_string(request.app, key_title_spec, "title spec")
    security = _app_key_or_string(request.app, key_security, "security")
    return json_response(generate_oas(apps, version_spec, title_spec, security))


async def oas_ui(request):
    """
    View to serve the swagger-ui to read open api specification of application.
    """
    template = _app_key_or_string(request.app, key_index_template, "index template")
    swagger_ui_version = request.app.get(key_swagger_ui_version, "5")
    return Response(
        text=template.render(
            {
                "openapi_spec_url": request.app.router["spec"].canonical,
                "display_configurations": request.app[key_display_configurations],
                "swagger_ui_version": swagger_ui_version,
            }
        ),
        content_type="text/html",
        charset="utf-8",
    )
