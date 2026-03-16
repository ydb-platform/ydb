import logging

from rest_framework.request import is_form_media_type
from rest_framework.schemas import AutoSchema
from rest_framework.status import is_success

from .. import openapi
from ..errors import SwaggerGenerationError
from ..utils import (
    filter_none,
    force_real_str,
    force_serializer_instance,
    get_consumes,
    get_produces,
    guess_response_status,
    merge_params,
    no_body,
    param_list_to_dict,
)
from .base import ViewInspector, call_view_method

logger = logging.getLogger(__name__)


class SwaggerAutoSchema(ViewInspector):
    def __init__(
        self, view, path, method, components, request, overrides, operation_keys=None
    ):
        super(SwaggerAutoSchema, self).__init__(
            view, path, method, components, request, overrides
        )
        self._sch = AutoSchema()
        self._sch.view = view
        self.operation_keys = operation_keys

    def get_operation(self, operation_keys=None):
        operation_keys = operation_keys or self.operation_keys

        consumes = self.get_consumes()
        produces = self.get_produces()

        body = self.get_request_body_parameters(consumes)
        query = self.get_query_parameters()
        parameters = body + query
        parameters = filter_none(parameters)
        parameters = self.add_manual_parameters(parameters)

        operation_id = self.get_operation_id(operation_keys)
        summary, description = self.get_summary_and_description()
        security = self.get_security()
        assert security is None or isinstance(security, list), (
            "security must be a list of security requirement objects"
        )
        deprecated = self.is_deprecated()
        tags = self.get_tags(operation_keys)

        responses = self.get_responses()

        return openapi.Operation(
            operation_id=operation_id,
            description=force_real_str(description),
            summary=force_real_str(summary),
            responses=responses,
            parameters=parameters,
            consumes=consumes,
            produces=produces,
            tags=tags,
            security=security,
            deprecated=deprecated,
        )

    def get_request_body_parameters(self, consumes):
        """Return the request body parameters for this view. |br|
        This is either:

        -  a list with a single object Parameter with a :class:`.Schema` derived from
            the request serializer
        -  a list of primitive Parameters parsed as form data

        :param list[str] consumes: a list of accepted MIME types as returned by
            :meth:`.get_consumes`
        :return: a (potentially empty) list of :class:`.Parameter`\\ s either
            ``in: body`` or ``in: formData``
        :rtype: list[openapi.Parameter]
        """
        serializer = self.get_request_serializer()
        schema = None
        if serializer is None:
            return []

        if isinstance(serializer, openapi.Schema.OR_REF):
            schema = serializer

        if any(is_form_media_type(encoding) for encoding in consumes):
            if schema is not None:
                raise SwaggerGenerationError("form request body cannot be a Schema")
            return self.get_request_form_parameters(serializer)
        else:
            if schema is None:
                schema = self.get_request_body_schema(serializer)
            return [self.make_body_parameter(schema)] if schema is not None else []

    def get_view_serializer(self):
        """Return the serializer as defined by the view's ``get_serializer()`` method.

        :return: the view's ``Serializer``
        :rtype: rest_framework.serializers.Serializer
        """
        return call_view_method(self.view, "get_serializer")

    def _get_request_body_override(self):
        """Parse the request_body key in the override dict. This method is not public
        API."""
        body_override = self.overrides.get("request_body", None)

        if body_override is not None:
            if body_override is no_body:
                return no_body
            if self.method not in self.body_methods:
                raise SwaggerGenerationError(
                    "request_body can only be applied to ("
                    + ",".join(self.body_methods)
                    + "); are you looking for query_serializer or manual_parameters?"
                )
            if isinstance(body_override, openapi.Schema.OR_REF):
                return body_override
            return force_serializer_instance(body_override)

        return body_override

    def get_request_serializer(self):
        """Return the request serializer (used for parsing the request payload) for this
        endpoint.

        :return: the request serializer, or one of :class:`.Schema`,
            :class:`.SchemaRef`, ``None``
        :rtype: rest_framework.serializers.Serializer
        """
        body_override = self._get_request_body_override()

        if body_override is None and self.method in self.implicit_body_methods:
            return self.get_view_serializer()

        if body_override is no_body:
            return None

        return body_override

    def get_request_form_parameters(self, serializer):
        """Given a Serializer, return a list of ``in: formData``
        :class:`.Parameter`\\ s.

        :param serializer: the view's request serializer as returned by
            :meth:`.get_request_serializer`
        :rtype: list[openapi.Parameter]
        """
        return self.serializer_to_parameters(serializer, in_=openapi.IN_FORM)

    def get_request_body_schema(self, serializer):
        """Return the :class:`.Schema` for a given request's body data. Only applies to
        PUT, PATCH and POST requests.

        :param serializer: the view's request serializer as returned by
            :meth:`.get_request_serializer`
        :rtype: openapi.Schema
        """
        return self.serializer_to_schema(serializer)

    def make_body_parameter(self, schema):
        """Given a :class:`.Schema` object, create an ``in: body`` :class:`.Parameter`.

        :param openapi.Schema schema: the request body schema
        :rtype: openapi.Parameter
        """
        return openapi.Parameter(
            name="data", in_=openapi.IN_BODY, required=True, schema=schema
        )

    def add_manual_parameters(self, parameters):
        """Add/replace parameters from the given list of automatically generated request
        parameters.

        :param list[openapi.Parameter] parameters: generated parameters
        :return: modified parameters
        :rtype: list[openapi.Parameter]
        """
        manual_parameters = self.overrides.get("manual_parameters", None) or []

        if any(
            param.in_ == openapi.IN_BODY for param in manual_parameters
        ):  # pragma: no cover
            raise SwaggerGenerationError(
                "specify the body parameter as a Schema or Serializer in request_body"
            )
        if any(
            param.in_ == openapi.IN_FORM for param in manual_parameters
        ):  # pragma: no cover
            has_body_parameter = any(
                param.in_ == openapi.IN_BODY for param in parameters
            )
            if has_body_parameter or not any(
                is_form_media_type(encoding) for encoding in self.get_consumes()
            ):
                raise SwaggerGenerationError(
                    "cannot add form parameters when the request has a request body; "
                    "did you forget to set an appropriate parser class on the view?"
                )
            if self.method not in self.body_methods:
                raise SwaggerGenerationError(
                    "form parameters can only be applied to "
                    "(" + ",".join(self.body_methods) + ") HTTP methods"
                )

        return merge_params(parameters, manual_parameters)

    def get_responses(self):
        """Get the possible responses for this view as a swagger :class:`.Responses`
        object.

        :return: the documented responses
        :rtype: openapi.Responses
        """
        response_serializers = self.get_response_serializers()
        return openapi.Responses(
            responses=self.get_response_schemas(response_serializers)
        )

    def get_default_response_serializer(self):
        """Return the default response serializer for this endpoint. This is derived
        from either the ``request_body`` override or the request serializer
        (:meth:`.get_view_serializer`).

        :return: response serializer, :class:`.Schema`, :class:`.SchemaRef`, ``None``
        """
        body_override = self._get_request_body_override()
        if body_override and body_override is not no_body:
            return body_override

        return self.get_view_serializer()

    def get_default_responses(self):
        """Get the default responses determined for this view from the request
        serializer and request method.

        :type: dict[str, openapi.Schema]
        """
        method = self.method.lower()

        default_status = guess_response_status(method)
        default_schema = ""
        if method in ("get", "post", "put", "patch"):
            default_schema = self.get_default_response_serializer()

        default_schema = default_schema or ""
        if default_schema and not isinstance(default_schema, openapi.Schema):
            default_schema = self.serializer_to_schema(default_schema) or ""

        if default_schema:
            if self.has_list_response():
                default_schema = openapi.Schema(
                    type=openapi.TYPE_ARRAY, items=default_schema
                )
            if self.should_page():
                default_schema = (
                    self.get_paginated_response(default_schema) or default_schema
                )

        return {str(default_status): default_schema}

    def get_response_serializers(self):
        """Return the response codes that this view is expected to return, and the
        serializer for each response body. The return value should be a dict where the
        keys are possible status codes, and values are either strings,
        ``Serializer``\\ s, :class:`.Schema`, :class:`.SchemaRef` or :class:`.Response`
        objects. See
        :func:`@swagger_auto_schema <.swagger_auto_schema>` for more details.

        :return: the response serializers
        :rtype: dict
        """
        manual_responses = self.overrides.get("responses", None) or {}
        manual_responses = {str(sc): resp for sc, resp in manual_responses.items()}

        responses = {}
        if not any(is_success(int(sc)) for sc in manual_responses if sc != "default"):
            responses = self.get_default_responses()

        responses.update((str(sc), resp) for sc, resp in manual_responses.items())
        return responses

    def get_response_schemas(self, response_serializers):
        """Return the :class:`.openapi.Response` objects calculated for this view.

        :param dict response_serializers: response serializers as returned by
            :meth:`.get_response_serializers`
        :return: a dictionary of status code to :class:`.Response` object
        :rtype: dict[str, openapi.Response]
        """
        responses = {}
        for sc, serializer in response_serializers.items():
            if isinstance(serializer, str):
                response = openapi.Response(description=force_real_str(serializer))
            elif not serializer:
                continue
            elif isinstance(serializer, openapi.Response):
                response = serializer
                if hasattr(response, "schema") and not isinstance(
                    response.schema, openapi.Schema.OR_REF
                ):
                    serializer = force_serializer_instance(response.schema)
                    response.schema = self.serializer_to_schema(serializer)
            elif isinstance(serializer, openapi.Schema.OR_REF):
                response = openapi.Response(
                    description="",
                    schema=serializer,
                )
            elif isinstance(serializer, openapi._Ref):
                response = serializer
            else:
                serializer = force_serializer_instance(serializer)
                response = openapi.Response(
                    description="",
                    schema=self.serializer_to_schema(serializer),
                )

            responses[str(sc)] = response

        return responses

    def get_query_serializer(self):
        """Return the query serializer (used for parsing query parameters) for this
        endpoint.

        :return: the query serializer, or ``None``
        """
        query_serializer = self.overrides.get("query_serializer", None)
        if query_serializer is not None:
            query_serializer = force_serializer_instance(query_serializer)
        return query_serializer

    def get_query_parameters(self):
        """Return the query parameters accepted by this view.

        :rtype: list[openapi.Parameter]
        """
        natural_parameters = (
            self.get_filter_parameters() + self.get_pagination_parameters()
        )

        query_serializer = self.get_query_serializer()
        serializer_parameters = []
        if query_serializer is not None:
            serializer_parameters = self.serializer_to_parameters(
                query_serializer, in_=openapi.IN_QUERY
            )

            if (
                len(
                    set(param_list_to_dict(natural_parameters))
                    & set(param_list_to_dict(serializer_parameters))
                )
                != 0
            ):
                raise SwaggerGenerationError(
                    "your query_serializer contains fields that conflict with the "
                    "filter_backend or paginator_class on the view - %s %s"
                    % (self.method, self.path)
                )

        return natural_parameters + serializer_parameters

    def get_operation_id(self, operation_keys=None):
        """Return an unique ID for this operation. The ID must be unique across
        all :class:`.Operation` objects in the API.

        :param tuple[str] operation_keys: an array of keys derived from the path
            describing the hierarchical layout of this view in the API; e.g.
            ``('snippets', 'list')``, ``('snippets', 'retrieve')``, etc.
        :rtype: str
        """
        operation_keys = operation_keys or self.operation_keys

        operation_id = self.overrides.get("operation_id", "")
        if not operation_id:
            operation_id = "_".join(operation_keys)
        return operation_id

    def split_summary_from_description(self, description):
        """Decide if and how to split a summary out of the given description. The
        default implementation uses the first paragraph of the description as a summary
        if it is less than 120 characters long.

        :param description: the full description to be analyzed
        :return: summary and description
        :rtype: (str,str)
        """
        # https://www.python.org/dev/peps/pep-0257/#multi-line-docstrings
        summary = None
        summary_max_len = (
            120  # OpenAPI 2.0 spec says summary should be under 120 characters
        )
        sections = description.split("\n\n", 1)
        if len(sections) == 2:
            sections[0] = sections[0].strip()
            if len(sections[0]) < summary_max_len:
                summary, description = sections
                description = description.strip()

        return summary, description

    def get_summary_and_description(self):
        """Return an operation summary and description determined from the view's
        docstring.

        :return: summary and description
        :rtype: (str,str)
        """
        description = self.overrides.get("operation_description", None)
        summary = self.overrides.get("operation_summary", None)
        if description is None:
            description = self._sch.get_description(self.path, self.method) or ""
            description = description.strip().replace("\r", "")

            if description and (summary is None):
                # description from docstring... do summary magic
                summary, description = self.split_summary_from_description(description)

        return summary, description

    def get_security(self):
        """Return a list of security requirements for this operation.

        Returning an empty list marks the endpoint as unauthenticated (i.e. removes all
        accepted authentication schemes). Returning ``None`` will inherit the top-level
        security requirements.

        :return: security requirements
        :rtype: list[dict[str,list[str]]]"""
        return self.overrides.get("security", None)

    def is_deprecated(self):
        """Return ``True`` if this operation is to be marked as deprecated.

        :return: deprecation status
        :rtype: bool
        """
        return self.overrides.get("deprecated", None)

    def get_tags(self, operation_keys=None):
        """Get a list of tags for this operation. Tags determine how operations relate
        with each other, and in the UI each tag will show as a group containing the
        operations that use it. If not provided in overrides, tags will be inferred
        from the operation url.

        :param tuple[str] operation_keys: an array of keys derived from the path
            describing the hierarchical layout of this view in the API; e.g.
            ``('snippets', 'list')``, ``('snippets', 'retrieve')``, etc.
        :rtype: list[str]
        """
        operation_keys = operation_keys or self.operation_keys

        tags = self.overrides.get("tags")
        if not tags:
            tags = [operation_keys[0]]

        return tags

    def get_consumes(self):
        """Return the MIME types this endpoint can consume.

        :rtype: list[str]
        """
        return self.overrides.get("consumes") or get_consumes(self.get_parser_classes())

    def get_produces(self):
        """Return the MIME types this endpoint can produce.

        :rtype: list[str]
        """
        return self.overrides.get("produces") or get_produces(
            self.get_renderer_classes()
        )
