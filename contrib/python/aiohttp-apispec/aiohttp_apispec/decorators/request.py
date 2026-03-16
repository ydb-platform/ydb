from functools import partial
import copy


def request_schema(schema, locations=None, put_into=None, example=None, add_to_refs=False, **kwargs):
    """
    Add request info into the swagger spec and
    prepare injection keyword arguments from the specified
    webargs arguments into the decorated view function in
    request['data'] for validation_middleware validation middleware.

    Usage:

    .. code-block:: python

        from aiohttp import web
        from marshmallow import Schema, fields


        class RequestSchema(Schema):
            id = fields.Int()
            name = fields.Str(description='name')

        @request_schema(RequestSchema(strict=True))
        async def index(request):
            # aiohttp_apispec_middleware should be used for it
            data = request['data']
            return web.json_response({'name': data['name'],
                                      'id': data['id']})

    :param schema: :class:`Schema <marshmallow.Schema>` class or instance
    :param locations: Default request locations to parse
    :param put_into: name of the key in Request object
                     where validated data will be placed.
                     If None (by default) default key will be used
    :param dict example: Adding example for current schema
    :param bool add_to_refs: Working only if example not None,
                             if True, add example for ref schema.
                             Otherwise add example to endpoint.
                             Default False
    """
    if callable(schema):
        schema = schema()
    # location kwarg added for compatibility with old versions
    locations = locations or []
    if not locations:
        locations = kwargs.pop("location", None)
        if locations:
            locations = [locations]
        else:
            locations = None

    options = {"required": kwargs.pop("required", False)}
    if locations:
        options["default_in"] = locations[0]

    def wrapper(func):
        if not hasattr(func, "__apispec__"):
            func.__apispec__ = {"schemas": [], "responses": {}, "parameters": []}
            func.__schemas__ = []

        _example = copy.copy(example) or {}
        if _example:
            _example['add_to_refs'] = add_to_refs
        func.__apispec__["schemas"].append({"schema": schema, "options": options, "example": _example})
        # TODO: Remove this block?
        if locations and "body" in locations:
            body_schema_exists = (
                "body" in func_schema["locations"] for func_schema in func.__schemas__
            )
            if any(body_schema_exists):
                raise RuntimeError("Multiple body parameters are not allowed")

        func.__schemas__.append({"schema": schema, "locations": locations, "put_into": put_into})

        return func

    return wrapper


# For backward compatibility
use_kwargs = request_schema

# Decorators for specific request data validations (shortenings)
match_info_schema = partial(
    request_schema,
    locations=["match_info"],
    put_into="match_info"
)
querystring_schema = partial(
    request_schema,
    locations=["querystring"],
    put_into="querystring"
)
form_schema = partial(request_schema, locations=["form"], put_into="form")
json_schema = partial(request_schema, locations=["json"], put_into="json")
headers_schema = partial(request_schema, locations=["headers"], put_into="headers")
cookies_schema = partial(request_schema, locations=["cookies"], put_into="cookies")
