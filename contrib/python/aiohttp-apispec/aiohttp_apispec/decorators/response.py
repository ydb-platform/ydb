def response_schema(schema, code=200, required=False, description=None):
    """
    Add response info into the swagger spec

    Usage:

    .. code-block:: python

        from aiohttp import web
        from marshmallow import Schema, fields


        class ResponseSchema(Schema):
            msg = fields.Str()
            data = fields.Dict()

        @response_schema(ResponseSchema(), 200)
        async def index(request):
            return web.json_response({'msg': 'done', 'data': {}})

    :param str description: response description
    :param bool required:
    :param schema: :class:`Schema <marshmallow.Schema>` class or instance
    :param int code: HTTP response code
    """
    if callable(schema):
        schema = schema()

    def wrapper(func):
        if not hasattr(func, "__apispec__"):
            func.__apispec__ = {"schemas": [], "responses": {}, "parameters": []}
            func.__schemas__ = []
        func.__apispec__["responses"]["%s" % code] = {
            "schema": schema,
            "required": required,
            "description": description or "",
        }
        return func

    return wrapper


# For backward compatibility
marshal_with = response_schema
