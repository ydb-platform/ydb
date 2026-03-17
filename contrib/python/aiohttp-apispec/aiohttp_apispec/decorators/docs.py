def docs(**kwargs):
    """
    Annotate the decorated view function with the specified Swagger
    attributes.

    Usage:

    .. code-block:: python

        from aiohttp import web

        @docs(tags=['my_tag'],
              summary='Test method summary',
              description='Test method description',
              parameters=[{
                      'in': 'header',
                      'name': 'X-Request-ID',
                      'schema': {'type': 'string', 'format': 'uuid'},
                      'required': 'true'
                  }]
              )
        async def index(request):
            return web.json_response({'msg': 'done', 'data': {}})

    """

    def wrapper(func):
        if not kwargs.get("produces"):
            kwargs["produces"] = ["application/json"]
        if not hasattr(func, "__apispec__"):
            func.__apispec__ = {"schemas": [], "responses": {}, "parameters": []}
            func.__schemas__ = []
        extra_parameters = kwargs.pop("parameters", [])
        extra_responses = kwargs.pop("responses", {})
        func.__apispec__["parameters"].extend(extra_parameters)
        func.__apispec__["responses"].update(extra_responses)
        func.__apispec__.update(kwargs)
        return func

    return wrapper
