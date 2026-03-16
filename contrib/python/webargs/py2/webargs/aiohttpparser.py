"""aiohttp request argument parsing module.

Example: ::

    import asyncio
    from aiohttp import web

    from webargs import fields
    from webargs.aiohttpparser import use_args


    hello_args = {
        'name': fields.Str(required=True)
    }
    @asyncio.coroutine
    @use_args(hello_args)
    def index(request, args):
        return web.Response(
            body='Hello {}'.format(args['name']).encode('utf-8')
        )

    app = web.Application()
    app.router.add_route('GET', '/', index)
"""
import typing

from aiohttp import web
from aiohttp.web import Request
from aiohttp import web_exceptions
from marshmallow import Schema, ValidationError
from marshmallow.fields import Field

from webargs import core
from webargs.core import json
from webargs.asyncparser import AsyncParser


def is_json_request(req: Request) -> bool:
    content_type = req.content_type
    return core.is_json(content_type)


class HTTPUnprocessableEntity(web.HTTPClientError):
    status_code = 422


# Mapping of status codes to exception classes
# Adapted from werkzeug
exception_map = {422: HTTPUnprocessableEntity}


def _find_exceptions() -> None:
    for name in web_exceptions.__all__:
        obj = getattr(web_exceptions, name)
        try:
            is_http_exception = issubclass(obj, web_exceptions.HTTPException)
        except TypeError:
            is_http_exception = False
        if not is_http_exception or obj.status_code is None:
            continue
        old_obj = exception_map.get(obj.status_code, None)
        if old_obj is not None and issubclass(obj, old_obj):
            continue
        exception_map[obj.status_code] = obj


# Collect all exceptions from aiohttp.web_exceptions
_find_exceptions()
del _find_exceptions


class AIOHTTPParser(AsyncParser):
    """aiohttp request argument parser."""

    __location_map__ = dict(
        match_info="parse_match_info",
        path="parse_match_info",
        **core.Parser.__location_map__
    )

    def parse_querystring(self, req: Request, name: str, field: Field) -> typing.Any:
        """Pull a querystring value from the request."""
        return core.get_value(req.query, name, field)

    async def parse_form(self, req: Request, name: str, field: Field) -> typing.Any:
        """Pull a form value from the request."""
        post_data = self._cache.get("post")
        if post_data is None:
            self._cache["post"] = await req.post()
        return core.get_value(self._cache["post"], name, field)

    async def parse_json(self, req: Request, name: str, field: Field) -> typing.Any:
        """Pull a json value from the request."""
        json_data = self._cache.get("json")
        if json_data is None:
            if not (req.body_exists and is_json_request(req)):
                return core.missing
            try:
                json_data = await req.json(loads=json.loads)
            except json.JSONDecodeError as e:
                if e.doc == "":
                    return core.missing
                else:
                    return self.handle_invalid_json_error(e, req)
            except UnicodeDecodeError as e:
                return self.handle_invalid_json_error(e, req)

            self._cache["json"] = json_data
        return core.get_value(json_data, name, field, allow_many_nested=True)

    def parse_headers(self, req: Request, name: str, field: Field) -> typing.Any:
        """Pull a value from the header data."""
        return core.get_value(req.headers, name, field)

    def parse_cookies(self, req: Request, name: str, field: Field) -> typing.Any:
        """Pull a value from the cookiejar."""
        return core.get_value(req.cookies, name, field)

    def parse_files(self, req: Request, name: str, field: Field) -> None:
        raise NotImplementedError(
            "parse_files is not implemented. You may be able to use parse_form for "
            "parsing upload data."
        )

    def parse_match_info(self, req: Request, name: str, field: Field) -> typing.Any:
        """Pull a value from the request's ``match_info``."""
        return core.get_value(req.match_info, name, field)

    def get_request_from_view_args(
        self, view: typing.Callable, args: typing.Iterable, kwargs: typing.Mapping
    ) -> Request:
        """Get request object from a handler function or method. Used internally by
        ``use_args`` and ``use_kwargs``.
        """
        req = None
        for arg in args:
            if isinstance(arg, web.Request):
                req = arg
                break
            elif isinstance(arg, web.View):
                req = arg.request
                break
        assert isinstance(req, web.Request), "Request argument not found for handler"
        return req

    def handle_error(
        self,
        error: ValidationError,
        req: Request,
        schema: Schema,
        error_status_code: typing.Union[int, None] = None,
        error_headers: typing.Union[typing.Mapping[str, str], None] = None,
    ) -> "typing.NoReturn":
        """Handle ValidationErrors and return a JSON response of error messages
        to the client.
        """
        error_class = exception_map.get(
            error_status_code or self.DEFAULT_VALIDATION_STATUS
        )
        if not error_class:
            raise LookupError("No exception for {0}".format(error_status_code))
        headers = error_headers
        raise error_class(
            body=json.dumps(error.messages).encode("utf-8"),
            headers=headers,
            content_type="application/json",
        )

    def handle_invalid_json_error(
        self,
        error: typing.Union[json.JSONDecodeError, UnicodeDecodeError],
        req: Request,
        *args,
        **kwargs
    ) -> "typing.NoReturn":
        error_class = exception_map[400]
        messages = {"json": ["Invalid JSON body."]}
        raise error_class(
            body=json.dumps(messages).encode("utf-8"), content_type="application/json"
        )


parser = AIOHTTPParser()
use_args = parser.use_args  # type: typing.Callable
use_kwargs = parser.use_kwargs  # type: typing.Callable
