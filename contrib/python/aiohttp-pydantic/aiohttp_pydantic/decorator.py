from functools import update_wrapper
from inspect import iscoroutinefunction, signature
from typing import Callable, Union

from aiohttp.typedefs import Handler
from aiohttp.web_response import StreamResponse, json_response
from pydantic import ValidationError

from .injectors import (
    CONTEXT,
    BodyGetter,
    HeadersGetter,
    MatchInfoGetter,
    QueryGetter,
    _parse_func_signature,
)


async def json_response_error(
    exception: ValidationError, context: CONTEXT
) -> StreamResponse:
    errors = exception.errors(include_url=False)
    for error in errors:
        error["in"] = context
        if "ctx" in error and "error" in error["ctx"]:
            error["ctx"]["error"] = str(error["ctx"]["error"])
    return json_response(data=errors, status=400)


class InjectParamsFactory:
    """
    Decorator to unpack the query string, route path, body and http header in
    the parameters of the web handler regarding annotations.

    @inject_params
    async def get(name: str = "") -> r200[list[Friend]]:
        ...

    If you need the request parameter, use:


    @inject_params.and_request
    async def get(request, name: str = "") -> r200[list[Friend]]:
        ...


    You can add a function to customize the error response.

    @inject_params(on_validation_error=format_error).and_request
    async def get(request, name: str = "") -> r200[list[Friend]]:
        ...


    You can decorate method:

    class View:

        @inject_params.in_method
        async def get(self, name: str = "") -> r200[list[Friend]]:
            ...

        @inject_params.in_method(on_validation_error=format_error)
        async def get(self, name: str = "") -> r200[list[Friend]]:
            ...

    """

    def __init__(self, on_validation_error=None):
        self._on_validation_error = on_validation_error

    @property
    def and_request(self):
        return InjectParamsRequestFactory(on_validation_error=self._on_validation_error)

    @property
    def in_method(self):
        return InjectParamsInMtdFactory(on_validation_error=self._on_validation_error)

    def __call__(self, handler=None, /, *, on_validation_error=None):
        if handler is None:
            return InjectParamsFactory(on_validation_error)
        return _inject_params(
            handler,
            decorate_method=False,
            inject_request=False,
            on_validation_error=self._on_validation_error,
        )


class InjectParamsRequestFactory:

    def __init__(self, on_validation_error):
        self._on_validation_error = on_validation_error

    def __call__(self, handler=None, /, *, on_validation_error=None):
        if handler is None:
            return InjectParamsRequestFactory(
                on_validation_error=on_validation_error or self._on_validation_error
            )
        return _inject_params(
            handler,
            decorate_method=False,
            inject_request=True,
            on_validation_error=self._on_validation_error,
        )


class InjectParamsInMtdFactory:
    def __init__(self, on_validation_error):
        self._on_validation_error = on_validation_error

    def __call__(self, handler=None, /, *, on_validation_error=None):
        if handler is None:
            return InjectParamsInMtdFactory(
                on_validation_error or self._on_validation_error
            )
        return _inject_params(
            handler,
            decorate_method=True,
            inject_request=False,
            on_validation_error=self._on_validation_error,
        )


inject_params = InjectParamsFactory()


def _inject_params(
    handler=None,
    /,
    *,
    decorate_method: bool,
    inject_request: bool,
    on_validation_error=None,
) -> Union[
    Callable[
        [
            Callable,
        ],
        Handler,
    ],
    Handler,
]:
    if decorate_method and inject_request:
        raise ValueError("Cannot set decorate_method and inject_request both")

    if decorate_method and on_validation_error:
        raise ValueError("Cannot set decorate_method and on_validation_error both")

    if on_validation_error is None:
        on_validation_error = json_response_error

    def decorator(handler_) -> Handler:
        if decorate_method:
            path_args, body_args, qs_args, header_args, defaults = (
                _parse_func_signature(
                    handler_,
                    ignore_params=("self",),
                )
            )
        elif inject_request:
            _first_param = next(iter(signature(handler_).parameters), "")
            path_args, body_args, qs_args, header_args, defaults = (
                _parse_func_signature(
                    handler_,
                    ignore_params=(_first_param,),
                )
            )
        else:
            path_args, body_args, qs_args, header_args, defaults = (
                _parse_func_signature(handler_)
            )

        def default_value(args: dict) -> dict:
            """
            Returns the default values of args.
            """
            return {name: defaults[name] for name in args if name in defaults}

        injectors = []
        if path_args:
            injectors.append(MatchInfoGetter(path_args, default_value(path_args)))
        if body_args:
            injectors.append(BodyGetter(body_args, default_value(body_args)))
        if qs_args:
            injectors.append(QueryGetter(qs_args, default_value(qs_args)))
        if header_args:
            injectors.append(HeadersGetter(header_args, default_value(header_args)))

        if decorate_method:

            async def wrapped_handler(self):
                args = []
                kwargs = {}
                for injector in injectors:
                    try:
                        if iscoroutinefunction(injector.inject):
                            await injector.inject(self.request, args, kwargs)
                        else:
                            injector.inject(self.request, args, kwargs)
                    except ValidationError as error:
                        return await getattr(
                            self, "on_validation_error", on_validation_error
                        )(error, injector.context)
                return await handler(self, *args, **kwargs)

        elif inject_request:

            async def wrapped_handler(request):
                args = [request]
                kwargs = {}
                for injector in injectors:
                    try:
                        if iscoroutinefunction(injector.inject):
                            await injector.inject(request, args, kwargs)
                        else:
                            injector.inject(request, args, kwargs)
                    except ValidationError as error:
                        return await on_validation_error(error, injector.context)

                return await handler_(*args, **kwargs)

        else:

            async def wrapped_handler(request):
                args = []
                kwargs = {}
                for injector in injectors:
                    try:
                        if iscoroutinefunction(injector.inject):
                            await injector.inject(request, args, kwargs)
                        else:
                            injector.inject(request, args, kwargs)
                    except ValidationError as error:
                        return await on_validation_error(error, injector.context)

                return await handler_(*args, **kwargs)

        update_wrapper(wrapped_handler, handler_)
        wrapped_handler.is_aiohttp_pydantic_handler = True

        return wrapped_handler

    if handler is None:
        return decorator
    return decorator(handler)
