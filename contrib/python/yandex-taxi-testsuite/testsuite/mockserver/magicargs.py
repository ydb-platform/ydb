import typing

import aiohttp.web

from testsuite.utils import callinfo, http


def magic_arg(func):
    func.need_wrapped_request = False
    return func


def magic_arg_wrapped(func):
    func.need_wrapped_request = True
    return func


@magic_arg_wrapped
def arg_body_json(request: http.Request):
    return request.json


@magic_arg_wrapped
def arg_body_binary(request: http.Request):
    return request.get_data()


@magic_arg_wrapped
def arg_form(request: http.Request):
    return request.form


@magic_arg
def arg_cookies(request: aiohttp.web.BaseRequest):
    return request.cookies


@magic_arg
def arg_method(request: aiohttp.web.BaseRequest):
    return request.method


@magic_arg
def arg_path(request: aiohttp.web.BaseRequest):
    return request.path


@magic_arg
def arg_headers(request: aiohttp.web.BaseRequest):
    return request.headers


@magic_arg
def arg_query(request: aiohttp.web.BaseRequest):
    return request.query


@magic_arg
def arg_content_type(request: aiohttp.web.BaseRequest):
    return request.content_type


class MagicArgsHandler:
    magic_args_handlers = {
        'body_binary': arg_body_binary,
        'body_json': arg_body_json,
        'content_type': arg_content_type,
        'cookies': arg_cookies,
        'form': arg_form,
        'headers': arg_headers,
        'method': arg_method,
        'path': arg_path,
        'query': arg_query,
    }
    has_request = False

    def __init__(self, func: typing.Callable, *, raw_request: bool) -> None:
        signature = callinfo.getfullargspec(func)
        self.magic_args: list = []
        self.raw_request = raw_request
        if signature.args:
            self.has_request = True
            request_arg = signature.args[0]
            if request_arg in signature.annotations:
                self._infer_request_type(signature.annotations[request_arg])
            for arg in signature.args[1:]:
                self._handle_arg(arg)
        elif signature.varargs:
            self.has_request = True
        if signature.kwonlyargs:
            for arg in signature.kwonlyargs:
                self._handle_arg(arg)

    def _infer_request_type(self, request_type: type) -> None:
        if request_type is aiohttp.web.BaseRequest:
            self.raw_request = True
        elif request_type is http.Request:
            self.raw_request = False

    def _handle_arg(self, arg: str) -> None:
        if arg in self.magic_args_handlers:
            self.magic_args.append((arg, self.magic_args_handlers[arg]))

    async def build_args(
        self,
        request: aiohttp.web.BaseRequest,
        orig_kwargs: dict[str, object],
    ) -> tuple:
        wrapped_request: http.Request | None
        if self.has_request and not self.raw_request:
            wrapped_request = await http.wrap_request(request)
        else:
            wrapped_request = None

        kwargs = orig_kwargs.copy()
        for arg, handler in self.magic_args:
            if arg in kwargs:
                continue
            if handler.need_wrapped_request:
                if wrapped_request is None:
                    wrapped_request = await http.wrap_request(request)
                kwargs[arg] = handler(wrapped_request)
            else:
                kwargs[arg] = handler(request)

        args: tuple
        if self.has_request:
            if self.raw_request:
                args = (request,)
            else:
                args = (wrapped_request,)
        else:
            args = ()
        return args, kwargs
