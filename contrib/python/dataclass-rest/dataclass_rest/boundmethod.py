from abc import ABC, abstractmethod
from inspect import getcallargs
from logging import getLogger
from typing import Any, Callable, Dict, NoReturn, Optional, Type

from .client_protocol import ClientMethodProtocol, ClientProtocol
from .exceptions import ClientLibraryError, MalformedResponse
from .http_request import File, HttpRequest
from .methodspec import MethodSpec

logger = getLogger(__name__)


class BoundMethod(ClientMethodProtocol, ABC):
    def __init__(
        self,
        name: str,
        method_spec: MethodSpec,
        client: ClientProtocol,
        on_error: Optional[Callable[[Any], Any]],
    ):
        self.name = name
        self.method_spec = method_spec
        self.client = client
        self.on_error = on_error or self._on_error_default

    def _apply_args(self, *args, **kwargs) -> Dict:
        return getcallargs(
            self.method_spec.func,
            self.client,
            *args,
            **kwargs,
        )

    def _get_url(self, args) -> str:
        args = {
            arg: value
            for arg, value in args.items()
            if arg in self.method_spec.url_params
        }
        return self.method_spec.url_template(**args)

    def _get_body(self, args) -> Any:
        python_body = args.get(self.method_spec.body_param_name)
        return self.client.request_body_factory.dump(
            python_body,
            self.method_spec.body_type,
        )

    def _get_query_params(self, args) -> Any:
        return self.client.request_args_factory.dump(
            args,
            self.method_spec.query_params_type,
        )

    def _get_files(self, args) -> Dict[str, File]:
        return {
            field: args[field]
            for field in self.method_spec.file_param_names
            if field in args
        }

    def _create_request(
        self,
        url: str,
        query_params: Any,
        files: Dict[str, File],
        data: Any,
    ) -> HttpRequest:
        return HttpRequest(
            method=self.method_spec.http_method,
            query_params=query_params,
            is_json_request=self.method_spec.is_json_request,
            data=data,
            files=files,
            url=url,
            headers={},
        )

    def get_query_params_type(self) -> Type:
        return self.method_spec.query_params_type

    @abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def _on_error_default(self, response: Any) -> Any:
        raise ClientLibraryError


class SyncMethod(BoundMethod):
    def __call__(self, *args, **kwargs):
        logger.debug("Calling sync method %s", self.name)
        func_args = self._apply_args(*args, **kwargs)
        request = self._create_request(
            url=self._get_url(func_args),
            query_params=self._get_query_params(func_args),
            data=self._get_body(func_args),
            files=self._get_files(func_args),
        )
        request = self._pre_process_request(request)
        raw_response = self.client.do_request(request)
        response = self._pre_process_response(raw_response)
        return self._post_process_response(response)

    def _pre_process_request(self, request: HttpRequest) -> HttpRequest:
        return request

    def _post_process_response(self, response: Any) -> Any:
        return response

    def _pre_process_response(self, response: Any) -> Any:
        if not self._response_ok(response):
            return self.on_error(response)

        body = self._response_body(response)
        try:
            return self.client.response_body_factory.load(
                body,
                self.method_spec.response_type,
            )
        except (ValueError, TypeError, AttributeError) as e:
            raise MalformedResponse from e

    @abstractmethod
    def _response_ok(self, response: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _response_body(self, response: Any) -> Any:
        raise NotImplementedError


class AsyncMethod(BoundMethod):
    async def __call__(self, *args, **kwargs):
        logger.debug("Calling async method %s", self.name)
        func_args = self._apply_args(*args, **kwargs)
        request = self._create_request(
            url=self._get_url(func_args),
            query_params=self._get_query_params(func_args),
            data=self._get_body(func_args),
            files=self._get_files(func_args),
        )
        request = await self._pre_process_request(request)
        raw_response = await self.client.do_request(request)
        response = await self._pre_process_response(raw_response)
        await self._release_raw_response(raw_response)
        return await self._post_process_response(response)

    async def _pre_process_request(self, request: HttpRequest) -> HttpRequest:
        return request

    @abstractmethod
    async def _release_raw_response(self, response: Any) -> None:
        raise NotImplementedError

    async def _post_process_response(self, response: Any) -> Any:
        return response

    async def _pre_process_response(self, response: Any) -> Any:
        if not await self._response_ok(response):
            return await self.on_error(response)

        body = await self._response_body(response)
        try:
            return self.client.response_body_factory.load(
                body,
                self.method_spec.response_type,
            )
        except (ValueError, TypeError, AttributeError) as e:
            raise MalformedResponse from e

    async def _on_error_default(self, response: Any) -> NoReturn:
        raise ClientLibraryError

    @abstractmethod
    async def _response_body(self, response: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def _response_ok(self, response: Any) -> bool:
        raise NotImplementedError
