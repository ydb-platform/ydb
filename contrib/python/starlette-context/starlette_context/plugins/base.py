import abc
import uuid
from typing import Any

from starlette.datastructures import MutableHeaders
from starlette.requests import HTTPConnection, Request
from starlette.responses import Response
from starlette.types import Message

from starlette_context import context
from starlette_context.errors import ConfigurationError, WrongUUIDError

__all__ = ["Plugin", "PluginUUIDBase"]


class Plugin(metaclass=abc.ABCMeta):
    """
    Base class for building those plugins to extract things from request.

    One plugin should be responsible for extracting one thing.
    key: the key that allows to access value in headers
    """

    key: str

    async def extract_value_from_header_by_key(
        self, request: Request | HTTPConnection
    ) -> Any | None:
        return request.headers.get(self.key)

    async def process_request(
        self, request: Request | HTTPConnection
    ) -> Any | None:
        """
        Runs always on request.

        Extracts value from header by default.
        """
        if not isinstance(self.key, str):
            raise ConfigurationError(
                f"Plugin {self.__class__.__name__} is missing a valid key."
            )
        return await self.extract_value_from_header_by_key(request)

    async def enrich_response(self, arg: Response | Message) -> None:
        """
        Runs always on response.

        Does nothing by default.
        """
        ...


class PluginUUIDBase(Plugin):
    uuid_functions_mapper = {4: uuid.uuid4}

    def __init__(
        self,
        force_new_uuid: bool = False,
        version: int = 4,
        validate: bool = True,
        error_response: Response | None = None,
    ):
        if version not in self.uuid_functions_mapper:
            raise ConfigurationError(
                f"UUID version {version} is not supported."
            )
        self.force_new_uuid = force_new_uuid
        self.version = version
        self.validate = validate
        self.error_response = error_response

    def validate_uuid(self, uuid_to_validate: str) -> None:
        try:
            uuid.UUID(uuid_to_validate, version=self.version)
        except Exception as e:
            raise WrongUUIDError(
                "Wrong uuid", error_response=self.error_response
            ) from e

    def get_new_uuid(self) -> str:
        func = self.uuid_functions_mapper[self.version]
        return func().hex

    async def extract_value_from_header_by_key(
        self, request: Request | HTTPConnection
    ) -> str | None:
        value = await super().extract_value_from_header_by_key(request)

        # if force_new_uuid or value was not found, create one
        if self.force_new_uuid or not value:
            value = self.get_new_uuid()

        if self.validate:
            self.validate_uuid(value)

        return value

    async def enrich_response(self, arg: Any) -> None:
        value = str(context.get(self.key))

        # for ContextMiddleware
        if isinstance(arg, Response):
            arg.headers[self.key] = value
        # for ContextPureMiddleware
        else:
            if arg["type"] == "http.response.start":
                headers = MutableHeaders(scope=arg)
                headers.append(self.key, value)
