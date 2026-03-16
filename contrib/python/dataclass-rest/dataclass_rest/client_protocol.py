from typing import (
    Any,
    Callable,
    Optional,
    Protocol,
    Type,
    TypeVar,
    runtime_checkable,
)

from .http_request import HttpRequest


@runtime_checkable
class ClientMethodProtocol(Protocol):
    def get_query_params_type(self) -> Type:
        raise NotImplementedError


TypeT = TypeVar("TypeT")


class FactoryProtocol(Protocol):
    def load(self, data: Any, class_: Type[TypeT]) -> TypeT:
        raise NotImplementedError

    def dump(
        self,
        data: TypeT,
        class_: Optional[Type[TypeT]] = None,
    ) -> Any:
        raise NotImplementedError


class ClientProtocol(Protocol):
    request_body_factory: FactoryProtocol
    request_args_factory: FactoryProtocol
    response_body_factory: FactoryProtocol
    method_class: Optional[Callable]

    def do_request(
        self,
        request: HttpRequest,
    ) -> Any:
        raise NotImplementedError
