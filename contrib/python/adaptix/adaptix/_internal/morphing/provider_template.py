from abc import ABC, abstractmethod
from collections.abc import Container, Sequence
from typing import final

from ..common import Dumper, Loader, TypeHint
from ..provider.essential import CannotProvide, Mediator, Provider, RequestChecker, RequestHandlerRegisterRecord
from ..provider.loc_stack_filtering import ExactOriginLSC
from ..provider.loc_stack_tools import format_type
from ..provider.located_request import LocatedRequestChecker, LocatedRequestMethodsProvider
from ..provider.methods_provider import method_handler
from ..provider.request_checkers import AlwaysTrueRequestChecker
from ..type_tools import get_generic_args, normalize_type
from .json_schema.definitions import JSONSchema
from .json_schema.request_cls import JSONSchemaRequest
from .json_schema.schema_model import JSONSchemaDialect
from .request_cls import DumperRequest, LoaderRequest


class LoaderProvider(LocatedRequestMethodsProvider, ABC):
    @method_handler
    @abstractmethod
    def provide_loader(self, mediator: Mediator[Loader], request: LoaderRequest) -> Loader:
        ...


class DumperProvider(LocatedRequestMethodsProvider, ABC):
    @method_handler
    @abstractmethod
    def provide_dumper(self, mediator: Mediator[Dumper], request: DumperRequest) -> Dumper:
        ...


class JSONSchemaProvider(LocatedRequestMethodsProvider, ABC):
    SUPPORTED_JSON_SCHEMA_DIALECTS: Container[str] = (JSONSchemaDialect.DRAFT_2020_12, )

    @final
    @method_handler
    def provide_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        if request.ctx.dialect not in self.SUPPORTED_JSON_SCHEMA_DIALECTS:
            raise CannotProvide(f"Dialect {request.ctx.dialect} is not supported for this type")
        return self._generate_json_schema(mediator, request)

    @abstractmethod
    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        ...


class MorphingProvider(
    LoaderProvider,
    DumperProvider,
    JSONSchemaProvider,
    ABC,
):
    pass


class ProxyProvider(Provider, ABC):
    def __init__(self, *, for_loader: bool = True, for_dumper: bool = True):
        self._for_loader = for_loader
        self._for_dumper = for_dumper

    def get_request_handlers(self) -> Sequence[RequestHandlerRegisterRecord]:
        result: list[RequestHandlerRegisterRecord] = []
        if self._for_loader:
            result.append(
                (LoaderRequest, self._get_request_checker(), self._provide_proxy),
            )
        if self._for_dumper:
            result.append(
                (DumperRequest, self._get_request_checker(), self._provide_proxy),
            )
        return result

    def _provide_proxy(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.mandatory_provide(
            request.with_loc_stack(
                request.loc_stack.replace_last_type(
                    self._get_proxy_target(request.loc_stack.last.type),
                ),
            ),
            lambda x: self._get_error_text(),
        )

    @abstractmethod
    def _get_request_checker(self) -> RequestChecker:
        ...

    @abstractmethod
    def _get_proxy_target(self, tp: TypeHint) -> TypeHint:
        ...

    @abstractmethod
    def _get_error_text(self) -> str:
        ...


class ABCProxy(ProxyProvider):
    def __init__(self, abstract: TypeHint, impl: TypeHint, *, for_loader: bool = True, for_dumper: bool = True):
        self._abstract = normalize_type(abstract).origin
        self._impl = impl
        super().__init__(for_dumper=for_dumper, for_loader=for_loader)

    def _get_request_checker(self) -> RequestChecker:
        return LocatedRequestChecker(ExactOriginLSC(self._abstract))

    def _get_proxy_target(self, tp: TypeHint) -> TypeHint:
        return self._impl[get_generic_args(tp)]

    def _get_error_text(self) -> str:
        return f"Try to proxy abstract {format_type(self._abstract)} to {format_type(self._impl)}"


class ToVarTupleProxy(ABCProxy):
    def __init__(self, abstract: TypeHint, *, for_loader: bool = True, for_dumper: bool = True):
        super().__init__(abstract, tuple, for_loader=for_loader, for_dumper=for_dumper)

    def _get_proxy_target(self, tp: TypeHint) -> TypeHint:
        return self._impl[(get_generic_args(tp)[0], ...)]


class DelegatingProvider(ProxyProvider, ABC):
    def _get_request_checker(self) -> RequestChecker:
        return AlwaysTrueRequestChecker()
