from abc import ABC, abstractmethod
from typing import final

from ..common import Coercer, Converter
from ..provider.essential import Mediator
from ..provider.methods_provider import MethodsProvider, method_handler
from .request_cls import CoercerRequest, ConverterRequest, LinkingRequest, LinkingResult


class ConverterProvider(MethodsProvider, ABC):
    @final
    @method_handler
    def _outer_provide_converter(self, mediator: Mediator, request: ConverterRequest):
        return self._provide_converter(mediator, request)

    @abstractmethod
    def _provide_converter(self, mediator: Mediator, request: ConverterRequest) -> Converter:
        ...


class CoercerProvider(MethodsProvider, ABC):
    @method_handler
    @abstractmethod
    def _provide_coercer(self, mediator: Mediator, request: CoercerRequest) -> Coercer:
        ...


class LinkingProvider(MethodsProvider, ABC):
    @method_handler
    @abstractmethod
    def _provide_linking(self, mediator: Mediator, request: LinkingRequest) -> LinkingResult:
        ...
