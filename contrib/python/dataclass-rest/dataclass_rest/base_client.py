from adaptix import Retort

from .client_protocol import (
    ClientProtocol,
    FactoryProtocol,
)


class BaseClient(ClientProtocol):
    def __init__(self):
        self.request_body_factory = self._init_request_body_factory()
        self.request_args_factory = self._init_request_args_factory()
        self.response_body_factory = self._init_response_body_factory()

    def _init_request_body_factory(self) -> FactoryProtocol:
        return Retort()

    def _init_request_args_factory(self) -> FactoryProtocol:
        return Retort()

    def _init_response_body_factory(self) -> FactoryProtocol:
        return self.request_body_factory
