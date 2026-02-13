# -*- coding: utf-8 -*-
from typing import Any, Optional, TYPE_CHECKING

from . import issues
from . import _apis

if TYPE_CHECKING:
    from .settings import BaseRequestSettings
    from .connection import _RpcState


def _forget_operation_request(operation_id: str) -> Any:
    request = _apis.ydb_operation.ForgetOperationRequest(id=operation_id)
    return request


def _forget_operation_response(rpc_state: "_RpcState", response: Any) -> None:  # pylint: disable=W0613
    issues._process_response(response)


def _cancel_operation_request(operation_id: str) -> Any:
    request = _apis.ydb_operation.CancelOperationRequest(id=operation_id)
    return request


def _cancel_operation_response(rpc_state: "_RpcState", response: Any) -> None:  # pylint: disable=W0613
    issues._process_response(response)


def _get_operation_request(operation: "Operation") -> Any:
    request = _apis.ydb_operation.GetOperationRequest(id=operation.id)
    return request


class OperationClient:
    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def cancel(self, operation_id: str, settings: Optional["BaseRequestSettings"] = None) -> Any:
        return self._driver(
            _cancel_operation_request(operation_id),
            _apis.OperationService.Stub,
            _apis.OperationService.CancelOperation,
            _cancel_operation_response,
            settings,
        )

    def forget(self, operation_id: str, settings: Optional["BaseRequestSettings"] = None) -> Any:
        return self._driver(
            _forget_operation_request(operation_id),
            _apis.OperationService.Stub,
            _apis.OperationService.ForgetOperation,
            _forget_operation_response,
            settings,
        )


class Operation:
    __slots__ = ("id", "_driver", "self_cls")

    id: str
    _driver: Any

    def __init__(self, rpc_state: "_RpcState", response: Any, driver: Any = None) -> None:  # pylint: disable=W0613
        # implement proper interface a bit later
        issues._process_response(response.operation)
        self.id = response.operation.id
        self._driver = driver
        # self.ready = operation.ready

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return "<Operation %s>" % (self.id,)

    def _ensure_implements(self) -> None:
        if self._driver is None:
            raise ValueError("Operation doesn't implement request!")

    def cancel(self, settings: Optional["BaseRequestSettings"] = None) -> Any:
        self._ensure_implements()
        return self._driver(
            _cancel_operation_request(self.id),
            _apis.OperationService.Stub,
            _apis.OperationService.CancelOperation,
            _cancel_operation_response,
            settings,
        )

    def forget(self, settings: Optional["BaseRequestSettings"] = None) -> Any:
        self._ensure_implements()
        return self._driver(
            _forget_operation_request(self.id),
            _apis.OperationService.Stub,
            _apis.OperationService.ForgetOperation,
            _forget_operation_response,
            settings,
        )

    def get(self, settings: Optional["BaseRequestSettings"] = None) -> "Operation":
        self._ensure_implements()
        return self._driver(
            _get_operation_request(self),
            _apis.OperationService.Stub,
            _apis.OperationService.GetOperation,
            self.__class__,
            settings,
            (self._driver,),
        )
