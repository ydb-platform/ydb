import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Type, Union

import grpc
from google.protobuf.empty_pb2 import Empty

from yandex.cloud.operation.operation_service_pb2 import GetOperationRequest
from yandex.cloud.operation.operation_service_pb2_grpc import OperationServiceStub
from yandexcloud._backoff import backoff_exponential_jittered_min_interval
from yandexcloud._retry_interceptor import RetryInterceptor
from yandexcloud.operations import OperationError, OperationResult

if TYPE_CHECKING:
    import google.protobuf.message

    from yandex.cloud.operation.operation_pb2 import Operation
    from yandexcloud._sdk import SDK


def operation_waiter(sdk: "SDK", operation_id: str, timeout: Optional[float]) -> "OperationWaiter":
    retriable_codes = (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.INTERNAL,
    )
    # withstand server downtime for ~3.4 minutes with an exponential backoff
    retry_interceptor = RetryInterceptor(
        max_retry_count=13,
        per_call_timeout=30,
        back_off_func=backoff_exponential_jittered_min_interval(),
        retriable_codes=retriable_codes,
    )
    operation_service = sdk.client(
        OperationServiceStub,
        interceptor=retry_interceptor,
    )
    return OperationWaiter(operation_id, operation_service, timeout)


def wait_for_operation(sdk: "SDK", operation_id: str, timeout: Optional[float]) -> Optional["Operation"]:
    waiter = operation_waiter(sdk, operation_id, timeout)
    for _ in waiter:
        time.sleep(1)
    return waiter.operation


def get_operation_result(
    sdk: "SDK",
    operation: "Operation",
    response_type: Optional[Type["google.protobuf.message.Message"]] = None,
    meta_type: Optional[Type["google.protobuf.message.Message"]] = None,
    timeout: Optional[float] = None,
    logger: Optional[logging.Logger] = None,
) -> Union["OperationResult", "OperationError"]:
    if not logger:
        logger = logging.getLogger()
        logger.addHandler(logging.NullHandler())
    operation_result = OperationResult(operation)
    created_at = datetime.fromtimestamp(operation.created_at.seconds)
    message = (
        "Running Yandex.Cloud operation. ID: {id}. "
        "Description: {description}. Created at: {created_at}. "
        "Created by: {created_by}."
    )
    message = message.format(
        id=operation.id,
        description=operation.description,
        created_at=created_at,
        created_by=operation.created_by,
    )
    if meta_type and meta_type is not Empty:
        unpacked_meta = meta_type()
        operation.metadata.Unpack(unpacked_meta)
        operation_result.meta = unpacked_meta
        message += f" Meta: {unpacked_meta}."
    logger.info(message)
    result = wait_for_operation(sdk, operation.id, timeout=timeout)
    if result is None:
        return OperationError(message="Unexpected operation result", operation_result=OperationResult(operation))
    if result.error and result.error.code:
        error_message = (
            "Error Yandex.Cloud operation. ID: {id}. Error code: {code}. Details: {details}. Message: {message}."
        )
        error_message = error_message.format(
            id=result.id,
            code=result.error.code,
            details=result.error.details,
            message=result.error.message,
        )
        logger.error(error_message)
        raise OperationError(message=error_message, operation_result=OperationResult(operation))

    log_message = f"Done Yandex.Cloud operation. ID: {operation.id}."
    if response_type and response_type is not Empty:
        unpacked_response = response_type()
        result.response.Unpack(unpacked_response)
        operation_result.response = unpacked_response
        log_message += f" Response: {unpacked_response}."
    logger.info(log_message)
    return operation_result


class OperationWaiter:
    def __init__(self, operation_id: str, operation_service: "OperationServiceStub", timeout: Optional[float] = None):
        self.__operation: Optional["Operation"] = None
        self.__operation_id = operation_id
        self.__operation_service = operation_service
        self.__deadline = time.time() + timeout if timeout else None

    @property
    def operation(self) -> Optional["Operation"]:
        return self.__operation

    @property
    def done(self) -> bool:
        self.__operation = self.__operation_service.Get(GetOperationRequest(operation_id=self.__operation_id))
        return self.__operation is not None and self.__operation.done

    def __iter__(self) -> "OperationWaiter":
        return self

    def __next__(self) -> None:
        if self.done or self.__deadline is not None and time.time() >= self.__deadline:
            raise StopIteration()

    next = __next__  # for Python 2
