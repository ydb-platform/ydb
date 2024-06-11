import collections
import time
import uuid
from typing import Callable, Iterable, Optional

import grpc


class _ClientCallDetails(
    collections.namedtuple(
        "_ClientCallDetails", ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression")
    ),
    grpc.ClientCallDetails,
):
    pass


class _RetryCall(Exception):
    pass


class RetryInterceptor(grpc.UnaryUnaryClientInterceptor):
    """RetryInterceptor implements grpc retries.
    It supports retries quantity, list of retriable codes, backoff function,
    per retry call timeout, and writing retry attempt to call metadata.
    Important nodes:
    1. If default parameters are used, no retries will be executed.
    2. It will always add idempotency token to grpc call metadata, if one is not already present.
    3. Negative max_retry_count parameter will result in INFINITE retries.
    4. DEADLINE_EXCEEDED and CANCELLED are not retriable codes.
    5. Default retriable codes are UNAVAILABLE and RESOURCE_EXHAUSTED.
    6. Backoff function is called with retry attempt counter and should return sleep time in seconds (float).
    """

    _DEFAULT_RETRIABLE_CODES = (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    )
    _NON_RETRIABLE_CODES = [grpc.StatusCode.CANCELLED, grpc.StatusCode.DEADLINE_EXCEEDED]
    _IDEMPOTENCY_TOKEN_METADATA_KEY = "idempotency-key"
    _ATTEMPT_METADATA_KEY = "x-retry-attempt"

    def __init__(
        self,
        max_retry_count: int = 0,
        retriable_codes: Iterable["grpc.StatusCode"] = _DEFAULT_RETRIABLE_CODES,
        add_retry_count_to_header: bool = False,
        back_off_func: Optional[Callable[[int], float]] = None,
        per_call_timeout: Optional[float] = None,
    ):
        # pylint: disable=super-init-not-called
        self.__max_retry_count = max_retry_count
        self.__retriable_codes = retriable_codes
        self.__add_retry_count_to_header = add_retry_count_to_header
        self.__back_off_func = back_off_func
        self.__per_call_timeout = per_call_timeout

    def intercept_unary_unary(
        self,
        continuation: Callable[["grpc.ClientCallDetails", "grpc.TRequest"], "grpc.CallFuture[grpc.TResponse]"],
        client_call_details: "grpc.ClientCallDetails",
        request: "grpc.TRequest",
    ) -> "grpc.CallFuture[grpc.TResponse]":
        client_call_details = self.__add_idempotency_token(client_call_details)

        attempt = 0
        deadline = self.__deadline(client_call_details.timeout)

        while True:
            try:
                return self.__grpc_call(attempt, deadline, continuation, client_call_details, request)
            except _RetryCall:
                attempt += 1

    def __wait_backoff(self, attempt: int, deadline: Optional[float]) -> None:
        if self.__back_off_func is None:
            return

        backoff_timeout = self.__back_off_func(attempt)

        if deadline is not None:
            deadline_timeout = deadline - time.time()

            if backoff_timeout > deadline_timeout:  # pylint: disable=consider-using-min-builtin
                backoff_timeout = deadline_timeout

        if backoff_timeout > 0.0:
            time.sleep(backoff_timeout)

    @staticmethod
    def __deadline(timeout: Optional[float]) -> Optional[float]:
        return time.time() + timeout if timeout is not None else None

    def __is_retriable(self, error: "grpc.StatusCode") -> bool:
        if error in self._NON_RETRIABLE_CODES:
            return False

        if error in self.__retriable_codes:
            return True

        return False

    @staticmethod
    def __min_deadline(d1: Optional[float], d2: Optional[float]) -> Optional[float]:
        if d2 is None and d1 is None:
            return None
        if d1 is None:
            return d2

        if d2 is None:
            return d1

        return min(d1, d2)

    def __grpc_call(
        self,
        attempt: int,
        deadline: Optional[float],
        continuation: Callable[["grpc.ClientCallDetails", "grpc.TRequest"], "grpc.CallFuture[grpc.TResponse]"],
        client_call_details: "grpc.ClientCallDetails",
        request: "grpc.TRequest",
    ) -> "grpc.CallFuture[grpc.TResponse]":
        if attempt > 0:
            if self.__add_retry_count_to_header:
                client_call_details = self.__append_retry_attempt_header(client_call_details, attempt)

            call_deadline = self.__deadline(self.__per_call_timeout)
            call_deadline = self.__min_deadline(deadline, call_deadline)

            if call_deadline is not None:
                client_call_details = self.__adjust_timeout(client_call_details, call_deadline)

        def retry() -> None:
            self.__wait_backoff(attempt, deadline)
            raise _RetryCall()

        try:
            result = continuation(client_call_details, request)
            if isinstance(result, grpc.RpcError):  # type: ignore
                raise result
            return result
        except grpc.RpcError as error:
            # no retries left
            if 0 <= self.__max_retry_count <= attempt:
                raise

            err_code = error.code()  # pylint: disable=no-member
            if err_code == grpc.StatusCode.DEADLINE_EXCEEDED:
                # if there is no per_call_timeout, or it is original deadline -> abort, otherwise, retry call.
                if self.__per_call_timeout is None or deadline is not None and deadline < time.time():
                    raise

                retry()

            if not self.__is_retriable(err_code):
                raise

        retry()
        raise RuntimeError("Unexpected behavior")

    @staticmethod
    def __adjust_timeout(client_call_details: "grpc.ClientCallDetails", deadline: float) -> "grpc.ClientCallDetails":
        timeout = max(deadline - time.time(), 0.0)
        return _ClientCallDetails(
            client_call_details.method,
            timeout,
            client_call_details.metadata,
            client_call_details.credentials,
            getattr(client_call_details, "wait_for_ready", None),
            getattr(client_call_details, "compression", None),
        )

    def __add_idempotency_token(self, client_call_details: "grpc.ClientCallDetails") -> "grpc.ClientCallDetails":
        return self.__append_metadata(client_call_details, self._IDEMPOTENCY_TOKEN_METADATA_KEY, str(uuid.uuid4()))

    def __append_retry_attempt_header(
        self, client_call_details: "grpc.ClientCallDetails", attempt: int
    ) -> "grpc.ClientCallDetails":
        return self.__append_metadata(client_call_details, self._ATTEMPT_METADATA_KEY, str(attempt), force=True)

    @staticmethod
    def __append_metadata(
        client_call_details: "grpc.ClientCallDetails", header: str, value: str, force: bool = False
    ) -> "grpc.ClientCallDetails":
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        if not force:
            # Do not add value, if there is already one.
            for item in metadata:
                if item[0] == header:
                    return client_call_details

        metadata.append(
            (
                header,
                value,
            )
        )
        return _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            getattr(client_call_details, "wait_for_ready", None),
            getattr(client_call_details, "compression", None),
        )
