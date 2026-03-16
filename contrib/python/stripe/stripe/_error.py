from typing import Dict, Optional, Union, cast

from stripe._error_object import ErrorObject


class StripeError(Exception):
    _message: Optional[str]
    http_body: Optional[str]
    http_status: Optional[int]
    json_body: Optional[object]
    headers: Optional[Dict[str, str]]
    code: Optional[str]
    request_id: Optional[str]
    error: Optional["ErrorObject"]

    def __init__(
        self,
        message: Optional[str] = None,
        http_body: Optional[Union[bytes, str]] = None,
        http_status: Optional[int] = None,
        json_body: Optional[object] = None,
        headers: Optional[Dict[str, str]] = None,
        code: Optional[str] = None,
    ):
        super(StripeError, self).__init__(message)

        body: Optional[str] = None
        if http_body:
            # http_body can sometimes be a memoryview which must be cast
            # to a "bytes" before calling decode, so we check for the
            # decode attribute and then cast
            if hasattr(http_body, "decode"):
                try:
                    body = cast(bytes, http_body).decode("utf-8")
                except BaseException:
                    body = (
                        "<Could not decode body as utf-8. "
                        "Please report to support@stripe.com>"
                    )
            elif isinstance(http_body, str):
                body = http_body

        self._message = message
        self.http_body = body
        self.http_status = http_status
        self.json_body = json_body
        self.headers = headers or {}
        self.code = code
        self.request_id = self.headers.get("request-id", None)
        self.error = self._construct_error_object()

    def __str__(self):
        msg = self._message or "<empty message>"
        if self.request_id is not None:
            return "Request {0}: {1}".format(self.request_id, msg)
        else:
            return msg

    # Returns the underlying `Exception` (base class) message, which is usually
    # the raw message returned by Stripe's API. This was previously available
    # in python2 via `error.message`. Unlike `str(error)`, it omits "Request
    # req_..." from the beginning of the string.
    @property
    def user_message(self):
        return self._message

    def __repr__(self):
        return "%s(message=%r, http_status=%r, request_id=%r)" % (
            self.__class__.__name__,
            self._message,
            self.http_status,
            self.request_id,
        )

    def _construct_error_object(self) -> Optional[ErrorObject]:
        if (
            self.json_body is None
            or not isinstance(self.json_body, dict)
            or "error" not in self.json_body
            or not isinstance(self.json_body["error"], dict)
        ):
            return None
        from stripe._error_object import ErrorObject
        from stripe._api_requestor import _APIRequestor

        return ErrorObject._construct_from(
            values=self.json_body["error"],
            requestor=_APIRequestor._global_instance(),
            # We pass in API mode as "V1" here because it's required,
            # but ErrorObject is reused for both V1 and V2 errors.
            api_mode="V1",
        )


class APIError(StripeError):
    pass


class APIConnectionError(StripeError):
    should_retry: bool

    def __init__(
        self,
        message,
        http_body=None,
        http_status=None,
        json_body=None,
        headers=None,
        code=None,
        should_retry=False,
    ):
        super(APIConnectionError, self).__init__(
            message, http_body, http_status, json_body, headers, code
        )
        self.should_retry = should_retry


class StripeErrorWithParamCode(StripeError):
    def __repr__(self):
        return (
            "%s(message=%r, param=%r, code=%r, http_status=%r, "
            "request_id=%r)"
            % (
                self.__class__.__name__,
                self._message,
                self.param,  # pyright: ignore
                self.code,
                self.http_status,
                self.request_id,
            )
        )


class CardError(StripeErrorWithParamCode):
    def __init__(
        self,
        message,
        param,
        code,
        http_body=None,
        http_status=None,
        json_body=None,
        headers=None,
    ):
        super(CardError, self).__init__(
            message, http_body, http_status, json_body, headers, code
        )
        self.param = param


class IdempotencyError(StripeError):
    pass


class InvalidRequestError(StripeErrorWithParamCode):
    def __init__(
        self,
        message,
        param,
        code=None,
        http_body=None,
        http_status=None,
        json_body=None,
        headers=None,
    ):
        super(InvalidRequestError, self).__init__(
            message, http_body, http_status, json_body, headers, code
        )
        self.param = param


class AuthenticationError(StripeError):
    pass


class PermissionError(StripeError):
    pass


class RateLimitError(StripeError):
    pass


class SignatureVerificationError(StripeError):
    def __init__(self, message, sig_header, http_body=None):
        super(SignatureVerificationError, self).__init__(message, http_body)
        self.sig_header = sig_header


# classDefinitions: The beginning of the section generated from our OpenAPI spec
class TemporarySessionExpiredError(StripeError):
    pass


# classDefinitions: The end of the section generated from our OpenAPI spec
