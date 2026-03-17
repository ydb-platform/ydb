from stripe._error import StripeError
from stripe._error_object import OAuthErrorObject


class OAuthError(StripeError):
    def __init__(
        self,
        code,
        description,
        http_body=None,
        http_status=None,
        json_body=None,
        headers=None,
    ):
        super(OAuthError, self).__init__(
            description, http_body, http_status, json_body, headers, code
        )

    def _construct_error_object(self):
        if self.json_body is None:
            return None

        from stripe._api_requestor import _APIRequestor

        return OAuthErrorObject._construct_from(
            values=self.json_body,  # type: ignore
            requestor=_APIRequestor._global_instance(),
            api_mode="V1",
        )


class InvalidClientError(OAuthError):
    pass


class InvalidGrantError(OAuthError):
    pass


class InvalidRequestError(OAuthError):
    pass


class InvalidScopeError(OAuthError):
    pass


class UnsupportedGrantTypeError(OAuthError):
    pass


class UnsupportedResponseTypeError(OAuthError):
    pass
