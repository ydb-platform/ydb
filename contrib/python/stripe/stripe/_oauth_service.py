from stripe._stripe_service import StripeService
from stripe._error import AuthenticationError
from stripe._encode import _api_encode
from urllib.parse import urlencode

from typing import cast, Optional
from typing_extensions import NotRequired, TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._client_options import _ClientOptions
    from stripe._request_options import RequestOptions
    from stripe._oauth import OAuth


class OAuthService(StripeService):
    _options: Optional["_ClientOptions"]

    def __init__(self, client, options=None):
        super(OAuthService, self).__init__(client)
        self._options = options

    class OAuthAuthorizeUrlOptions(TypedDict):
        express: NotRequired[bool]
        """
        Express only
        Boolean to indicate that the user should be sent to the express onboarding flow instead of the standard onboarding flow.
        """

    def _set_client_id(self, params):
        if "client_id" in params:
            return

        client_id = self._options and self._options.client_id

        if client_id:
            params["client_id"] = client_id
            return

        raise AuthenticationError(
            "No client_id provided. (HINT: set your client_id when configuring "
            'your StripeClient: "stripe.StripeClient(..., client_id=<CLIENT_ID>)"). '
            "You can find your client_ids in your Stripe dashboard at "
            "https://dashboard.stripe.com/account/applications/settings, "
            "after registering your account as a platform. See "
            "https://stripe.com/docs/connect/standalone-accounts for details, "
            "or email support@stripe.com if you have any questions."
        )

    def authorize_url(
        self,
        params: Optional["OAuth.OAuthAuthorizeUrlParams"] = None,
        options: Optional[OAuthAuthorizeUrlOptions] = None,
    ) -> str:
        if params is None:
            params = {}
        if options is None:
            options = {}

        if options.get("express"):
            path = "/express/oauth/authorize"
        else:
            path = "/oauth/authorize"

        self._set_client_id(params)
        if "response_type" not in params:
            params["response_type"] = "code"
        query = urlencode(list(_api_encode(params)))

        # connect_api_base will be always set to stripe.DEFAULT_CONNECT_API_BASE
        # if it is not overridden on the client explicitly.
        connect_api_base = self._requestor.base_addresses.get("connect")
        assert connect_api_base is not None

        url = connect_api_base + path + "?" + query
        return url

    def token(
        self,
        params: "OAuth.OAuthTokenParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OAuth.OAuthToken":
        if options is None:
            options = {}
        return cast(
            "OAuth.OAuthToken",
            self._requestor.request(
                "post",
                "/oauth/token",
                params=params,
                options=options,
                base_address="connect",
            ),
        )

    def deauthorize(
        self,
        params: "OAuth.OAuthDeauthorizeParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OAuth.OAuthDeauthorization":
        if options is None:
            options = {}
        self._set_client_id(params)
        return cast(
            "OAuth.OAuthDeauthorization",
            self._requestor.request(
                "post",
                "/oauth/deauthorize",
                params=params,
                options=options,
                base_address="connect",
            ),
        )
