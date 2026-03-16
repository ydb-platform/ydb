# Used for global variables
from stripe import connect_api_base
from stripe._error import AuthenticationError
from stripe._api_requestor import _APIRequestor
from stripe._encode import _api_encode
from urllib.parse import urlencode
from stripe._stripe_object import StripeObject

from typing import List, cast, Optional
from typing_extensions import (
    Literal,
    NotRequired,
    TypedDict,
    Unpack,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions


class OAuth(object):
    class OAuthToken(StripeObject):
        access_token: Optional[str]
        """
        The access token you can use to make requests on behalf of this Stripe account. Use it as you would any Stripe secret API key.
        This key does not expire, but may be revoked by the user at any time (you'll get a account.application.deauthorized webhook event when this happens).
        """
        scope: Optional[str]
        """
        The scope granted to the access token, depending on the scope of the authorization code and scope parameter.
        """
        livemode: Optional[bool]
        """
        The live mode indicator for the token. If true, the access_token can be used as a live secret key. If false, the access_token can be used as a test secret key.
        Depends on the mode of the secret API key used to make the request.
        """
        token_type: Optional[Literal["bearer"]]
        """
        Will always have a value of bearer.
        """
        refresh_token: Optional[str]
        """
        Can be used to get a new access token of an equal or lesser scope, or of a different live mode (where applicable).
        """
        stripe_user_id: Optional[str]
        """
        The unique id of the account you have been granted access to, as a string.
        """
        stripe_publishable_key: Optional[str]
        """
        A publishable key that can be used with this account. Matches the mode—live or test—of the token.
        """

    class OAuthDeauthorization(StripeObject):
        stripe_user_id: str
        """
        The unique id of the account you have revoked access to, as a string.
        This is the same as the stripe_user_id you passed in.
        If this is returned, the revocation was successful.
        """

    class OAuthAuthorizeUrlParams(TypedDict):
        client_id: NotRequired[str]
        """
        The unique identifier provided to your application, found in your application settings.
        """
        response_type: NotRequired[Literal["code"]]
        """
        The only option at the moment is `'code'`.
        """
        redirect_uri: NotRequired[str]
        """
        The URL for the authorize response redirect. If provided, this must exactly match one of the comma-separated redirect_uri values in your application settings.
        To protect yourself from certain forms of man-in-the-middle attacks, the live mode redirect_uri must use a secure HTTPS connection.
        Defaults to the redirect_uri in your application settings if not provided.
        """
        scope: NotRequired[str]
        """
        read_write or read_only, depending on the level of access you need.
        Defaults to read_only.
        """
        state: NotRequired[str]
        """
        An arbitrary string value we will pass back to you, useful for CSRF protection.
        """
        stripe_landing: NotRequired[str]
        """
        login or register, depending on what type of screen you want your users to see. Only override this to be login if you expect all your users to have Stripe accounts already (e.g., most read-only applications, like analytics dashboards or accounting software).
        Defaults to login for scope read_only and register for scope read_write.
        """
        always_prompt: NotRequired[bool]
        """
        Boolean to indicate that the user should always be asked to connect, even if they're already connected.
        Defaults to false.
        """
        suggested_capabilities: NotRequired[List[str]]
        """
        Express only
        An array of capabilities to apply to the connected account.
        """
        stripe_user: NotRequired["OAuth.OAuthAuthorizeUrlParamsStripeUser"]
        """
        Stripe will use these to prefill details in the account form for new users.
        Some prefilled fields (e.g., URL or product category) may be automatically hidden from the user's view.
        Any parameters with invalid values will be silently ignored.
        """

    class OAuthAuthorizeUrlParamsStripeUser(TypedDict):
        """
        A more detailed explanation of what it means for a field to be
        required or optional can be found in our API documentation.
        See `Account Creation (Overview)` and `Account Update`
        """

        email: NotRequired[str]
        """
        Recommended
        The user's email address. Must be a valid email format.
        """
        url: NotRequired[str]
        """
        Recommended
        The URL for the user's business. This may be the user's website, a profile page within your application, or another publicly available profile for the business, such as a LinkedIn or Facebook profile.
        Must be URL-encoded and include a scheme (http or https).
        If you will be prefilling this field, we highly recommend that the linked page contain a description of the user's products or services and their contact information. If we don't have enough information, we'll have to reach out to the user directly before initiating payouts.
        """
        country: NotRequired[str]
        """
        Two-letter country code (e.g., US or CA).
        Must be a country that Stripe currently supports.
        """
        phone_number: NotRequired[str]
        """
        The business phone number. Must be 10 digits only.
        Must also prefill stripe_user[country] with the corresponding country.
        """
        business_name: NotRequired[str]
        """
        The legal name of the business, also used for the statement descriptor.
        """
        business_type: NotRequired[str]
        """
        The type of the business.
        Must be one of sole_prop, corporation, non_profit, partnership, or llc.
        """
        first_name: NotRequired[str]
        """
        First name of the person who will be filling out a Stripe application.
        """
        last_name: NotRequired[str]
        """
        Last name of the person who will be filling out a Stripe application.
        """
        dob_day: NotRequired[str]
        """
        Day (0-31), month (1-12), and year (YYYY, greater than 1900) for the birth date of the person who will be filling out a Stripe application.
        If you choose to pass these parameters, you must pass all three.
        """
        dob_month: NotRequired[str]
        """
        Day (0-31), month (1-12), and year (YYYY, greater than 1900) for the birth date of the person who will be filling out a Stripe application.
        If you choose to pass these parameters, you must pass all three.
        """
        dob_year: NotRequired[str]
        """
        Day (0-31), month (1-12), and year (YYYY, greater than 1900) for the birth date of the person who will be filling out a Stripe application.
        If you choose to pass these parameters, you must pass all three.
        """
        street_address: NotRequired[str]
        """
        Standard only
        Street address of the business.
        """
        city: NotRequired[str]
        """
        Address city of the business.
        We highly recommend that you also prefill stripe_user[country] with the corresponding country.
        """
        state: NotRequired[str]
        """
        Standard only
        Address state of the business, must be the two-letter state or province code (e.g., NY for a U.S. business or AB for a Canadian one).
        Must also prefill stripe_user[country] with the corresponding country.
        """
        zip: NotRequired[str]
        """
        Standard only
        Address ZIP code of the business, must be a string.
        We highly recommend that you also prefill stripe_user[country] with the corresponding country.
        """
        physical_product: NotRequired[str]
        """
        Standard only
        A string: true if the user sells a physical product, false otherwise.
        """
        product_description: NotRequired[str]
        """
        A description of what the business is accepting payments for.
        """
        currency: NotRequired[str]
        """
        Standard only
        Three-letter ISO code representing currency, in lowercase (e.g., usd or cad).
        Must be a valid country and currency combination that Stripe supports.
        Must prefill stripe_user[country] with the corresponding country.
        """
        first_name_kana: NotRequired[str]
        """
        The Kana variation of the first name of the person who will be filling out a Stripe application.
        Must prefill stripe_user[country] with JP, as this parameter is only relevant for Japan.
        """
        first_name_kanji: NotRequired[str]
        """
        The Kanji variation of the first name of the person who will be filling out a Stripe application.
        Must prefill stripe_user[country] with JP, as this parameter is only relevant for Japan.
        """
        last_name_kana: NotRequired[str]
        """
        The Kana variation of the last name of the person who will be filling out a Stripe application.
        Must prefill stripe_user[country] with JP, as this parameter is only relevant for Japan.
        """
        last_name_kanji: NotRequired[str]
        """
        The Kanji variation of the last name of the person who will be filling out a Stripe application.
        Must prefill stripe_user[country] with JP, as this parameter is only relevant for Japan.
        """
        gender: NotRequired[str]
        """
        The gender of the person who will be filling out a Stripe application. (International regulations require either male or female.)
        Must prefill stripe_user[country] with JP, as this parameter is only relevant for Japan.
        """
        block_kana: NotRequired[str]
        """
        Standard only
        The Kana variation of the address block.
        This parameter is only relevant for Japan. You must prefill stripe_user[country] with JP and stripe_user[zip] with a valid Japanese postal code to use this parameter.
        """
        block_kanji: NotRequired[str]
        """
        Standard only
        The Kanji variation of the address block.
        This parameter is only relevant for Japan. You must prefill stripe_user[country] with JP and stripe_user[zip] with a valid Japanese postal code to use this parameter.
        """
        building_kana: NotRequired[str]
        """
        Standard only
        The Kana variation of the address building.
        This parameter is only relevant for Japan. You must prefill stripe_user[country] with JP and stripe_user[zip] with a valid Japanese postal code to use this parameter.
        """
        building_kanji: NotRequired[str]
        """
        Standard only
        The Kanji variation of the address building.
        This parameter is only relevant for Japan. You must prefill stripe_user[country] with JP and stripe_user[zip] with a valid Japanese postal code to use this parameter.
        """

    class OAuthTokenParams(TypedDict):
        grant_type: Literal["authorization_code", "refresh_token"]
        """
        `'authorization_code'` when turning an authorization code into an access token, or `'refresh_token'` when using a refresh token to get a new access token.
        """
        code: NotRequired[str]
        """
        The value of the code or refresh_token, depending on the grant_type.
        """
        refresh_token: NotRequired[str]
        """
        The value of the code or refresh_token, depending on the grant_type.
        """
        scope: NotRequired[str]
        """
        When requesting a new access token from a refresh token, any scope that has an equal or lesser scope as the refresh token. Has no effect when requesting an access token from an authorization code.
        Defaults to the scope of the refresh token.
        """
        assert_capabilities: NotRequired[List[str]]
        """
        Express only
        Check whether the suggested_capabilities were applied to the connected account.
        """

    class OAuthDeauthorizeParams(TypedDict):
        client_id: NotRequired[str]
        """
        The client_id of the application that you'd like to disconnect the account from.
        The account must be connected to this application.
        """
        stripe_user_id: str
        """
        The account you'd like to disconnect from.
        """

    @staticmethod
    def _set_client_id(params):
        if "client_id" in params:
            return

        from stripe import client_id

        if client_id:
            params["client_id"] = client_id
            return

        raise AuthenticationError(
            "No client_id provided. (HINT: set your client_id using "
            '"stripe.client_id = <CLIENT-ID>"). You can find your client_ids '
            "in your Stripe dashboard at "
            "https://dashboard.stripe.com/account/applications/settings, "
            "after registering your account as a platform. See "
            "https://stripe.com/docs/connect/standalone-accounts for details, "
            "or email support@stripe.com if you have any questions."
        )

    @staticmethod
    def authorize_url(
        express: bool = False, **params: Unpack[OAuthAuthorizeUrlParams]
    ) -> str:
        if express is False:
            path = "/oauth/authorize"
        else:
            path = "/express/oauth/authorize"

        OAuth._set_client_id(params)
        if "response_type" not in params:
            params["response_type"] = "code"
        query = urlencode(list(_api_encode(params)))
        url = connect_api_base + path + "?" + query
        return url

    @staticmethod
    def token(
        api_key: Optional[str] = None, **params: Unpack[OAuthTokenParams]
    ) -> OAuthToken:
        options: "RequestOptions" = {"api_key": api_key}
        requestor = _APIRequestor._global_instance()
        return cast(
            "OAuth.OAuthToken",
            requestor.request(
                "post",
                "/oauth/token",
                params=params,
                options=options,
                base_address="connect",
            ),
        )

    @staticmethod
    def deauthorize(
        api_key: Optional[str] = None, **params: Unpack[OAuthDeauthorizeParams]
    ) -> OAuthDeauthorization:
        options: "RequestOptions" = {"api_key": api_key}
        requestor = _APIRequestor._global_instance()
        OAuth._set_client_id(params)
        return cast(
            "OAuth.OAuthDeauthorization",
            requestor.request(
                "post",
                "/oauth/deauthorize",
                params=params,
                options=options,
                base_address="connect",
            ),
        )
