__all__ = ["ApiKey", "UserCreds", "ClientCreds", "ServiceAccountCreds"]


from typing import Mapping, Optional, Sequence
from ..utils import _dict


class ApiKey(str):
    pass


class IdToken(_dict):
    """
    OpenID connect ID token

    Example:

        {"iss":"accounts.google.com",
        "at_hash":"HK6E_P6Dh8Y93mRNtsDB1Q",
        "email_verified":"true",
        "sub":"10769150350006150715113082367",
        "azp":"1234987819200.apps.googleusercontent.com",
        "email":"jsmith@example.com",
        "aud":"1234987819200.apps.googleusercontent.com",
        "iat":1353601026,
        "exp":1353604926,
        "nonce": "0394852-3190485-2490358",
        "hd":"example.com"}

    Attributes:

        iss (str):

            * PROVIDED: always

            * The Issuer Identifier for the Issuer of the response. Always https://accounts.google.com or accounts.google.com for Google ID tokens.

        at_hash	(str):

            * Access token hash.

            * Provides validation that the access token is tied to the identity token.

            * If the ID token is issued with an access token in the server flow, this is always included.

            * This can be used as an alternate mechanism to protect against cross-site request forgery attacks, but if you follow Step 1 and Step 3 it is not necessary to verify the access token.

        email_verified (bool)

            * True if the user's e-mail address has been verified; otherwise false.

        sub (str):

            * PROVIDED: always

            * An identifier for the user, unique among all Google accounts and never reused.

            * A Google account can have multiple emails at different points in time, but the sub value is never changed.

            * Use sub within your application as the unique-identifier key for the user.

        azp	(str):

            * The client_id of the authorized presenter.

            * This claim is only needed when the party requesting the ID token is not the same as the audience of the ID token.

            * This may be the case at Google for hybrid apps where a web application and Android app have a different client_id but share the same project.

        email (str):

            * The user's email address.

            * This may not be unique and is not suitable for use as a primary key.

            * Provided only if your scope included the string "email".

        profile (str):

            * The URL of the user's profile page. Might be provided when:

                * The request scope included the string "profile"

                * The ID token is returned from a token refresh

            * When profile claims are present, you can use them to update your app's user records.

            * Note that this claim is never guaranteed to be present.

        picture (str):

            * The URL of the user's profile picture. Might be provided when:

                * The request scope included the string "profile"

                * The ID token is returned from a token refresh

            * When picture claims are present, you can use them to update your app's user records.

            * Note that this claim is never guaranteed to be present.

        name (str):

            * The user's full name, in a displayable form. Might be provided when:

                * The request scope included the string "profile"

                * The ID token is returned from a token refresh

            * When name claims are present, you can use them to update your app's user records. Note that this claim is never guaranteed to be present.

        aud (str):

            * PROVIDED:	always

            * The audience that this ID token is intended for.

            * It must be one of the OAuth 2.0 client IDs of your application.

        iat (str):

            * PROVIDED: always

            * The time the ID token was issued, represented in Unix time (integer seconds).

        exp (str):

            * PROVIDED: always

            * The time the ID token expires, represented in Unix time (integer seconds).

        nonce (str):

            * The value of the nonce supplied by your app in the authentication request.

            * You should enforce protection against replay attacks by ensuring it is presented only once.

        hd (str):

            * The hosted G Suite domain of the user. Provided only if the user belongs to a hosted domain.
    """

    pass


class UserCreds(_dict):
    """
    OAuth2 User Credentials Dictionary

    Attributes:

        access_token (str): Access Token
        refresh_token (str): Refresh Token
        refresh_token_expires_in (int): seconds till refresh token expiry from creation
        expires_in (int): seconds till expiry from creation
        expires_at (str): JSON datetime ISO 8601 expiry datetime
        scopes (list): list of scopes owned by access token

        id_token (aiogoogle.auth.creds.IdToken): Decoded OpenID JWT
        id_token_jwt (str): Encoded OpenID JWT

        token_type (str): Bearer
        token_uri (str): URI where this token was issued from
        token_info_uri (str): URI where one could get more info about this token
        revoke_uri (str): URI where this token should be revoked

    """

    def __init__(
        self,
        access_token=None,
        refresh_token=None,
        refresh_token_expires_in=None,
        expires_in=None,
        expires_at=None,
        scopes=None,
        id_token=None,
        id_token_jwt=None,
        token_type=None,
        token_uri=None,
        token_info_uri=None,
        revoke_uri=None,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.refresh_token_expires_in = refresh_token_expires_in
        self.expires_in = expires_in
        self.expires_at = expires_at
        self.scopes = scopes

        self.id_token = id_token
        self.id_token_jwt = id_token_jwt

        self.token_type = token_type
        self.token_uri = token_uri
        self.token_info_uri = token_info_uri
        self.revoke_uri = revoke_uri


class ClientCreds(_dict):
    """
    OAuth2 Client Credentials Dictionary

    Examples:

        Scopes: ['openid', 'email', 'https://www.googleapis.com/auth/youtube.force-ssl']

    Attributes:

        client_id (str): OAuth2 client ID
        client_secret (str): OAuth2 client secret
        scopes (list): List of scopes that this client should request from resource server
        redirect_uri (str): client's redirect uri
    """

    def __init__(
        self, client_id=None, client_secret=None, scopes=None, redirect_uri=None
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self.redirect_uri = redirect_uri


class ServiceAccountCreds(_dict):
    """
    Service account key (the one you download from Google Cloud Console) +
    some additional optional attributes.
    If you have created the service account using Google's IAM REST API, rather
    than downloading it using gcloud or Google's cloud console, the downloaded
    JSON key file will look different. Check here:
    https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating_service_account_keys
    to be able to pass the correct info to this data model.

    Attributes:

        type (string): Should be "service_account"

        project_id (string): Your project ID

        private_key_id (string): Private key ID

        private_key (string): RSA Private key

        client_email (string): service account email

        client_id (string): service account ID

        auth_uri (string): Auth URI

        token_uri (string): Token URI

        auth_provider_x509_cert_url (string): Auth provider cert URL

        client_x509_cert_url (string): Cert URL

        scopes (Sequence[str]): Scopes to request during the authorization grant.

        subject (str): For domain-wide delegation, the email address of the user to for which to request delegated access.

        additional_claims (Mapping[str, str]): Any additional claims for the JWT assertion used in the authorization grant.

    Example:

        ::

            service_account_creds = ServiceAccountCreds(
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
                **json.load(open('service-account-key.json'))
            )

            # or

            service_account_creds = {
                "scopes": ["..."],
                **json.load(open('service-account-key.json'))
            }
    """
    def __init__(
        self, type: Optional[str] = None, project_id: Optional[str] = None, private_key_id: Optional[str] = None, private_key: Optional[str] = None,
        client_email: Optional[str] = None, client_id: Optional[str] = None, auth_uri: Optional[str] = None, token_uri: Optional[str] = None, auth_provider_x509_cert_url: Optional[str] = None,
        client_x509_cert_url: Optional[str] = None, subject: Optional[str] = None, scopes: Optional[Sequence[str]] = None, additional_claims: Optional[Mapping[str, str]] = None,
        universe_domain: str = 'googleapis.com'
    ):
        self.type = type
        self.project_id = project_id
        self.private_key_id = private_key_id
        self.private_key = private_key
        self.client_email = client_email
        self.client_id = client_id
        self.auth_uri = auth_uri
        self.token_uri = token_uri
        self.auth_provider_x509_cert_url = auth_provider_x509_cert_url
        self.client_x509_cert_url = client_x509_cert_url
        # Starting from here, the info is not included in the key file
        # and is optional. (Some of the info above is also optional).
        self.subject = subject
        self.scopes = scopes
        self.additional_claims = additional_claims or {}
        self.universe_domain = universe_domain
