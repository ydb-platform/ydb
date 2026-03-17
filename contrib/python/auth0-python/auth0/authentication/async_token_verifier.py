"""Token Verifier module"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .. import TokenValidationError
from ..rest_async import AsyncRestClient
from .token_verifier import AsymmetricSignatureVerifier, JwksFetcher, TokenVerifier

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey


class AsyncAsymmetricSignatureVerifier(AsymmetricSignatureVerifier):
    """Async verifier for RSA signatures, which rely on public key certificates.

    Args:
        jwks_url (str): The url where the JWK set is located.
        algorithm (str, optional): The expected signing algorithm. Defaults to "RS256".
    """

    def __init__(self, jwks_url: str, algorithm: str = "RS256") -> None:
        super().__init__(jwks_url, algorithm)
        self._fetcher = AsyncJwksFetcher(jwks_url)

    def set_session(self, session: ClientSession) -> None:
        """Set Client Session to improve performance by reusing session.

        Args:
            session (aiohttp.ClientSession): The client session which should be closed
                manually or within context manager.
        """
        self._fetcher.set_session(session)

    async def _fetch_key(self, key_id=None):
        """Request the JWKS.

        Args:
        key_id (str): The key's key id."""
        return await self._fetcher.get_key(key_id)

    async def verify_signature(self, token) -> dict[str, Any]:
        """Verifies the signature of the given JSON web token.

        Args:
            token (str): The JWT to get its signature verified.

        Raises:
            TokenValidationError: if the token cannot be decoded, the algorithm is invalid
            or the token's signature doesn't match the calculated one.
        """
        kid = self._get_kid(token)
        secret_or_certificate = await self._fetch_key(key_id=kid)

        return self._decode_jwt(token, secret_or_certificate)


class AsyncJwksFetcher(JwksFetcher):
    """Class that async fetches and holds a JSON web key set.
    This class makes use of an in-memory cache. For it to work properly, define this instance once and re-use it.

    Args:
        jwks_url (str): The url where the JWK set is located.
        cache_ttl (str, optional): The lifetime of the JWK set cache in seconds. Defaults to 600 seconds.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._async_client = AsyncRestClient(None)

    def set_session(self, session: ClientSession) -> None:
        """Set Client Session to improve performance by reusing session.

        Args:
            session (aiohttp.ClientSession): The client session which should be closed
                manually or within context manager.
        """
        self._async_client.set_session(session)

    async def _fetch_jwks(self, force: bool = False) -> dict[str, RSAPublicKey]:
        """Attempts to obtain the JWK set from the cache, as long as it's still valid.
        When not, it will perform a network request to the jwks_url to obtain a fresh result
        and update the cache value with it.

        Args:
            force (bool, optional): whether to ignore the cache and force a network request or not. Defaults to False.
        """
        if force or self._cache_expired():
            self._cache_value = {}
            try:
                jwks = await self._async_client.get(self._jwks_url)
                self._cache_jwks(jwks)
            except:  # noqa: E722
                return self._cache_value
            return self._cache_value

        self._cache_is_fresh = False
        return self._cache_value

    async def get_key(self, key_id: str) -> RSAPublicKey:
        """Obtains the JWK associated with the given key id.

        Args:
            key_id (str): The id of the key to fetch.

        Returns:
            the JWK associated with the given key id.

        Raises:
            TokenValidationError: when a key with that id cannot be found
        """
        keys = await self._fetch_jwks()

        if keys and key_id in keys:
            return keys[key_id]

        if not self._cache_is_fresh:
            keys = await self._fetch_jwks(force=True)
            if keys and key_id in keys:
                return keys[key_id]
        raise TokenValidationError(f'RSA Public Key with ID "{key_id}" was not found.')


class AsyncTokenVerifier(TokenVerifier):
    """Class that verifies ID tokens following the steps defined in the OpenID Connect spec.
    An OpenID Connect ID token is not meant to be consumed until it's verified.

    Args:
        signature_verifier (AsyncAsymmetricSignatureVerifier): The instance that knows how to verify the signature.
        issuer (str): The expected issuer claim value.
        audience (str): The expected audience claim value.
        leeway (int, optional): The clock skew to accept when verifying date related claims in seconds.
        Defaults to 60 seconds.
    """

    def __init__(
        self,
        signature_verifier: AsyncAsymmetricSignatureVerifier,
        issuer: str,
        audience: str,
        leeway: int = 0,
    ) -> None:
        if not signature_verifier or not isinstance(
            signature_verifier, AsyncAsymmetricSignatureVerifier
        ):
            raise TypeError(
                "signature_verifier must be an instance of AsyncAsymmetricSignatureVerifier."
            )

        self.iss = issuer
        self.aud = audience
        self.leeway = leeway
        self._sv = signature_verifier
        self._clock = None  # legacy testing requirement

    def set_session(self, session: ClientSession) -> None:
        """Set Client Session to improve performance by reusing session.

        Args:
            session (aiohttp.ClientSession): The client session which should be closed
                manually or within context manager.
        """
        self._sv.set_session(session)

    async def verify(
        self,
        token: str,
        nonce: str | None = None,
        max_age: int | None = None,
        organization: str | None = None,
    ) -> dict[str, Any]:
        """Attempts to verify the given ID token, following the steps defined in the OpenID Connect spec.

        Args:
            token (str): The JWT to verify.
            nonce (str, optional): The nonce value sent during authentication.
            max_age (int, optional): The max_age value sent during authentication.
            organization (str, optional): The expected organization ID (org_id) or organization name (org_name) claim value. This should be specified
            when logging in to an organization.

        Returns:
            the decoded payload from the token

        Raises:
            TokenValidationError: when the token cannot be decoded, the token signing algorithm is not the expected one,
            the token signature is invalid or the token has a claim missing or with unexpected value.
        """

        # Verify token presence
        if not token or not isinstance(token, str):
            raise TokenValidationError("ID token is required but missing.")

        # Verify algorithm and signature
        payload = await self._sv.verify_signature(token)

        # Verify claims
        self._verify_payload(payload, nonce, max_age, organization)

        return payload
