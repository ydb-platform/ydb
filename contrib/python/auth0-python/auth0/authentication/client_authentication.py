from __future__ import annotations

import datetime
import uuid
from typing import Any

import jwt


def create_client_assertion_jwt(
    domain: str,
    client_id: str,
    client_assertion_signing_key: str,
    client_assertion_signing_alg: str | None,
) -> str:
    """Creates a JWT for the client_assertion field.

    Args:
        domain (str): The domain of your Auth0 tenant
        client_id (str): Your application's client ID
        client_assertion_signing_key (str): Private key used to sign the client assertion JWT
        client_assertion_signing_alg (str, optional): Algorithm used to sign the client assertion JWT (defaults to 'RS256')

    Returns:
        A JWT signed with the `client_assertion_signing_key`.
    """
    client_assertion_signing_alg = client_assertion_signing_alg or "RS256"
    now = datetime.datetime.utcnow()
    return jwt.encode(
        {
            "iss": client_id,
            "sub": client_id,
            "aud": f"https://{domain}/",
            "iat": now,
            "exp": now + datetime.timedelta(seconds=180),
            "jti": str(uuid.uuid4()),
        },
        client_assertion_signing_key,
        client_assertion_signing_alg,
    )


def add_client_authentication(
    payload: dict[str, Any],
    domain: str,
    client_id: str,
    client_secret: str | None,
    client_assertion_signing_key: str | None,
    client_assertion_signing_alg: str | None,
) -> dict[str, Any]:
    """Adds the client_assertion or client_secret fields to authenticate a payload.

    Args:
        payload (dict): The POST payload that needs additional fields to be authenticated.
        domain (str): The domain of your Auth0 tenant
        client_id (str): Your application's client ID
        client_secret (str, optional): Your application's client secret
        client_assertion_signing_key (str, optional): Private key used to sign the client assertion JWT
        client_assertion_signing_alg (str, optional): Algorithm used to sign the client assertion JWT (defaults to 'RS256')

    Returns:
        A copy of the payload with client authentication fields added.
    """

    authenticated_payload = payload.copy()
    if client_assertion_signing_key:
        authenticated_payload["client_assertion"] = create_client_assertion_jwt(
            domain,
            client_id,
            client_assertion_signing_key,
            client_assertion_signing_alg,
        )
        authenticated_payload[
            "client_assertion_type"
        ] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
    elif client_secret:
        authenticated_payload["client_secret"] = client_secret
    return authenticated_payload
