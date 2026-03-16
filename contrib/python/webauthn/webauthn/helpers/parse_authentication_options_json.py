import json
from json import JSONDecodeError
from typing import List, Optional, Union

from .base64url_to_bytes import base64url_to_bytes
from .exceptions import InvalidJSONStructure, InvalidAuthenticationOptions
from .structs import (
    AuthenticatorTransport,
    PublicKeyCredentialDescriptor,
    PublicKeyCredentialRequestOptions,
    UserVerificationRequirement,
)


def parse_authentication_options_json(
    json_val: Union[str, dict]
) -> PublicKeyCredentialRequestOptions:
    """
    Parse a JSON form of authentication options, as either stringified JSON or a plain dict, into an
    instance of `PublicKeyCredentialRequestOptions`. Typically useful in mapping output from
    `generate_authentication_options()`, that's been persisted as JSON via Redis/etc... back into
    structured data.
    """
    if isinstance(json_val, str):
        try:
            json_val = json.loads(json_val)
        except JSONDecodeError:
            raise InvalidJSONStructure("Unable to decode options as JSON")

    if not isinstance(json_val, dict):
        raise InvalidJSONStructure("Options were not a JSON object")

    """
    Check challenge
    """
    options_challenge = json_val.get("challenge")
    if not isinstance(options_challenge, str):
        raise InvalidJSONStructure("Options missing required challenge")

    """
    Check timeout
    """
    options_timeout = json_val.get("timeout")
    mapped_timeout = None
    if isinstance(options_timeout, int):
        mapped_timeout = options_timeout

    """
    Check rpId
    """
    options_rp_id = json_val.get("rpId")
    mapped_rp_id = None
    if isinstance(options_rp_id, str):
        mapped_rp_id = options_rp_id

    """
    Check userVerification
    """
    options_user_verification = json_val.get("userVerification")
    if not isinstance(options_user_verification, str):
        raise InvalidJSONStructure("Options missing required userVerification")

    try:
        mapped_user_verification = UserVerificationRequirement(options_user_verification)
    except ValueError as exc:
        raise InvalidJSONStructure("Options userVerification was invalid value") from exc

    """
    Check allowCredentials
    """
    options_allow_credentials = json_val.get("allowCredentials")
    mapped_allow_credentials: Optional[List[PublicKeyCredentialDescriptor]] = None
    if isinstance(options_allow_credentials, list):
        mapped_allow_credentials = []
        for cred in options_allow_credentials:
            _cred_id = cred.get("id")
            if not isinstance(_cred_id, str):
                raise InvalidJSONStructure("Options excludeCredentials entry missing required id")

            _mapped = PublicKeyCredentialDescriptor(id=base64url_to_bytes(_cred_id))

            _transports = cred.get("transports")
            if _transports is not None:
                if not isinstance(_transports, list):
                    raise InvalidJSONStructure(
                        "Options excludeCredentials entry transports was not list"
                    )
                try:
                    _mapped.transports = [
                        AuthenticatorTransport(_transport) for _transport in _transports
                    ]
                except ValueError as exc:
                    raise InvalidJSONStructure(
                        "Options excludeCredentials entry transports had invalid value"
                    ) from exc

            mapped_allow_credentials.append(_mapped)

    try:
        authentication_options = PublicKeyCredentialRequestOptions(
            challenge=base64url_to_bytes(options_challenge),
            timeout=mapped_timeout,
            rp_id=mapped_rp_id,
            user_verification=mapped_user_verification,
            allow_credentials=mapped_allow_credentials,
        )
    except Exception as exc:
        raise InvalidAuthenticationOptions(
            "Could not parse authentication options from JSON data"
        ) from exc

    return authentication_options
