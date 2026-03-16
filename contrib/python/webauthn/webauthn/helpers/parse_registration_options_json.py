import json
from json.decoder import JSONDecodeError
from typing import Union, Optional, List


from .structs import (
    PublicKeyCredentialCreationOptions,
    PublicKeyCredentialHint,
    PublicKeyCredentialRpEntity,
    PublicKeyCredentialUserEntity,
    AttestationConveyancePreference,
    AuthenticatorSelectionCriteria,
    AuthenticatorAttachment,
    ResidentKeyRequirement,
    UserVerificationRequirement,
    PublicKeyCredentialParameters,
    PublicKeyCredentialDescriptor,
    AuthenticatorTransport,
)
from .cose import COSEAlgorithmIdentifier
from .exceptions import InvalidJSONStructure, InvalidRegistrationOptions
from .base64url_to_bytes import base64url_to_bytes


def parse_registration_options_json(
    json_val: Union[str, dict]
) -> PublicKeyCredentialCreationOptions:
    """
    Parse a JSON form of registration options, as either stringified JSON or a plain dict, into an
    instance of `PublicKeyCredentialCreationOptions`. Typically useful in mapping output from
    `generate_registration_options()`, that's been persisted as JSON via Redis/etc... back into
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
    Check rp
    """
    options_rp = json_val.get("rp")
    if not isinstance(options_rp, dict):
        raise InvalidJSONStructure("Options missing required rp")

    options_rp_id = options_rp.get("id")
    if options_rp_id is not None and not isinstance(options_rp_id, str):
        raise InvalidJSONStructure("Options rp.id present but not string")

    options_rp_name = options_rp.get("name")
    if not isinstance(options_rp_name, str):
        raise InvalidJSONStructure("Options rp missing required name")

    """
    Check user
    """
    options_user = json_val.get("user")
    if not isinstance(options_user, dict):
        raise InvalidJSONStructure("Options missing required user")

    options_user_id = options_user.get("id")
    if not isinstance(options_user_id, str):
        raise InvalidJSONStructure("Options user missing required id")

    options_user_name = options_user.get("name")
    if not isinstance(options_user_name, str):
        raise InvalidJSONStructure("Options user missing required name")

    options_user_display_name = options_user.get("displayName")
    if not isinstance(options_user_display_name, str):
        raise InvalidJSONStructure("Options user missing required displayName")

    """
    Check attestation
    """
    options_attestation = json_val.get("attestation")
    if not isinstance(options_attestation, str):
        raise InvalidJSONStructure("Options missing required attestation")

    try:
        mapped_attestation = AttestationConveyancePreference(options_attestation)
    except ValueError as exc:
        raise InvalidJSONStructure("Options attestation was invalid value") from exc

    """
    Check authenticatorSelection
    """
    options_authr_selection = json_val.get("authenticatorSelection")
    mapped_authenticator_selection: Optional[AuthenticatorSelectionCriteria] = None
    if isinstance(options_authr_selection, dict):
        options_authr_selection_attachment = options_authr_selection.get("authenticatorAttachment")
        mapped_attachment = None
        if options_authr_selection_attachment is not None:
            try:
                mapped_attachment = AuthenticatorAttachment(options_authr_selection_attachment)
            except ValueError as exc:
                raise InvalidJSONStructure(
                    "Options authenticatorSelection attachment was invalid value"
                ) from exc

        options_authr_selection_rkey = options_authr_selection.get("residentKey")
        mapped_rkey = None
        if options_authr_selection_rkey is not None:
            try:
                mapped_rkey = ResidentKeyRequirement(options_authr_selection_rkey)
            except ValueError as exc:
                raise InvalidJSONStructure(
                    "Options authenticatorSelection residentKey was invalid value"
                ) from exc

        options_authr_selection_require_rkey = options_authr_selection.get("requireResidentKey")
        mapped_require_rkey = False
        if options_authr_selection_require_rkey is not None:
            if not isinstance(options_authr_selection_require_rkey, bool):
                raise InvalidJSONStructure(
                    "Options authenticatorSelection requireResidentKey was invalid boolean"
                )

            mapped_require_rkey = options_authr_selection_require_rkey

        options_authr_selection_uv = options_authr_selection.get("userVerification")
        mapped_user_verification = UserVerificationRequirement.PREFERRED
        if options_authr_selection_uv is not None:
            try:
                mapped_user_verification = UserVerificationRequirement(options_authr_selection_uv)
            except ValueError as exc:
                raise InvalidJSONStructure(
                    "Options authenticatorSelection userVerification was invalid value"
                ) from exc

        mapped_authenticator_selection = AuthenticatorSelectionCriteria(
            authenticator_attachment=mapped_attachment,
            resident_key=mapped_rkey,
            require_resident_key=mapped_require_rkey,
            user_verification=mapped_user_verification,
        )

    """
    Check challenge is present
    """
    options_challenge = json_val.get("challenge")
    if not isinstance(options_challenge, str):
        raise InvalidJSONStructure("Options missing required challenge")

    """
    Check pubKeyCredParams
    """
    options_pub_key_cred_params = json_val.get("pubKeyCredParams")
    if not isinstance(options_pub_key_cred_params, list):
        raise InvalidJSONStructure("Options pubKeyCredParams was invalid value")

    try:
        mapped_pub_key_cred_params = [
            PublicKeyCredentialParameters(
                alg=COSEAlgorithmIdentifier(param["alg"]), type="public-key"
            )
            for param in options_pub_key_cred_params
        ]
    except ValueError as exc:
        raise InvalidJSONStructure("Options pubKeyCredParams entry had invalid alg") from exc

    """
    Check excludeCredentials
    """
    options_exclude_credentials = json_val.get("excludeCredentials")
    mapped_exclude_credentials: Optional[List[PublicKeyCredentialDescriptor]] = None
    if isinstance(options_exclude_credentials, list):
        mapped_exclude_credentials = []
        for cred in options_exclude_credentials:
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

            mapped_exclude_credentials.append(_mapped)

    """
    Check timeout
    """
    options_timeout = json_val.get("timeout")
    mapped_timeout = None
    if isinstance(options_timeout, int):
        mapped_timeout = options_timeout

    """
    Check hints
    """
    options_hints = json_val.get("hints")
    mapped_hints = None
    if options_hints is not None:
        if not isinstance(options_hints, list):
            raise InvalidJSONStructure("Options hints was invalid value")

        try:
            mapped_hints = [PublicKeyCredentialHint(hint) for hint in options_hints]
        except ValueError as exc:
            raise InvalidJSONStructure("Options hints had invalid value") from exc

    try:
        registration_options = PublicKeyCredentialCreationOptions(
            rp=PublicKeyCredentialRpEntity(
                id=options_rp_id,
                name=options_rp_name,
            ),
            user=PublicKeyCredentialUserEntity(
                id=base64url_to_bytes(options_user_id),
                name=options_user_name,
                display_name=options_user_display_name,
            ),
            attestation=mapped_attestation,
            authenticator_selection=mapped_authenticator_selection,
            challenge=base64url_to_bytes(options_challenge),
            pub_key_cred_params=mapped_pub_key_cred_params,
            exclude_credentials=mapped_exclude_credentials,
            timeout=mapped_timeout,
            hints=mapped_hints,
        )
    except Exception as exc:
        raise InvalidRegistrationOptions(
            "Could not parse registration options from JSON data"
        ) from exc

    return registration_options
