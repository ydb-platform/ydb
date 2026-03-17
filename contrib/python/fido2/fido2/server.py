# Copyright (c) 2018 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import annotations

from .rpid import verify_rp_id
from .cose import CoseKey
from .utils import websafe_encode, websafe_decode
from .webauthn import (
    CollectedClientData,
    AuthenticatorData,
    AttestationObject,
    AttestedCredentialData,
    AttestationConveyancePreference,
    PublicKeyCredentialRpEntity,
    PublicKeyCredentialUserEntity,
    AuthenticatorSelectionCriteria,
    PublicKeyCredentialDescriptor,
    PublicKeyCredentialType,
    PublicKeyCredentialParameters,
    PublicKeyCredentialCreationOptions,
    PublicKeyCredentialRequestOptions,
    UserVerificationRequirement,
    ResidentKeyRequirement,
    AuthenticatorAttachment,
    RegistrationResponse,
    AuthenticationResponse,
    CredentialCreationOptions,
    CredentialRequestOptions,
)

from cryptography.hazmat.primitives import constant_time
from cryptography.exceptions import InvalidSignature as _InvalidSignature
from dataclasses import replace
from urllib.parse import urlparse
from typing import Sequence, Mapping, Optional, Callable, Union, Tuple, Any, overload

import os
import logging

logger = logging.getLogger(__name__)


VerifyAttestation = Callable[[AttestationObject, bytes], None]
VerifyOrigin = Callable[[str], bool]


def _verify_origin_for_rp(rp_id: str) -> VerifyOrigin:
    return lambda o: verify_rp_id(rp_id, o)


def _validata_challenge(challenge: Optional[bytes]) -> bytes:
    if challenge is None:
        challenge = os.urandom(32)
    else:
        if not isinstance(challenge, bytes):
            raise TypeError("Custom challenge must be of type 'bytes'.")
        if len(challenge) < 16:
            raise ValueError("Custom challenge length must be >= 16.")
    return challenge


def to_descriptor(
    credential: AttestedCredentialData, transports=None
) -> PublicKeyCredentialDescriptor:
    """Converts an AttestedCredentialData to a PublicKeyCredentialDescriptor.

    :param credential: AttestedCredentialData containing the credential ID to use.
    :param transports: Optional list of AuthenticatorTransport strings to add to the
        descriptor.
    :return: A descriptor of the credential, for use with register_begin or
        authenticate_begin.
    :rtype: PublicKeyCredentialDescriptor
    """
    return PublicKeyCredentialDescriptor(
        PublicKeyCredentialType.PUBLIC_KEY, credential.credential_id, transports
    )


def _wrap_credentials(
    creds: Optional[
        Sequence[Union[AttestedCredentialData, PublicKeyCredentialDescriptor]]
    ],
) -> Optional[Sequence[PublicKeyCredentialDescriptor]]:
    if creds is None:
        return None
    return [
        (
            to_descriptor(c)
            if isinstance(c, AttestedCredentialData)
            else PublicKeyCredentialDescriptor.from_dict(c)
        )
        for c in creds
    ]


def _ignore_attestation(
    attestation_object: AttestationObject, client_data_hash: bytes
) -> None:
    """Ignore attestation."""


class Fido2Server:
    """FIDO2 server.

    :param rp: Relying party data as `PublicKeyCredentialRpEntity` instance.
    :param attestation: (optional) Requirement on authenticator attestation.
    :param verify_origin: (optional) Alternative function to validate an origin.
    :param verify_attestation: (optional) function to validate attestation, which is
        invoked with attestation_object and client_data_hash. It should return nothing
        and raise an exception on failure. By default, attestation is ignored.
        Attestation is also ignored if `attestation` is set to `none`.
    """

    def __init__(
        self,
        rp: PublicKeyCredentialRpEntity,
        attestation: Optional[AttestationConveyancePreference] = None,
        verify_origin: Optional[VerifyOrigin] = None,
        verify_attestation: Optional[VerifyAttestation] = None,
    ):
        self.rp = PublicKeyCredentialRpEntity.from_dict(rp)
        self._verify = verify_origin or _verify_origin_for_rp(self.rp.id)
        self.timeout = None
        self.attestation = AttestationConveyancePreference(attestation)
        self.allowed_algorithms = [
            PublicKeyCredentialParameters(PublicKeyCredentialType.PUBLIC_KEY, alg)
            for alg in CoseKey.supported_algorithms()
        ]
        self._verify_attestation = verify_attestation or _ignore_attestation
        logger.debug(f"Fido2Server initialized for RP: {self.rp}")

    def register_begin(
        self,
        user: PublicKeyCredentialUserEntity,
        credentials: Optional[
            Sequence[Union[AttestedCredentialData, PublicKeyCredentialDescriptor]]
        ] = None,
        resident_key_requirement: Optional[ResidentKeyRequirement] = None,
        user_verification: Optional[UserVerificationRequirement] = None,
        authenticator_attachment: Optional[AuthenticatorAttachment] = None,
        challenge: Optional[bytes] = None,
        extensions=None,
    ) -> Tuple[CredentialCreationOptions, Any]:
        """Return a PublicKeyCredentialCreationOptions registration object and
        the internal state dictionary that needs to be passed as is to the
        corresponding `register_complete` call.

        :param user: The dict containing the user data.
        :param credentials: The list of previously registered credentials, these can be
            of type AttestedCredentialData, or PublicKeyCredentialDescriptor.
        :param resident_key_requirement: The desired RESIDENT_KEY_REQUIREMENT level.
        :param user_verification: The desired USER_VERIFICATION level.
        :param authenticator_attachment: The desired AUTHENTICATOR_ATTACHMENT
            or None to not provide a preference (and get both types).
        :param challenge: A custom challenge to sign and verify or None to use
            OS-specific random bytes.
        :return: Registration data, internal state."""
        if not self.allowed_algorithms:
            raise ValueError("Server has no allowed algorithms.")

        challenge = _validata_challenge(challenge)
        descriptors = _wrap_credentials(credentials)
        state = self._make_internal_state(challenge, user_verification)
        logger.debug(
            "Starting new registration, existing credentials: "
            + ", ".join(d.id.hex() for d in descriptors or [])
        )

        return (
            CredentialCreationOptions(
                PublicKeyCredentialCreationOptions(
                    self.rp,
                    user,
                    challenge,
                    self.allowed_algorithms,
                    self.timeout,
                    descriptors,
                    (
                        AuthenticatorSelectionCriteria(
                            authenticator_attachment,
                            resident_key_requirement,
                            user_verification,
                        )
                        if any(
                            (
                                authenticator_attachment,
                                resident_key_requirement,
                                user_verification,
                            )
                        )
                        else None
                    ),
                    self.attestation,
                    extensions,
                )
            ),
            state,
        )

    @overload
    def register_complete(
        self,
        state,
        response: Union[RegistrationResponse, Mapping[str, Any]],
    ) -> AuthenticatorData:
        pass

    @overload
    def register_complete(
        self,
        state,
        client_data: CollectedClientData,
        attestation_object: AttestationObject,
    ) -> AuthenticatorData:
        pass

    def register_complete(self, state, *args, **kwargs):
        """Verify the correctness of the registration data received from
        the client.

        :param state: The state data returned by the corresponding
            `register_begin`.
        :param client_data: The client data.
        :param attestation_object: The attestation object.
        :return: The authenticator data
        """
        response = None
        if len(args) == 1 and not kwargs:
            response = args[0]
        elif set(kwargs) == {"response"} and not args:
            response = kwargs["response"]
        if response:
            registration = RegistrationResponse.from_dict(response)
            client_data = registration.response.client_data
            attestation_object = registration.response.attestation_object
        else:
            names = ["client_data", "attestation_object"]
            pos = dict(zip(names, args))
            data = {**kwargs, **pos}
            if set(kwargs) & set(pos) or set(data) != set(names):
                raise TypeError("incorrect arguments passed to register_complete()")
            client_data = data[names[0]]
            attestation_object = data[names[1]]

        if client_data.type != CollectedClientData.TYPE.CREATE:
            raise ValueError("Incorrect type in CollectedClientData.")
        if not self._verify(client_data.origin):
            raise ValueError("Invalid origin in CollectedClientData.")
        if not constant_time.bytes_eq(
            websafe_decode(state["challenge"]), client_data.challenge
        ):
            raise ValueError("Wrong challenge in response.")
        if not constant_time.bytes_eq(
            self.rp.id_hash, attestation_object.auth_data.rp_id_hash
        ):
            raise ValueError("Wrong RP ID hash in response.")
        if not attestation_object.auth_data.is_user_present():
            raise ValueError("User Present flag not set.")

        if (
            state["user_verification"] == UserVerificationRequirement.REQUIRED
            and not attestation_object.auth_data.is_user_verified()
        ):
            raise ValueError(
                "User verification required, but User Verified flag not set."
            )

        if self.attestation not in (None, AttestationConveyancePreference.NONE):
            logger.debug(f"Verifying attestation of type {attestation_object.fmt}")
            self._verify_attestation(attestation_object, client_data.hash)
        # We simply ignore attestation if self.attestation == 'none', as not all
        # clients strip the attestation.

        auth_data = attestation_object.auth_data
        assert auth_data.credential_data is not None  # nosec
        logger.info(
            "New credential registered: "
            + auth_data.credential_data.credential_id.hex()
        )
        return auth_data

    def authenticate_begin(
        self,
        credentials: Optional[
            Sequence[Union[AttestedCredentialData, PublicKeyCredentialDescriptor]]
        ] = None,
        user_verification: Optional[UserVerificationRequirement] = None,
        challenge: Optional[bytes] = None,
        extensions=None,
    ) -> Tuple[CredentialRequestOptions, Any]:
        """Return a PublicKeyCredentialRequestOptions assertion object and the internal
        state dictionary that needs to be passed as is to the corresponding
        `authenticate_complete` call.

        :param credentials: The list of previously registered credentials, these can be
            of type AttestedCredentialData, or PublicKeyCredentialDescriptor.
        :param user_verification: The desired USER_VERIFICATION level.
        :param challenge: A custom challenge to sign and verify or None to use
            OS-specific random bytes.
        :return: Assertion data, internal state."""
        challenge = _validata_challenge(challenge)
        descriptors = _wrap_credentials(credentials)
        state = self._make_internal_state(challenge, user_verification)
        if descriptors is None:
            logger.debug("Starting new authentication without credentials")
        else:
            logger.debug(
                "Starting new authentication, for credentials: "
                + ", ".join(d.id.hex() for d in descriptors)
            )

        return (
            CredentialRequestOptions(
                PublicKeyCredentialRequestOptions(
                    challenge,
                    self.timeout,
                    self.rp.id,
                    descriptors,
                    user_verification,
                    extensions,
                )
            ),
            state,
        )

    @overload
    def authenticate_complete(
        self,
        state,
        credentials: Sequence[AttestedCredentialData],
        response: Union[AuthenticationResponse, Mapping[str, Any]],
    ) -> AttestedCredentialData:
        pass

    @overload
    def authenticate_complete(
        self,
        state,
        credentials: Sequence[AttestedCredentialData],
        credential_id: bytes,
        client_data: CollectedClientData,
        auth_data: AuthenticatorData,
        signature: bytes,
    ) -> AttestedCredentialData:
        pass

    def authenticate_complete(self, state, credentials, *args, **kwargs):
        """Verify the correctness of the assertion data received from
        the client.

        :param state: The state data returned by the corresponding
            `register_begin`.
        :param credentials: The list of previously registered credentials.
        :param credential_id: The credential id from the client response.
        :param client_data: The client data.
        :param auth_data: The authenticator data.
        :param signature: The signature provided by the client."""

        response = None
        if len(args) == 1 and not kwargs:
            response = args[0]
        elif set(kwargs) == {"response"} and not args:
            response = kwargs["response"]
        if response:
            authentication = AuthenticationResponse.from_dict(response)
            credential_id = authentication.id
            client_data = authentication.response.client_data
            auth_data = authentication.response.authenticator_data
            signature = authentication.response.signature
        else:
            names = ["credential_id", "client_data", "auth_data", "signature"]
            pos = dict(zip(names, args))
            data = {**kwargs, **pos}
            if set(kwargs) & set(pos) or set(data) != set(names):
                raise TypeError("incorrect arguments passed to authenticate_complete()")
            credential_id = data[names[0]]
            client_data = data[names[1]]
            auth_data = data[names[2]]
            signature = data[names[3]]

        if client_data.type != CollectedClientData.TYPE.GET:
            raise ValueError("Incorrect type in CollectedClientData.")
        if not self._verify(client_data.origin):
            raise ValueError("Invalid origin in CollectedClientData.")
        if websafe_decode(state["challenge"]) != client_data.challenge:
            raise ValueError("Wrong challenge in response.")
        if not constant_time.bytes_eq(self.rp.id_hash, auth_data.rp_id_hash):
            raise ValueError("Wrong RP ID hash in response.")
        if not auth_data.is_user_present():
            raise ValueError("User Present flag not set.")

        if (
            state["user_verification"] == UserVerificationRequirement.REQUIRED
            and not auth_data.is_user_verified()
        ):
            raise ValueError(
                "User verification required, but user verified flag not set."
            )

        for cred in credentials:
            if cred.credential_id == credential_id:
                try:
                    cred.public_key.verify(auth_data + client_data.hash, signature)
                except _InvalidSignature:
                    raise ValueError("Invalid signature.")
                logger.info(f"Credential authenticated: {credential_id.hex()}")
                return cred
        raise ValueError("Unknown credential ID.")

    @staticmethod
    def _make_internal_state(
        challenge: bytes, user_verification: Optional[UserVerificationRequirement]
    ):
        return {
            "challenge": websafe_encode(challenge),
            "user_verification": user_verification,
        }


def verify_app_id(app_id: str, origin: str) -> bool:
    """Checks if a FIDO U2F App ID is usable for a given origin.

    :param app_id: The App ID to validate.
    :param origin: The origin of the request.
    :return: True if the App ID is usable by the origin, False if not.
    """
    url = urlparse(app_id)
    if url.scheme != "https":
        return False
    hostname = url.hostname
    if not hostname:
        return False
    return verify_rp_id(hostname, origin)


class U2FFido2Server(Fido2Server):
    """Fido2Server which can be used with existing U2F credentials.

    This Fido2Server can be used with existing U2F credentials by using the
    WebAuthn appid extension, as well as with new WebAuthn credentials.
    See https://www.w3.org/TR/webauthn/#sctn-appid-extension for details.

    :param app_id: The appId which was used for U2F registration.
    :param verify_u2f_origin: (optional) Alternative function to validate an
        origin for U2F credentials.
    For other parameters, see Fido2Server.
    """

    def __init__(
        self,
        app_id: str,
        rp: PublicKeyCredentialRpEntity,
        verify_u2f_origin: Optional[VerifyOrigin] = None,
        *args,
        **kwargs,
    ):
        super().__init__(rp, *args, **kwargs)
        if verify_u2f_origin:
            kwargs["verify_origin"] = verify_u2f_origin
        else:
            kwargs["verify_origin"] = lambda o: verify_app_id(app_id, o)
        self._app_id = app_id
        self._app_id_server = Fido2Server(
            replace(PublicKeyCredentialRpEntity.from_dict(rp), id=app_id),
            *args,
            **kwargs,
        )

    def register_begin(self, *args, **kwargs):
        kwargs.setdefault("extensions", {})["appidExclude"] = self._app_id
        req, state = super().register_begin(*args, **kwargs)
        return req, state

    def authenticate_begin(self, *args, **kwargs):
        kwargs.setdefault("extensions", {})["appid"] = self._app_id
        req, state = super().authenticate_begin(*args, **kwargs)
        return req, state

    def authenticate_complete(self, *args, **kwargs):
        try:
            return super().authenticate_complete(*args, **kwargs)
        except ValueError:
            return self._app_id_server.authenticate_complete(*args, **kwargs)
