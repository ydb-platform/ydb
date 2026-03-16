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

from .hid import STATUS
from .ctap import CtapDevice, CtapError
from .ctap1 import Ctap1, APDU, ApduError
from .ctap2 import Ctap2, AssertionResponse, Info
from .ctap2.pin import ClientPin, PinProtocol
from .ctap2.extensions import Ctap2Extension
from .webauthn import (
    Aaguid,
    AttestationObject,
    CollectedClientData,
    PublicKeyCredentialCreationOptions,
    PublicKeyCredentialRequestOptions,
    AuthenticatorSelectionCriteria,
    UserVerificationRequirement,
    AuthenticatorAttestationResponse,
    AuthenticatorAssertionResponse,
    AttestationConveyancePreference,
)
from .cose import ES256
from .rpid import verify_rp_id
from .utils import sha256
from enum import IntEnum, unique
from urllib.parse import urlparse
from dataclasses import replace
from threading import Timer, Event
from typing import (
    Type,
    Any,
    Callable,
    Optional,
    Mapping,
    Sequence,
)

import abc
import platform
import inspect
import logging

logger = logging.getLogger(__name__)


class ClientError(Exception):
    @unique
    class ERR(IntEnum):
        OTHER_ERROR = 1
        BAD_REQUEST = 2
        CONFIGURATION_UNSUPPORTED = 3
        DEVICE_INELIGIBLE = 4
        TIMEOUT = 5

        def __call__(self, cause=None):
            return ClientError(self, cause)

    def __init__(self, code, cause=None):
        self.code = ClientError.ERR(code)
        self.cause = cause

    def __repr__(self):
        r = "Client error: {0} - {0.name}".format(self.code)
        if self.cause:
            r += f" (cause: {self.cause})"
        return r


def _ctap2client_err(e, err_cls=ClientError):
    if e.code in [CtapError.ERR.CREDENTIAL_EXCLUDED, CtapError.ERR.NO_CREDENTIALS]:
        ce = ClientError.ERR.DEVICE_INELIGIBLE
    elif e.code in [
        CtapError.ERR.KEEPALIVE_CANCEL,
        CtapError.ERR.ACTION_TIMEOUT,
        CtapError.ERR.USER_ACTION_TIMEOUT,
    ]:
        ce = ClientError.ERR.TIMEOUT
    elif e.code in [
        CtapError.ERR.UNSUPPORTED_ALGORITHM,
        CtapError.ERR.UNSUPPORTED_OPTION,
        CtapError.ERR.KEY_STORE_FULL,
    ]:
        ce = ClientError.ERR.CONFIGURATION_UNSUPPORTED
    elif e.code in [
        CtapError.ERR.INVALID_COMMAND,
        CtapError.ERR.CBOR_UNEXPECTED_TYPE,
        CtapError.ERR.INVALID_CBOR,
        CtapError.ERR.MISSING_PARAMETER,
        CtapError.ERR.INVALID_OPTION,
        CtapError.ERR.PUAT_REQUIRED,
        CtapError.ERR.PIN_INVALID,
        CtapError.ERR.PIN_BLOCKED,
        CtapError.ERR.PIN_NOT_SET,
        CtapError.ERR.PIN_POLICY_VIOLATION,
        CtapError.ERR.PIN_TOKEN_EXPIRED,
        CtapError.ERR.PIN_AUTH_INVALID,
        CtapError.ERR.PIN_AUTH_BLOCKED,
        CtapError.ERR.REQUEST_TOO_LARGE,
        CtapError.ERR.OPERATION_DENIED,
    ]:
        ce = ClientError.ERR.BAD_REQUEST
    else:
        ce = ClientError.ERR.OTHER_ERROR

    return err_cls(ce, e)


class PinRequiredError(ClientError):
    def __init__(
        self, code=ClientError.ERR.BAD_REQUEST, cause="PIN required but not provided"
    ):
        super().__init__(code, cause)


def _call_polling(poll_delay, event, on_keepalive, func, *args, **kwargs):
    event = event or Event()
    while not event.is_set():
        try:
            return func(*args, **kwargs)
        except ApduError as e:
            if e.code == APDU.USE_NOT_SATISFIED:
                if on_keepalive:
                    on_keepalive(STATUS.UPNEEDED)
                    on_keepalive = None
                event.wait(poll_delay)
            else:
                raise ClientError.ERR.OTHER_ERROR(e)
        except CtapError as e:
            raise _ctap2client_err(e)
    raise ClientError.ERR.TIMEOUT()


class _BaseClient:
    def __init__(self, origin: str, verify: Callable[[str, str], bool]):
        self.origin = origin
        self._verify = verify

    def _verify_rp_id(self, rp_id):
        try:
            if self._verify(rp_id, self.origin):
                return
        except Exception:  # nosec
            pass  # Fall through to ClientError
        raise ClientError.ERR.BAD_REQUEST()

    def _build_client_data(self, typ, challenge):
        return CollectedClientData.create(
            type=typ,
            origin=self.origin,
            challenge=challenge,
        )


class AssertionSelection:
    """GetAssertion result holding one or more assertions.

    Since multiple assertions may be retured by Fido2Client.get_assertion, this result
    is returned which can be used to select a specific response to get.
    """

    def __init__(
        self, client_data: CollectedClientData, assertions: Sequence[AssertionResponse]
    ):
        self._client_data = client_data
        self._assertions = assertions

    def get_assertions(self) -> Sequence[AssertionResponse]:
        """Get the raw AssertionResponses available to inspect before selecting one."""
        return self._assertions

    def _get_extension_results(
        self, assertion: AssertionResponse
    ) -> Optional[Mapping[str, Any]]:
        return None  # Not implemented

    def get_response(self, index: int) -> AuthenticatorAssertionResponse:
        """Get a single response."""
        assertion = self._assertions[index]

        return AuthenticatorAssertionResponse(
            self._client_data,
            assertion.auth_data,
            assertion.signature,
            assertion.user["id"] if assertion.user else None,
            assertion.credential["id"] if assertion.credential else None,
            self._get_extension_results(assertion),
        )


class WebAuthnClient(abc.ABC):
    @abc.abstractmethod
    def make_credential(
        self,
        options: PublicKeyCredentialCreationOptions,
        event: Optional[Event] = None,
    ) -> AuthenticatorAttestationResponse:
        """Creates a credential.

        :param options: PublicKeyCredentialCreationOptions data.
        :param threading.Event event: (optional) Signal to abort the operation.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_assertion(
        self,
        options: PublicKeyCredentialRequestOptions,
        event: Optional[Event] = None,
    ) -> AssertionSelection:
        """Get an assertion.

        :param options: PublicKeyCredentialRequestOptions data.
        :param threading.Event event: (optional) Signal to abort the operation.
        """
        raise NotImplementedError()


def _default_extensions() -> Sequence[Type[Ctap2Extension]]:
    return [
        cls for cls in Ctap2Extension.__subclasses__() if not inspect.isabstract(cls)
    ]


class UserInteraction:
    """Provides user interaction to the Client.

    Users of Fido2Client should subclass this to implement asking the user to perform
    specific actions, such as entering a PIN or touching their"""

    def prompt_up(self) -> None:
        """Called when the authenticator is awaiting a user presence check."""
        logger.info("User Presence check required.")

    def request_pin(
        self, permissions: ClientPin.PERMISSION, rp_id: Optional[str]
    ) -> Optional[str]:
        """Called when the client requires a PIN from the user.

        Should return a PIN, or None/Empty to cancel."""
        logger.info("PIN requested, but UserInteraction does not support it.")
        return None

    def request_uv(
        self, permissions: ClientPin.PERMISSION, rp_id: Optional[str]
    ) -> bool:
        """Called when the client is about to request UV from the user.

        Should return True if allowed, or False to cancel."""
        logger.info("User Verification requested.")
        return True


def _user_keepalive(user_interaction):
    def on_keepalive(status):
        if status == STATUS.UPNEEDED:  # Waiting for touch
            user_interaction.prompt_up()

    return on_keepalive


class _ClientBackend(abc.ABC):
    info: Info

    @abc.abstractmethod
    def selection(self, event: Optional[Event]) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def do_make_credential(self, *args) -> AuthenticatorAttestationResponse:
        raise NotImplementedError()

    @abc.abstractmethod
    def do_get_assertion(self, *args) -> AssertionSelection:
        raise NotImplementedError()


class _Ctap1ClientBackend(_ClientBackend):
    def __init__(self, device: CtapDevice, user_interaction: UserInteraction):
        self.ctap1 = Ctap1(device)
        self.info = Info(versions=["U2F_V2"], extensions=[], aaguid=Aaguid.NONE)
        self._poll_delay = 0.25
        self._on_keepalive = _user_keepalive(user_interaction)

    def selection(self, event):
        _call_polling(
            self._poll_delay,
            event,
            None,
            self.ctap1.register,
            b"\0" * 32,
            b"\0" * 32,
        )

    def do_make_credential(
        self,
        client_data,
        rp,
        user,
        key_params,
        exclude_list,
        extensions,
        rk,
        user_verification,
        enterprise_attestation,
        event,
    ):
        if (
            rk
            or user_verification == UserVerificationRequirement.REQUIRED
            or ES256.ALGORITHM not in [p.alg for p in key_params]
            or enterprise_attestation
        ):
            raise CtapError(CtapError.ERR.UNSUPPORTED_OPTION)

        app_param = sha256(rp["id"].encode())

        dummy_param = b"\0" * 32
        for cred in exclude_list or []:
            key_handle = cred["id"]
            try:
                self.ctap1.authenticate(dummy_param, app_param, key_handle, True)
                raise ClientError.ERR.OTHER_ERROR()  # Shouldn't happen
            except ApduError as e:
                if e.code == APDU.USE_NOT_SATISFIED:
                    _call_polling(
                        self._poll_delay,
                        event,
                        self._on_keepalive,
                        self.ctap1.register,
                        dummy_param,
                        dummy_param,
                    )
                    raise ClientError.ERR.DEVICE_INELIGIBLE()

        att_obj = AttestationObject.from_ctap1(
            app_param,
            _call_polling(
                self._poll_delay,
                event,
                self._on_keepalive,
                self.ctap1.register,
                client_data.hash,
                app_param,
            ),
        )
        return AuthenticatorAttestationResponse(
            client_data,
            AttestationObject.create(att_obj.fmt, att_obj.auth_data, att_obj.att_stmt),
            {},
        )

    def do_get_assertion(
        self,
        client_data,
        rp_id,
        allow_list,
        extensions,
        user_verification,
        event,
    ):
        if user_verification == UserVerificationRequirement.REQUIRED or not allow_list:
            raise CtapError(CtapError.ERR.UNSUPPORTED_OPTION)

        app_param = sha256(rp_id.encode())
        client_param = client_data.hash
        for cred in allow_list:
            try:
                auth_resp = _call_polling(
                    self._poll_delay,
                    event,
                    self._on_keepalive,
                    self.ctap1.authenticate,
                    client_param,
                    app_param,
                    cred["id"],
                )
                assertions = [AssertionResponse.from_ctap1(app_param, cred, auth_resp)]
                return AssertionSelection(client_data, assertions)
            except ClientError as e:
                if e.code == ClientError.ERR.TIMEOUT:
                    raise  # Other errors are ignored so we move to the next.
        raise ClientError.ERR.DEVICE_INELIGIBLE()


class _Ctap2ClientAssertionSelection(AssertionSelection):
    def __init__(
        self,
        client_data: CollectedClientData,
        assertions: Sequence[AssertionResponse],
        extensions: Sequence[Ctap2Extension],
        pin_token: Optional[str],
        pin_protocol: Optional[PinProtocol],
    ):
        super().__init__(client_data, assertions)
        self._extensions = extensions
        self._pin_token = pin_token
        self._pin_protocol = pin_protocol

    def _get_extension_results(self, assertion):
        # Process extenstion outputs
        extension_outputs = {}
        try:
            for ext in self._extensions:
                output = ext.process_get_output(
                    assertion, self._pin_token, self._pin_protocol
                )
                if output is not None:
                    extension_outputs.update(output)
        except ValueError as e:
            raise ClientError.ERR.CONFIGURATION_UNSUPPORTED(e)
        return extension_outputs


class _Ctap2ClientBackend(_ClientBackend):
    def __init__(
        self,
        device: CtapDevice,
        user_interaction: UserInteraction,
        extensions: Sequence[Type[Ctap2Extension]],
    ):
        self.ctap2 = Ctap2(device)
        self.info = self.ctap2.info
        self.extensions = extensions
        self.user_interaction = user_interaction

    def selection(self, event):
        if "FIDO_2_1" in self.info.versions:
            self.ctap2.selection(event=event)
        else:
            # Selection not supported, make dummy credential instead
            try:
                self.ctap2.make_credential(
                    b"\0" * 32,
                    {"id": "example.com", "name": "example.com"},
                    {"id": b"dummy", "name": "dummy"},
                    [{"type": "public-key", "alg": -7}],
                    pin_uv_param=b"",
                    event=event,
                )
            except CtapError as e:
                if e.code in (
                    CtapError.ERR.PIN_NOT_SET,
                    CtapError.ERR.PIN_INVALID,
                    CtapError.ERR.PIN_AUTH_INVALID,
                ):
                    return
                raise

    def _should_use_uv(self, user_verification, mc):
        uv_supported = any(
            k in self.info.options for k in ("uv", "clientPin", "bioEnroll")
        )
        uv_configured = any(
            self.info.options.get(k) for k in ("uv", "clientPin", "bioEnroll")
        )

        if (
            user_verification == UserVerificationRequirement.REQUIRED
            or (
                user_verification == UserVerificationRequirement.PREFERRED
                and uv_supported
            )
            or self.info.options.get("alwaysUv")
        ):
            if not uv_configured:
                raise ClientError.ERR.CONFIGURATION_UNSUPPORTED(
                    "User verification not configured/supported"
                )
            return True
        elif mc and uv_configured and not self.info.options.get("makeCredUvNotRqd"):
            return True
        return False

    def _get_token(
        self, client_pin, permissions, rp_id, event, on_keepalive, allow_internal_uv
    ):
        # Prefer UV
        if self.info.options.get("uv"):
            if ClientPin.is_token_supported(self.info):
                if self.user_interaction.request_uv(permissions, rp_id):
                    return client_pin.get_uv_token(
                        permissions, rp_id, event, on_keepalive
                    )
            elif allow_internal_uv:
                if self.user_interaction.request_uv(permissions, rp_id):
                    return None  # No token, use uv=True

        # PIN if UV not supported/allowed.
        if self.info.options.get("clientPin"):
            pin = self.user_interaction.request_pin(permissions, rp_id)
            if pin:
                return client_pin.get_pin_token(pin, permissions, rp_id)
            raise PinRequiredError()

        # Client PIN not configured.
        raise ClientError.ERR.CONFIGURATION_UNSUPPORTED(
            "User verification not configured"
        )

    def _get_auth_params(
        self, client_data, rp_id, user_verification, permissions, event, on_keepalive
    ):
        mc = client_data.type == CollectedClientData.TYPE.CREATE
        self.info = self.ctap2.get_info()  # Make sure we have "fresh" info

        pin_protocol = None
        pin_token = None
        pin_auth = None
        internal_uv = False
        if self._should_use_uv(user_verification, mc) or permissions:
            client_pin = ClientPin(self.ctap2)
            allow_internal_uv = not permissions
            permissions |= (
                ClientPin.PERMISSION.MAKE_CREDENTIAL
                if mc
                else ClientPin.PERMISSION.GET_ASSERTION
            )
            pin_token = self._get_token(
                client_pin, permissions, rp_id, event, on_keepalive, allow_internal_uv
            )
            if pin_token:
                pin_protocol = client_pin.protocol
                pin_auth = client_pin.protocol.authenticate(pin_token, client_data.hash)
            else:
                internal_uv = True
        return pin_protocol, pin_token, pin_auth, internal_uv

    def do_make_credential(
        self,
        client_data,
        rp,
        user,
        key_params,
        exclude_list,
        extensions,
        rk,
        user_verification,
        enterprise_attestation,
        event,
    ):
        if exclude_list:
            # Filter out credential IDs which are too long
            max_len = self.info.max_cred_id_length
            if max_len:
                exclude_list = [e for e in exclude_list if len(e) <= max_len]

            # Reject the request if too many credentials remain.
            max_creds = self.info.max_creds_in_list
            if max_creds and len(exclude_list) > max_creds:
                raise ClientError.ERR.BAD_REQUEST("exclude_list too long")

        # Process extensions
        client_inputs = extensions or {}
        extension_inputs = {}
        used_extensions = []
        permissions = ClientPin.PERMISSION(0)
        try:
            for ext in [cls(self.ctap2) for cls in self.extensions]:
                auth_input, req_perms = ext.process_create_input_with_permissions(
                    client_inputs
                )
                if auth_input is not None:
                    used_extensions.append(ext)
                    permissions |= req_perms
                    extension_inputs[ext.NAME] = auth_input
        except ValueError as e:
            raise ClientError.ERR.CONFIGURATION_UNSUPPORTED(e)

        on_keepalive = _user_keepalive(self.user_interaction)

        # Handle auth
        pin_protocol, pin_token, pin_auth, internal_uv = self._get_auth_params(
            client_data, rp["id"], user_verification, permissions, event, on_keepalive
        )

        if not (rk or internal_uv):
            options = None
        else:
            options = {}
            if rk:
                options["rk"] = True
            if internal_uv:
                options["uv"] = True

        att_obj = self.ctap2.make_credential(
            client_data.hash,
            rp,
            user,
            key_params,
            exclude_list or None,
            extension_inputs or None,
            options,
            pin_auth,
            pin_protocol.VERSION if pin_protocol else None,
            enterprise_attestation,
            event=event,
            on_keepalive=on_keepalive,
        )

        # Process extenstion outputs
        extension_outputs = {}
        try:
            for ext in used_extensions:
                output = ext.process_create_output(att_obj, pin_token, pin_protocol)
                if output is not None:
                    extension_outputs.update(output)
        except ValueError as e:
            raise ClientError.ERR.CONFIGURATION_UNSUPPORTED(e)

        return AuthenticatorAttestationResponse(
            client_data,
            AttestationObject.create(att_obj.fmt, att_obj.auth_data, att_obj.att_stmt),
            extension_outputs,
        )

    def do_get_assertion(
        self,
        client_data,
        rp_id,
        allow_list,
        extensions,
        user_verification,
        event,
    ):
        if allow_list:
            # Filter out credential IDs which are too long
            max_len = self.info.max_cred_id_length
            if max_len:
                allow_list = [e for e in allow_list if len(e) <= max_len]
            if not allow_list:
                raise CtapError(CtapError.ERR.NO_CREDENTIALS)

            # Reject the request if too many credentials remain.
            max_creds = self.info.max_creds_in_list
            if max_creds and len(allow_list) > max_creds:
                raise ClientError.ERR.BAD_REQUEST("allow_list too long")

        # Process extensions
        client_inputs = extensions or {}
        extension_inputs = {}
        used_extensions = []
        permissions = ClientPin.PERMISSION(0)
        try:
            for ext in [cls(self.ctap2) for cls in self.extensions]:
                auth_input, req_perms = ext.process_get_input_with_permissions(
                    client_inputs
                )
                if auth_input is not None:
                    used_extensions.append(ext)
                    permissions |= req_perms
                    extension_inputs[ext.NAME] = auth_input
        except ValueError as e:
            raise ClientError.ERR.CONFIGURATION_UNSUPPORTED(e)

        on_keepalive = _user_keepalive(self.user_interaction)

        pin_protocol, pin_token, pin_auth, internal_uv = self._get_auth_params(
            client_data, rp_id, user_verification, permissions, event, on_keepalive
        )
        options = {"uv": True} if internal_uv else None

        assertions = self.ctap2.get_assertions(
            rp_id,
            client_data.hash,
            allow_list or None,
            extension_inputs or None,
            options,
            pin_auth,
            pin_protocol.VERSION if pin_protocol else None,
            event=event,
            on_keepalive=on_keepalive,
        )

        return _Ctap2ClientAssertionSelection(
            client_data,
            assertions,
            used_extensions,
            pin_token,
            pin_protocol,
        )


class Fido2Client(WebAuthnClient, _BaseClient):
    """WebAuthn-like client implementation.

    The client allows registration and authentication of WebAuthn credentials against
    an Authenticator using CTAP (1 or 2).

    :param device: CtapDevice to use.
    :param str origin: The origin to use.
    :param verify: Function to verify an RP ID for a given origin.
    """

    def __init__(
        self,
        device: CtapDevice,
        origin: str,
        verify: Callable[[str, str], bool] = verify_rp_id,
        extension_types: Sequence[Type[Ctap2Extension]] = _default_extensions(),
        user_interaction: UserInteraction = UserInteraction(),
    ):
        super().__init__(origin, verify)

        # TODO: Decide how to configure this list.
        self._enterprise_rpid_list: Optional[Sequence[str]] = None

        try:
            self._backend: _ClientBackend = _Ctap2ClientBackend(
                device, user_interaction, extension_types
            )
        except (ValueError, CtapError):
            self._backend = _Ctap1ClientBackend(device, user_interaction)

    @property
    def info(self) -> Info:
        return self._backend.info

    def selection(self, event: Optional[Event] = None) -> None:
        try:
            self._backend.selection(event)
        except CtapError as e:
            raise _ctap2client_err(e)

    def make_credential(
        self,
        options: PublicKeyCredentialCreationOptions,
        event: Optional[Event] = None,
    ) -> AuthenticatorAttestationResponse:
        """Creates a credential.

        :param options: PublicKeyCredentialCreationOptions data.
        :param threading.Event event: (optional) Signal to abort the operation.
        """

        options = PublicKeyCredentialCreationOptions.from_dict(options)
        event = event or Event()
        if options.timeout:
            timer = Timer(options.timeout / 1000, event.set)
            timer.daemon = True
            timer.start()

        rp = options.rp
        if rp.id is None:
            url = urlparse(self.origin)
            if url.scheme != "https" or not url.netloc:
                raise ClientError.ERR.BAD_REQUEST(
                    "RP ID required for non-https origin."
                )
            rp = replace(rp, id=url.netloc)

        logger.debug(f"Register a new credential for RP ID: {rp.id}")
        self._verify_rp_id(rp.id)

        client_data = self._build_client_data(
            CollectedClientData.TYPE.CREATE, options.challenge
        )

        selection = options.authenticator_selection or AuthenticatorSelectionCriteria()
        enterprise_attestation = None
        if options.attestation == AttestationConveyancePreference.ENTERPRISE:
            if self.info.options.get("ep"):
                if self._enterprise_rpid_list is not None:
                    # Platform facilitated
                    if rp.id in self._enterprise_rpid_list:
                        enterprise_attestation = 2
                else:
                    # Vendor facilitated
                    enterprise_attestation = 1

        try:
            return self._backend.do_make_credential(
                client_data,
                rp,
                options.user,
                options.pub_key_cred_params,
                options.exclude_credentials,
                options.extensions,
                selection.require_resident_key,
                selection.user_verification,
                enterprise_attestation,
                event,
            )
        except CtapError as e:
            raise _ctap2client_err(e)
        finally:
            if options.timeout:
                timer.cancel()

    def get_assertion(
        self,
        options: PublicKeyCredentialRequestOptions,
        event: Optional[Event] = None,
    ) -> AssertionSelection:
        """Get an assertion.

        :param options: PublicKeyCredentialRequestOptions data.
        :param threading.Event event: (optional) Signal to abort the operation.
        """

        options = PublicKeyCredentialRequestOptions.from_dict(options)
        event = event or Event()
        if options.timeout:
            timer = Timer(options.timeout / 1000, event.set)
            timer.daemon = True
            timer.start()

        logger.debug(f"Assert a credential for RP ID: {options.rp_id}")
        self._verify_rp_id(options.rp_id)

        client_data = self._build_client_data(
            CollectedClientData.TYPE.GET, options.challenge
        )

        try:
            return self._backend.do_get_assertion(
                client_data,
                options.rp_id,
                options.allow_credentials,
                options.extensions,
                options.user_verification,
                event,
            )
        except CtapError as e:
            raise _ctap2client_err(e)
        finally:
            if options.timeout:
                timer.cancel()


if platform.system().lower() == "windows":
    try:
        from .win_api import (
            WinAPI,
            WebAuthNAuthenticatorAttachment,
            WebAuthNUserVerificationRequirement,
            WebAuthNAttestationConvoyancePreference,
        )
    except Exception:  # nosec # TODO: Make this less generic
        pass


class WindowsClient(WebAuthnClient, _BaseClient):
    """Fido2Client-like class using the Windows WebAuthn API.

    Note: This class only works on Windows 10 19H1 or later. This is also when Windows
    started restricting access to FIDO devices, causing the standard client classes to
    require admin priveleges to run (unlike this one).

    The make_credential and get_assertion methods are intended to work as a drop-in
    replacement for the Fido2Client methods of the same name.

    :param str origin: The origin to use.
    :param verify: Function to verify an RP ID for a given origin.
    :param ctypes.wintypes.HWND handle: (optional) Window reference to use.
    """

    def __init__(
        self,
        origin: str,
        verify: Callable[[str, str], bool] = verify_rp_id,
        handle=None,
    ):
        super().__init__(origin, verify)
        self.api = WinAPI(handle)
        self.info = Info(
            versions=["U2F_V2", "FIDO_2_0"], extensions=[], aaguid=Aaguid.NONE
        )

    @staticmethod
    def is_available() -> bool:
        return platform.system().lower() == "windows" and WinAPI.version > 0

    def make_credential(self, options, **kwargs):
        """Create a credential using Windows WebAuthN APIs.

        :param options: PublicKeyCredentialCreationOptions data.
        :param threading.Event event: (optional) Signal to abort the operation.
        """

        options = PublicKeyCredentialCreationOptions.from_dict(options)

        logger.debug(f"Register a new credential for RP ID: {options.rp.id}")
        self._verify_rp_id(options.rp.id)

        client_data = self._build_client_data(
            CollectedClientData.TYPE.CREATE, options.challenge
        )

        selection = options.authenticator_selection or AuthenticatorSelectionCriteria()

        try:
            result = self.api.make_credential(
                options.rp,
                options.user,
                options.pub_key_cred_params,
                client_data,
                options.timeout or 0,
                selection.require_resident_key or False,
                WebAuthNAuthenticatorAttachment.from_string(
                    selection.authenticator_attachment or "any"
                ),
                WebAuthNUserVerificationRequirement.from_string(
                    selection.user_verification or "discouraged"
                ),
                WebAuthNAttestationConvoyancePreference.from_string(
                    options.attestation or "none"
                ),
                options.exclude_credentials,
                options.extensions,
                kwargs.get("event"),
            )
        except OSError as e:
            raise ClientError.ERR.OTHER_ERROR(e)

        logger.info("New credential registered")
        return AuthenticatorAttestationResponse(
            client_data, AttestationObject(result), {}
        )

    def get_assertion(self, options, **kwargs):
        """Get assertion using Windows WebAuthN APIs.

        :param options: PublicKeyCredentialRequestOptions data.
        :param threading.Event event: (optional) Signal to abort the operation.
        """

        options = PublicKeyCredentialRequestOptions.from_dict(options)

        logger.debug(f"Assert a credential for RP ID: {options.rp_id}")
        self._verify_rp_id(options.rp_id)

        client_data = self._build_client_data(
            CollectedClientData.TYPE.GET, options.challenge
        )

        try:
            (credential, auth_data, signature, user_id) = self.api.get_assertion(
                options.rp_id,
                client_data,
                options.timeout or 0,
                WebAuthNAuthenticatorAttachment.ANY,
                WebAuthNUserVerificationRequirement.from_string(
                    options.user_verification or "discouraged"
                ),
                options.allow_credentials,
                options.extensions,
                kwargs.get("event"),
            )
        except OSError as e:
            raise ClientError.ERR.OTHER_ERROR(e)

        user = {"id": user_id} if user_id else None
        return AssertionSelection(
            client_data,
            [
                AssertionResponse(
                    credential=credential,
                    auth_data=auth_data,
                    signature=signature,
                    user=user,
                )
            ],
        )
