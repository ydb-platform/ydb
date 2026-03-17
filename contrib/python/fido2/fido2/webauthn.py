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

from . import cbor
from .cose import CoseKey, ES256
from .utils import (
    sha256,
    websafe_decode,
    websafe_encode,
    ByteBuffer,
    _CamelCaseDataObject,
)
from .features import webauthn_json_mapping
from enum import Enum, EnumMeta, unique, IntFlag
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence, Tuple, Union, cast
import struct
import json

"""
Data classes based on the W3C WebAuthn specification (https://www.w3.org/TR/webauthn/).

See the specification for a description and details on their usage.
"""

# Binary types


class Aaguid(bytes):
    def __init__(self, data: bytes):
        if len(self) != 16:
            raise ValueError("AAGUID must be 16 bytes")

    def __bool__(self):
        return self != Aaguid.NONE

    def __str__(self):
        h = self.hex()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"

    def __repr__(self):
        return f"AAGUID({str(self)})"

    @classmethod
    def parse(cls, value: str) -> Aaguid:
        return cls.fromhex(value.replace("-", ""))

    NONE: Aaguid


# Special instance of AAGUID used when there is no AAGUID
Aaguid.NONE = Aaguid(b"\0" * 16)


@dataclass(init=False, frozen=True)
class AttestedCredentialData(bytes):
    aaguid: Aaguid
    credential_id: bytes
    public_key: CoseKey

    def __init__(self, _: bytes):
        super().__init__()

        parsed = AttestedCredentialData._parse(self)
        object.__setattr__(self, "aaguid", parsed[0])
        object.__setattr__(self, "credential_id", parsed[1])
        object.__setattr__(self, "public_key", parsed[2])
        if parsed[3]:
            raise ValueError("Wrong length")

    def __str__(self):  # Override default implementation from bytes.
        return repr(self)

    @staticmethod
    def _parse(data: bytes) -> Tuple[bytes, bytes, CoseKey, bytes]:
        """Parse the components of an AttestedCredentialData from a binary
        string, and return them.

        :param data: A binary string containing an attested credential data.
        :return: AAGUID, credential ID, public key, and remaining data.
        """
        reader = ByteBuffer(data)
        aaguid = Aaguid(reader.read(16))
        cred_id = reader.read(reader.unpack(">H"))
        pub_key, rest = cbor.decode_from(reader.read())
        return aaguid, cred_id, CoseKey.parse(pub_key), rest

    @classmethod
    def create(
        cls, aaguid: bytes, credential_id: bytes, public_key: CoseKey
    ) -> AttestedCredentialData:
        """Create an AttestedCredentialData by providing its components.

        :param aaguid: The AAGUID of the authenticator.
        :param credential_id: The binary ID of the credential.
        :param public_key: A COSE formatted public key.
        :return: The attested credential data.
        """
        return cls(
            aaguid
            + struct.pack(">H", len(credential_id))
            + credential_id
            + cbor.encode(public_key)
        )

    @classmethod
    def unpack_from(cls, data: bytes) -> Tuple[AttestedCredentialData, bytes]:
        """Unpack an AttestedCredentialData from a byte string, returning it and
        any remaining data.

        :param data: A binary string containing an attested credential data.
        :return: The parsed AttestedCredentialData, and any remaining data from
            the input.
        """
        aaguid, cred_id, pub_key, rest = cls._parse(data)
        return cls.create(aaguid, cred_id, pub_key), rest

    @classmethod
    def from_ctap1(cls, key_handle: bytes, public_key: bytes) -> AttestedCredentialData:
        """Create an AttestatedCredentialData from a CTAP1 RegistrationData instance.

        :param key_handle: The CTAP1 credential key_handle.
        :type key_handle: bytes
        :param public_key: The CTAP1 65 byte public key.
        :type public_key: bytes
        :return: The credential data, using an all-zero AAGUID.
        :rtype: AttestedCredentialData
        """
        return cls.create(Aaguid.NONE, key_handle, ES256.from_ctap1(public_key))


@dataclass(init=False, frozen=True)
class AuthenticatorData(bytes):
    """Binary encoding of the authenticator data.

    :param _: The binary representation of the authenticator data.
    :ivar rp_id_hash: SHA256 hash of the RP ID.
    :ivar flags: The flags of the authenticator data, see
        AuthenticatorData.FLAG.
    :ivar counter: The signature counter of the authenticator.
    :ivar credential_data: Attested credential data, if available.
    :ivar extensions: Authenticator extensions, if available.
    """

    class FLAG(IntFlag):
        """Authenticator data flags

        See https://www.w3.org/TR/webauthn/#sec-authenticator-data for details
        """

        # Names used in WebAuthn
        UP = 0x01
        UV = 0x04
        BE = 0x08
        BS = 0x10
        AT = 0x40
        ED = 0x80

        # Aliases (for historical purposes)
        USER_PRESENT = 0x01
        USER_VERIFIED = 0x04
        BACKUP_ELIGIBILITY = 0x08
        BACKUP_STATE = 0x10
        ATTESTED = 0x40
        EXTENSION_DATA = 0x80

    rp_id_hash: bytes
    flags: AuthenticatorData.FLAG
    counter: int
    credential_data: Optional[AttestedCredentialData]
    extensions: Optional[Mapping]

    def __init__(self, _: bytes):
        super().__init__()

        reader = ByteBuffer(self)
        object.__setattr__(self, "rp_id_hash", reader.read(32))
        object.__setattr__(self, "flags", reader.unpack("B"))
        object.__setattr__(self, "counter", reader.unpack(">I"))
        rest = reader.read()

        if self.flags & AuthenticatorData.FLAG.AT:
            credential_data, rest = AttestedCredentialData.unpack_from(rest)
        else:
            credential_data = None
        object.__setattr__(self, "credential_data", credential_data)

        if self.flags & AuthenticatorData.FLAG.ED:
            extensions, rest = cbor.decode_from(rest)
        else:
            extensions = None
        object.__setattr__(self, "extensions", extensions)

        if rest:
            raise ValueError("Wrong length")

    def __str__(self):  # Override default implementation from bytes.
        return repr(self)

    @classmethod
    def create(
        cls,
        rp_id_hash: bytes,
        flags: AuthenticatorData.FLAG,
        counter: int,
        credential_data: bytes = b"",
        extensions: Optional[Mapping] = None,
    ):
        """Create an AuthenticatorData instance.

        :param rp_id_hash: SHA256 hash of the RP ID.
        :param flags: Flags of the AuthenticatorData.
        :param counter: Signature counter of the authenticator data.
        :param credential_data: Authenticated credential data (only if attested
            credential data flag is set).
        :param extensions: Authenticator extensions (only if ED flag is set).
        :return: The authenticator data.
        """
        return cls(
            rp_id_hash
            + struct.pack(">BI", flags, counter)
            + credential_data
            + (cbor.encode(extensions) if extensions is not None else b"")
        )

    def is_user_present(self) -> bool:
        """Return true if the User Present flag is set."""
        return bool(self.flags & AuthenticatorData.FLAG.UP)

    def is_user_verified(self) -> bool:
        """Return true if the User Verified flag is set."""
        return bool(self.flags & AuthenticatorData.FLAG.UV)

    def is_backup_eligible(self) -> bool:
        """Return true if the Backup Eligibility flag is set."""
        return bool(self.flags & AuthenticatorData.FLAG.BE)

    def is_backed_up(self) -> bool:
        """Return true if the Backup State flag is set."""
        return bool(self.flags & AuthenticatorData.FLAG.BS)

    def is_attested(self) -> bool:
        """Return true if the Attested credential data flag is set."""
        return bool(self.flags & AuthenticatorData.FLAG.AT)

    def has_extension_data(self) -> bool:
        """Return true if the Extenstion data flag is set."""
        return bool(self.flags & AuthenticatorData.FLAG.ED)


@dataclass(init=False, frozen=True)
class AttestationObject(bytes):  # , Mapping[str, Any]):
    """Binary CBOR encoded attestation object.

    :param _: The binary representation of the attestation object.
    :ivar fmt: The type of attestation used.
    :ivar auth_data: The attested authenticator data.
    :ivar att_statement: The attestation statement.
    """

    fmt: str
    auth_data: AuthenticatorData
    att_stmt: Mapping[str, Any]

    def __init__(self, _: bytes):
        super().__init__()

        data = cast(Mapping[str, Any], cbor.decode(bytes(self)))
        object.__setattr__(self, "fmt", data["fmt"])
        object.__setattr__(self, "auth_data", AuthenticatorData(data["authData"]))
        object.__setattr__(self, "att_stmt", data["attStmt"])

    def __str__(self):  # Override default implementation from bytes.
        return repr(self)

    @classmethod
    def create(
        cls, fmt: str, auth_data: AuthenticatorData, att_stmt: Mapping[str, Any]
    ) -> AttestationObject:
        return cls(
            cbor.encode({"fmt": fmt, "authData": auth_data, "attStmt": att_stmt})
        )

    @classmethod
    def from_ctap1(cls, app_param: bytes, registration) -> AttestationObject:
        """Create an AttestationObject from a CTAP1 RegistrationData instance.

        :param app_param: SHA256 hash of the RP ID used for the CTAP1 request.
        :type app_param: bytes
        :param registration: The CTAP1 registration data.
        :type registration: RegistrationData
        :return: The attestation object, using the "fido-u2f" format.
        :rtype: AttestationObject
        """
        return cls.create(
            "fido-u2f",
            AuthenticatorData.create(
                app_param,
                AuthenticatorData.FLAG.AT | AuthenticatorData.FLAG.UP,
                0,
                AttestedCredentialData.from_ctap1(
                    registration.key_handle, registration.public_key
                ),
            ),
            {"x5c": [registration.certificate], "sig": registration.signature},
        )


@dataclass(init=False, frozen=True)
class CollectedClientData(bytes):
    @unique
    class TYPE(str, Enum):
        CREATE = "webauthn.create"
        GET = "webauthn.get"

    type: str
    challenge: bytes
    origin: str
    cross_origin: bool = False

    def __init__(self, _: bytes):
        super().__init__()

        data = json.loads(self.decode())
        object.__setattr__(self, "type", data["type"])
        object.__setattr__(self, "challenge", websafe_decode(data["challenge"]))
        object.__setattr__(self, "origin", data["origin"])
        object.__setattr__(self, "cross_origin", data.get("crossOrigin", False))

    @classmethod
    def create(
        cls,
        type: str,
        challenge: Union[bytes, str],
        origin: str,
        cross_origin: bool = False,
        **kwargs,
    ) -> CollectedClientData:
        if isinstance(challenge, bytes):
            encoded_challenge = websafe_encode(challenge)
        else:
            encoded_challenge = challenge
        return cls(
            json.dumps(
                {
                    "type": type,
                    "challenge": encoded_challenge,
                    "origin": origin,
                    "crossOrigin": cross_origin,
                    **kwargs,
                },
                separators=(",", ":"),
            ).encode()
        )

    def __str__(self):  # Override default implementation from bytes.
        return repr(self)

    @property
    def b64(self) -> str:
        return websafe_encode(self)

    @property
    def hash(self) -> bytes:
        return sha256(self)


class _StringEnumMeta(EnumMeta):
    def _get_value(cls, value):
        return None

    def __call__(cls, value, *args, **kwargs):
        try:
            return super().__call__(value, *args, **kwargs)
        except ValueError:
            return cls._get_value(value)


class _StringEnum(str, Enum, metaclass=_StringEnumMeta):
    """Enum of strings for WebAuthn types.

    Unrecognized values are treated as missing.
    """


_b64_metadata = dict(
    serialize=lambda x: websafe_encode(x) if webauthn_json_mapping.enabled else x,
    deserialize=lambda x: websafe_decode(x) if webauthn_json_mapping.enabled else x,
)


@unique
class AttestationConveyancePreference(_StringEnum):
    NONE = "none"
    INDIRECT = "indirect"
    DIRECT = "direct"
    ENTERPRISE = "enterprise"


@unique
class UserVerificationRequirement(_StringEnum):
    REQUIRED = "required"
    PREFERRED = "preferred"
    DISCOURAGED = "discouraged"


@unique
class ResidentKeyRequirement(_StringEnum):
    REQUIRED = "required"
    PREFERRED = "preferred"
    DISCOURAGED = "discouraged"


@unique
class AuthenticatorAttachment(_StringEnum):
    PLATFORM = "platform"
    CROSS_PLATFORM = "cross-platform"


@unique
class AuthenticatorTransport(_StringEnum):
    USB = "usb"
    NFC = "nfc"
    BLE = "ble"
    HYBRID = "hybrid"
    INTERNAL = "internal"


@unique
class PublicKeyCredentialType(_StringEnum):
    PUBLIC_KEY = "public-key"


@dataclass(eq=False, frozen=True)
class PublicKeyCredentialRpEntity(_CamelCaseDataObject):
    name: str
    id: Optional[str] = None

    @property
    def id_hash(self) -> Optional[bytes]:
        """Return SHA256 hash of the identifier."""
        return sha256(self.id.encode("utf8")) if self.id else None


@dataclass(eq=False, frozen=True)
class PublicKeyCredentialUserEntity(_CamelCaseDataObject):
    name: str
    id: bytes = field(metadata=_b64_metadata)
    display_name: Optional[str] = None


@dataclass(eq=False, frozen=True)
class PublicKeyCredentialParameters(_CamelCaseDataObject):
    type: PublicKeyCredentialType
    alg: int

    @classmethod
    def _deserialize_list(cls, value):
        if value is None:
            return None
        items = [cls.from_dict(e) for e in value]
        return [e for e in items if e.type is not None]


@dataclass(eq=False, frozen=True)
class PublicKeyCredentialDescriptor(_CamelCaseDataObject):
    type: PublicKeyCredentialType
    id: bytes = field(metadata=_b64_metadata)
    transports: Optional[Sequence[AuthenticatorTransport]] = None

    @classmethod
    def _deserialize_list(cls, value):
        if value is None:
            return None
        items = [cls.from_dict(e) for e in value]
        return [e for e in items if e.type is not None]


@dataclass(eq=False, frozen=True)
class AuthenticatorSelectionCriteria(_CamelCaseDataObject):
    authenticator_attachment: Optional[AuthenticatorAttachment] = None
    resident_key: Optional[ResidentKeyRequirement] = None
    user_verification: Optional[UserVerificationRequirement] = None
    require_resident_key: Optional[bool] = False

    def __post_init__(self):
        super().__post_init__()

        if self.resident_key is None:
            object.__setattr__(
                self,
                "resident_key",
                (
                    ResidentKeyRequirement.REQUIRED
                    if self.require_resident_key
                    else ResidentKeyRequirement.DISCOURAGED
                ),
            )
        object.__setattr__(
            self,
            "require_resident_key",
            self.resident_key == ResidentKeyRequirement.REQUIRED,
        )


@dataclass(eq=False, frozen=True)
class PublicKeyCredentialCreationOptions(_CamelCaseDataObject):
    rp: PublicKeyCredentialRpEntity
    user: PublicKeyCredentialUserEntity
    challenge: bytes = field(metadata=_b64_metadata)
    pub_key_cred_params: Sequence[PublicKeyCredentialParameters] = field(
        metadata=dict(deserialize=PublicKeyCredentialParameters._deserialize_list),
    )
    timeout: Optional[int] = None
    exclude_credentials: Optional[Sequence[PublicKeyCredentialDescriptor]] = field(
        default=None,
        metadata=dict(deserialize=PublicKeyCredentialDescriptor._deserialize_list),
    )
    authenticator_selection: Optional[AuthenticatorSelectionCriteria] = None
    attestation: Optional[AttestationConveyancePreference] = None
    extensions: Optional[Mapping[str, Any]] = None


@dataclass(eq=False, frozen=True)
class PublicKeyCredentialRequestOptions(_CamelCaseDataObject):
    challenge: bytes = field(metadata=_b64_metadata)
    timeout: Optional[int] = None
    rp_id: Optional[str] = None
    allow_credentials: Optional[Sequence[PublicKeyCredentialDescriptor]] = field(
        default=None,
        metadata=dict(deserialize=PublicKeyCredentialDescriptor._deserialize_list),
    )
    user_verification: Optional[UserVerificationRequirement] = None
    extensions: Optional[Mapping[str, Any]] = None


@dataclass(eq=False, frozen=True)
class AuthenticatorAttestationResponse(_CamelCaseDataObject):
    client_data: CollectedClientData = field(
        metadata=dict(
            _b64_metadata,
            name="clientDataJSON",
        )
    )
    attestation_object: AttestationObject = field(metadata=_b64_metadata)
    extension_results: Optional[Mapping[str, Any]] = None

    def __getitem__(self, key):
        if key == "clientData" and not webauthn_json_mapping.enabled:
            return self.client_data
        return super().__getitem__(key)

    @classmethod
    def from_dict(cls, data: Optional[Mapping[str, Any]]):
        if data is not None and not webauthn_json_mapping.enabled:
            value = dict(data)
            value["clientDataJSON"] = value.pop("clientData", None)
            data = value
        return super().from_dict(data)


@dataclass(eq=False, frozen=True)
class AuthenticatorAssertionResponse(_CamelCaseDataObject):
    client_data: CollectedClientData = field(
        metadata=dict(
            _b64_metadata,
            name="clientDataJSON",
        )
    )
    authenticator_data: AuthenticatorData = field(metadata=_b64_metadata)
    signature: bytes = field(metadata=_b64_metadata)
    user_handle: Optional[bytes] = field(metadata=_b64_metadata, default=None)
    credential_id: Optional[bytes] = field(metadata=_b64_metadata, default=None)
    extension_results: Optional[Mapping[str, Any]] = None

    def __getitem__(self, key):
        if key == "clientData" and not webauthn_json_mapping.enabled:
            return self.client_data
        return super().__getitem__(key)

    @classmethod
    def from_dict(cls, data: Optional[Mapping[str, Any]]):
        if data is not None and not webauthn_json_mapping.enabled:
            value = dict(data)
            value["clientDataJSON"] = value.pop("clientData", None)
            data = value
        return super().from_dict(data)


@dataclass(eq=False, frozen=True)
class RegistrationResponse(_CamelCaseDataObject):
    id: bytes = field(metadata=_b64_metadata)
    response: AuthenticatorAttestationResponse
    authenticator_attachment: Optional[AuthenticatorAttachment] = None
    client_extension_results: Optional[Mapping] = None
    type: Optional[PublicKeyCredentialType] = None

    def __post_init__(self):
        webauthn_json_mapping.require()
        super().__post_init__()


@dataclass(eq=False, frozen=True)
class AuthenticationResponse(_CamelCaseDataObject):
    id: bytes = field(metadata=_b64_metadata)
    response: AuthenticatorAssertionResponse
    authenticator_attachment: Optional[AuthenticatorAttachment] = None
    client_extension_results: Optional[Mapping] = None
    type: Optional[PublicKeyCredentialType] = None

    def __post_init__(self):
        webauthn_json_mapping.require()
        super().__post_init__()


@dataclass(eq=False, frozen=True)
class CredentialCreationOptions(_CamelCaseDataObject):
    public_key: PublicKeyCredentialCreationOptions


@dataclass(eq=False, frozen=True)
class CredentialRequestOptions(_CamelCaseDataObject):
    public_key: PublicKeyCredentialRequestOptions
