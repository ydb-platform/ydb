#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# Copyright (c) 2022-2024, LeXtudio Inc. <support@lextudio.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
"""
This module provides constants and backward-compatible protocol IDs for SNMP authentication and privacy protocols, as well as key material types used in the User-based Security Model (USM).

Constants:
    USM_AUTH_NONE: No Authentication Protocol.
    USM_AUTH_HMAC96_MD5: The HMAC-MD5-96 Digest Authentication Protocol.
    USM_AUTH_HMAC96_SHA: The HMAC-SHA-96 Digest Authentication Protocol AKA SHA-1.
    USM_AUTH_HMAC128_SHA224: The HMAC-SHA-2 Digest Authentication Protocols.
    USM_AUTH_HMAC192_SHA256: The HMAC-SHA-2 Digest Authentication Protocols.
    USM_AUTH_HMAC256_SHA384: The HMAC-SHA-2 Digest Authentication Protocols.
    USM_AUTH_HMAC384_SHA512: The HMAC-SHA-2 Digest Authentication Protocols.
    USM_PRIV_NONE: No Privacy Protocol.
    USM_PRIV_CBC56_DES: The CBC-DES Symmetric Encryption Protocol.
    USM_PRIV_CBC168_3DES: The 3DES-EDE Symmetric Encryption Protocol.
    USM_PRIV_CFB128_AES: The CFB128-AES-128 Symmetric Encryption Protocol.
    USM_PRIV_CFB192_AES: The CFB128-AES-192 Symmetric Encryption Protocol with Reeder key localization.
    USM_PRIV_CFB256_AES: The CFB128-AES-256 Symmetric Encryption Protocol with Reeder key localization.
    USM_PRIV_CFB192_AES_BLUMENTHAL: The CFB128-AES-192 Symmetric Encryption Protocol.
    USM_PRIV_CFB256_AES_BLUMENTHAL: The CFB128-AES-256 Symmetric Encryption Protocol.
    USM_KEY_TYPE_PASSPHRASE: USM key material type - plain-text pass phrase.
    USM_KEY_TYPE_MASTER: USM key material type - hashed pass-phrase AKA master key.
    USM_KEY_TYPE_LOCALIZED: USM key material type - hashed pass-phrase hashed with Context SNMP Engine ID.

Backward-compatible protocol IDs:
    usmNoAuthProtocol: Alias for USM_AUTH_NONE.
    usmHMACMD5AuthProtocol: Alias for USM_AUTH_HMAC96_MD5.
    usmHMACSHAAuthProtocol: Alias for USM_AUTH_HMAC96_SHA.
    usmHMAC128SHA224AuthProtocol: Alias for USM_AUTH_HMAC128_SHA224.
    usmHMAC192SHA256AuthProtocol: Alias for USM_AUTH_HMAC192_SHA256.
    usmHMAC256SHA384AuthProtocol: Alias for USM_AUTH_HMAC256_SHA384.
    usmHMAC384SHA512AuthProtocol: Alias for USM_AUTH_HMAC384_SHA512.
    usmNoPrivProtocol: Alias for USM_PRIV_NONE.
    usmDESPrivProtocol: Alias for USM_PRIV_CBC56_DES.
    usm3DESEDEPrivProtocol: Alias for USM_PRIV_CBC168_3DES.
    usmAesCfb128Protocol: Alias for USM_PRIV_CFB128_AES.
    usmAesCfb192Protocol: Alias for USM_PRIV_CFB192_AES.
    usmAesCfb256Protocol: Alias for USM_PRIV_CFB256_AES.
    usmAesBlumenthalCfb192Protocol: Alias for USM_PRIV_CFB192_AES_BLUMENTHAL.
    usmAesBlumenthalCfb256Protocol: Alias for USM_PRIV_CFB256_AES_BLUMENTHAL.
    usmKeyTypePassphrase: Alias for USM_KEY_TYPE_PASSPHRASE.
    usmKeyTypeMaster: Alias for USM_KEY_TYPE_MASTER.
    usmKeyTypeLocalized: Alias for USM_KEY_TYPE_LOCALIZED.

Deprecated Attributes:
    A dictionary mapping old attribute names to new attribute names.

Functions:
    __getattr__(attr: str):
        Handles access to deprecated attributes, issuing a warning and returning
        the new attribute if available, or raising an AttributeError if not.
"""
import warnings

from pysnmp.entity.engine import *
from pysnmp.hlapi.v3arch.asyncio import auth
from pysnmp.hlapi.v3arch.asyncio.auth import CommunityData, UsmUserData
from pysnmp.hlapi.v3arch.asyncio.cmdgen import *
from pysnmp.hlapi.v3arch.asyncio.context import *
from pysnmp.hlapi.v3arch.asyncio.ntforg import *
from pysnmp.hlapi.v3arch.asyncio.transport import *
from pysnmp.proto.rfc1902 import *
from pysnmp.proto.rfc1905 import EndOfMibView, NoSuchInstance, NoSuchObject
from pysnmp.smi.rfc1902 import *

USM_AUTH_NONE = auth.USM_AUTH_NONE
"""No Authentication Protocol"""

USM_AUTH_HMAC96_MD5 = auth.USM_AUTH_HMAC96_MD5
"""The HMAC-MD5-96 Digest Authentication Protocol (:RFC:`3414#section-6`)"""

USM_AUTH_HMAC96_SHA = auth.USM_AUTH_HMAC96_SHA
"""The HMAC-SHA-96 Digest Authentication Protocol AKA SHA-1 \
(:RFC:`3414#section-7`)"""

USM_AUTH_HMAC128_SHA224 = auth.USM_AUTH_HMAC128_SHA224
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_AUTH_HMAC192_SHA256 = auth.USM_AUTH_HMAC192_SHA256
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_AUTH_HMAC256_SHA384 = auth.USM_AUTH_HMAC256_SHA384
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_AUTH_HMAC384_SHA512 = auth.USM_AUTH_HMAC384_SHA512
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_PRIV_NONE = auth.USM_PRIV_NONE
"""No Privacy Protocol"""

USM_PRIV_CBC56_DES = auth.USM_PRIV_CBC56_DES
"""The CBC-DES Symmetric Encryption Protocol (:RFC:`3414#section-8`)"""

USM_PRIV_CBC168_3DES = auth.USM_PRIV_CBC168_3DES
"""The 3DES-EDE Symmetric Encryption Protocol (`draft-reeder-snmpv3-usm-3desede-00 \
<https:://tools.ietf.org/html/draft-reeder-snmpv3-usm-3desede-00#section-5>`_)"""

USM_PRIV_CFB128_AES = auth.USM_PRIV_CFB128_AES
"""The CFB128-AES-128 Symmetric Encryption Protocol (:RFC:`3826#section-3`)"""

USM_PRIV_CFB192_AES = auth.USM_PRIV_CFB192_AES
"""The CFB128-AES-192 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 \
<https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_) with \
Reeder key localization. Also known as AES-192-Cisco"""

USM_PRIV_CFB256_AES = auth.USM_PRIV_CFB256_AES
"""The CFB128-AES-256 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 \
<https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_) with \
Reeder key localization. Also also known as AES-256-Cisco"""

USM_PRIV_CFB192_AES_BLUMENTHAL = auth.USM_PRIV_CFB192_AES_BLUMENTHAL
"""The CFB128-AES-192 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 \
<https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_)"""

USM_PRIV_CFB256_AES_BLUMENTHAL = auth.USM_PRIV_CFB256_AES_BLUMENTHAL
"""The CFB128-AES-256 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 \
<https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_)"""

USM_KEY_TYPE_PASSPHRASE = auth.USM_KEY_TYPE_PASSPHRASE
"""USM key material type - plain-text pass phrase (:RFC:`3414#section-2.6`)"""

USM_KEY_TYPE_MASTER = auth.USM_KEY_TYPE_MASTER
"""USM key material type - hashed pass-phrase AKA master key \
(:RFC:`3414#section-2.6`)"""

USM_KEY_TYPE_LOCALIZED = auth.USM_KEY_TYPE_LOCALIZED
"""USM key material type - hashed pass-phrase hashed with Context SNMP Engine \
ID (:RFC:`3414#section-2.6`)"""

# Backward-compatible protocol IDs

usmNoAuthProtocol = USM_AUTH_NONE  # noqa: N816
usmHMACMD5AuthProtocol = USM_AUTH_HMAC96_MD5  # noqa: N816
usmHMACSHAAuthProtocol = USM_AUTH_HMAC96_SHA  # noqa: N816
usmHMAC128SHA224AuthProtocol = USM_AUTH_HMAC128_SHA224  # noqa: N816
usmHMAC192SHA256AuthProtocol = USM_AUTH_HMAC192_SHA256  # noqa: N816
usmHMAC256SHA384AuthProtocol = USM_AUTH_HMAC256_SHA384  # noqa: N816
usmHMAC384SHA512AuthProtocol = USM_AUTH_HMAC384_SHA512  # noqa: N816
usmNoPrivProtocol = USM_PRIV_NONE  # noqa: N816
usmDESPrivProtocol = USM_PRIV_CBC56_DES  # noqa: N816
usm3DESEDEPrivProtocol = USM_PRIV_CBC168_3DES  # noqa: N816
usmAesCfb128Protocol = USM_PRIV_CFB128_AES  # noqa: N816
usmAesCfb192Protocol = USM_PRIV_CFB192_AES  # noqa: N816
usmAesCfb256Protocol = USM_PRIV_CFB256_AES  # noqa: N816
usmAesBlumenthalCfb192Protocol = USM_PRIV_CFB192_AES_BLUMENTHAL  # noqa: N816
usmAesBlumenthalCfb256Protocol = USM_PRIV_CFB256_AES_BLUMENTHAL  # noqa: N816

usmKeyTypePassphrase = USM_KEY_TYPE_PASSPHRASE  # noqa: N816
usmKeyTypeMaster = USM_KEY_TYPE_MASTER  # noqa: N816
usmKeyTypeLocalized = USM_KEY_TYPE_LOCALIZED  # noqa: N816

# Old to new attribute mapping
deprecated_attributes = {
    "usmNoAuthProtocol": "USM_AUTH_NONE",
    "usmHMACMD5AuthProtocol": "USM_AUTH_HMAC96_MD5",
    "usmHMACSHAAuthProtocol": "USM_AUTH_HMAC96_SHA",
    "usmHMAC128SHA224AuthProtocol": "USM_AUTH_HMAC128_SHA224",
    "usmHMAC192SHA256AuthProtocol": "USM_AUTH_HMAC192_SHA256",
    "usmHMAC256SHA384AuthProtocol": "USM_AUTH_HMAC256_SHA384",
    "usmHMAC384SHA512AuthProtocol": "USM_AUTH_HMAC384_SHA512",
    "usmNoPrivProtocol": "USM_PRIV_NONE",
    "usmDESPrivProtocol": "USM_PRIV_CBC56_DES",
    "usm3DESEDEPrivProtocol": "USM_PRIV_CBC168_3DES",
    "usmAesCfb128Protocol": "USM_PRIV_CFB128_AES",
    "usmAesCfb192Protocol": "USM_PRIV_CFB192_AES",
    "usmAesCfb256Protocol": "USM_PRIV_CFB256_AES",
    "usmAesBlumenthalCfb192Protocol": "USM_PRIV_CFB192_AES_BLUMENTHAL",
    "usmAesBlumenthalCfb256Protocol": "USM_PRIV_CFB256_AES_BLUMENTHAL",
    "usmKeyTypePassphrase": "USM_KEY_TYPE_PASSPHRASE",
    "usmKeyTypeMaster": "USM_KEY_TYPE_MASTER",
    "usmKeyTypeLocalized": "USM_KEY_TYPE_LOCALIZED",
}


def __getattr__(attr: str):
    if new_attr := deprecated_attributes.get(attr):
        warnings.warn(
            f"{attr} is deprecated. Please use {new_attr} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[new_attr]
    raise AttributeError(f"module '{__name__}' has no attribute '{attr}'")
