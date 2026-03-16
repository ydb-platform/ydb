#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import warnings


from pysnmp import error
from pysnmp.entity import config

__all__ = [
    "CommunityData",
    "UsmUserData",
    "USM_PRIV_CBC168_3DES",
    "USM_PRIV_CFB128_AES",
    "USM_PRIV_CFB192_AES",
    "USM_PRIV_CFB256_AES",
    "USM_PRIV_CFB192_AES_BLUMENTHAL",
    "USM_PRIV_CFB256_AES_BLUMENTHAL",
    "USM_PRIV_CBC56_DES",
    "USM_AUTH_HMAC96_MD5",
    "USM_AUTH_HMAC96_SHA",
    "USM_AUTH_HMAC128_SHA224",
    "USM_AUTH_HMAC192_SHA256",
    "USM_AUTH_HMAC256_SHA384",
    "USM_AUTH_HMAC384_SHA512",
    "USM_AUTH_NONE",
    "USM_PRIV_NONE",
    "USM_KEY_TYPE_PASSPHRASE",
    "USM_KEY_TYPE_MASTER",
    "USM_KEY_TYPE_LOCALIZED",
]


class CommunityData:
    """Creates SNMP v1/v2c configuration entry.

    This object can be used by
    :py:class:`~pysnmp.hlapi.v3arch.asyncio.AsyncCommandGenerator` or
    :py:class:`~pysnmp.hlapi.v3arch.asyncio.AsyncNotificationOriginator`
    and their derivatives for adding new entries to Local Configuration
    Datastore (LCD) managed by :py:class:`~pysnmp.hlapi.v3arch.asyncio.SnmpEngine`
    class instance.

    See :RFC:`2576#section-5.3` for more information on the
    *SNMP-COMMUNITY-MIB::snmpCommunityTable*.

    Parameters
    ----------
    communityIndex: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        Unique index value of a row in snmpCommunityTable. If it is the
        only positional parameter, it is treated as a *communityName*.

    communityName: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        SNMP v1/v2c community string.

    mpModel: :py:class:`int`
        SNMP message processing model AKA SNMP version. Known SNMP versions are:

        * `0` - for SNMP v1
        * `1` - for SNMP v2c (default)


    contextEngineId: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        Indicates the location of the context in which management
        information is accessed when using the community string
        specified by the above communityName.

    contextName: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        The context in which management information is accessed when
        using the above communityName.

    tag: :py:class:`str`
        Arbitrary string that specifies a set of transport endpoints
        from which a command responder application will accept
        management requests with given *communityName* or to which
        notification originator application will send notifications
        when targets are specified by a tag value(s).

        The other way to look at the *tag* feature is that it can make
        specific *communityName* only valid for certain targets.

        The other use-case is when multiple distinct SNMP peers share
        the same *communityName* -- binding each instance of
        *communityName* to transport endpoint lets you distinguish
        SNMP peers from each other (e.g. resolving *communityName* into
        proper *securityName*).

        For more technical information on SNMP configuration tags please
        refer to :RFC:`3413#section-4.1.1` and :RFC:`2576#section-5.3`
        (e.g. the *snmpCommunityTransportTag* object).

        See also: :py:class:`~pysnmp.hlapi.v3arch.asyncio.UdpTransportTarget`

    Warnings
    --------
    If the same *communityIndex* value is supplied repeatedly with
    different *communityName* (or other parameters), the later call
    supersedes all previous calls.

    Make sure not to configure duplicate *communityName* values unless
    they have distinct *mpModel* and/or *tag* fields. This will make
    *communityName* based database lookup ambiguous.

    Examples
    --------
    >>> from pysnmp.hlapi.v3arch.asyncio import CommunityData
    >>> CommunityData('public')
    CommunityData(communityIndex='s1410706889', communityName=<COMMUNITY>, mpModel=1, contextEngineId=None, contextName='', tag='')
    >>> CommunityData('public', 'public')
    CommunityData(communityIndex='public', communityName=<COMMUNITY>, mpModel=1, contextEngineId=None, contextName='', tag='')
    >>>

    """

    message_processing_model = 1  # Default is SMIv2
    security_model = message_processing_model + 1
    security_level = "noAuthNoPriv"
    context_name = b""
    tag = b""

    def __init__(
        self,
        communityIndex,
        communityName=None,
        mpModel=None,
        contextEngineId=None,
        contextName=None,
        tag=None,
        securityName=None,
    ):
        """Create a community data object."""
        if mpModel is not None:
            self.message_processing_model = mpModel
            self.security_model = mpModel + 1
        self.contextEngineId = contextEngineId
        if contextName is not None:
            self.context_name = contextName
        if tag is not None:
            self.tag = tag
        # a single arg is considered as a community name
        if communityName is None:
            communityName, communityIndex = communityIndex, None
        self.communityName = communityName
        # Autogenerate communityIndex if not specified
        if communityIndex is None:
            self.communityIndex = self.securityName = "s%s" % hash(
                (
                    self.communityName,
                    self.message_processing_model,
                    self.contextEngineId,
                    self.context_name,
                    self.tag,
                )
            )
        else:
            self.communityIndex = communityIndex
            self.securityName = (
                securityName is not None and securityName or communityIndex
            )

    def __hash__(self):
        """Return a hash value of the object."""
        raise TypeError("%s is not hashable" % self.__class__.__name__)

    def __repr__(self):
        """Return a string representation of the object."""
        return "{}(communityIndex={!r}, communityName=<COMMUNITY>, mpModel={!r}, contextEngineId={!r}, contextName={!r}, tag={!r}, securityName={!r})".format(
            self.__class__.__name__,
            self.communityIndex,
            self.message_processing_model,
            self.contextEngineId,
            self.context_name,
            self.tag,
            self.securityName,
        )

    def clone(
        self,
        communityIndex=None,
        communityName=None,
        mpModel=None,
        contextEngineId=None,
        contextName=None,
        tag=None,
        securityName=None,
    ):
        """Clone the object with new parameters."""
        # a single arg is considered as a community name
        if communityName is None:
            communityName, communityIndex = communityIndex, None
        return self.__class__(
            communityIndex,
            communityName is None and self.communityName or communityName,
            mpModel is None and self.message_processing_model or mpModel,
            contextEngineId is None and self.contextEngineId or contextEngineId,
            contextName is None and self.context_name or contextName,
            tag is None and self.tag or tag,
            securityName is None and self.securityName or securityName,
        )


USM_AUTH_NONE = config.USM_AUTH_NONE
"""No Authentication Protocol"""

USM_AUTH_HMAC96_MD5 = config.USM_AUTH_HMAC96_MD5
"""The HMAC-MD5-96 Digest Authentication Protocol (:RFC:`3414#section-6`)"""

USM_AUTH_HMAC96_SHA = config.USM_AUTH_HMAC96_SHA
"""The HMAC-SHA-96 Digest Authentication Protocol AKA SHA-1 (:RFC:`3414#section-7`)"""

USM_AUTH_HMAC128_SHA224 = config.USM_AUTH_HMAC128_SHA224
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_AUTH_HMAC192_SHA256 = config.USM_AUTH_HMAC192_SHA256
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_AUTH_HMAC256_SHA384 = config.USM_AUTH_HMAC256_SHA384
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_AUTH_HMAC384_SHA512 = config.USM_AUTH_HMAC384_SHA512
"""The HMAC-SHA-2 Digest Authentication Protocols (:RFC:`7860`)"""

USM_PRIV_NONE = config.USM_PRIV_NONE
"""No Privacy Protocol"""

USM_PRIV_CBC56_DES = config.USM_PRIV_CBC56_DES
"""The CBC-DES Symmetric Encryption Protocol (:RFC:`3414#section-8`)"""

USM_PRIV_CBC168_3DES = config.USM_PRIV_CBC168_3DES
"""The 3DES-EDE Symmetric Encryption Protocol (`draft-reeder-snmpv3-usm-3desede-00 <https:://tools.ietf.org/html/draft-reeder-snmpv3-usm-3desede-00#section-5>`_)"""

USM_PRIV_CFB128_AES = config.USM_PRIV_CFB128_AES
"""The CFB128-AES-128 Symmetric Encryption Protocol (:RFC:`3826#section-3`)"""

USM_PRIV_CFB192_AES = config.USM_PRIV_CFB192_AES
"""The CFB128-AES-192 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 <https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_) with Reeder key localization. Also known as AES-192-Cisco"""

USM_PRIV_CFB256_AES = config.USM_PRIV_CFB256_AES
"""The CFB128-AES-256 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 <https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_) with Reeder key localization. Also known as AES-256-Cisco"""

USM_PRIV_CFB192_AES_BLUMENTHAL = config.USM_PRIV_CFB192_AES_BLUMENTHAL
"""The CFB128-AES-192 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 <https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_)"""

USM_PRIV_CFB256_AES_BLUMENTHAL = config.USM_PRIV_CFB256_AES_BLUMENTHAL
"""The CFB128-AES-256 Symmetric Encryption Protocol (`draft-blumenthal-aes-usm-04 <https:://tools.ietf.org/html/draft-blumenthal-aes-usm-04#section-3>`_)"""

USM_KEY_TYPE_PASSPHRASE = config.USM_KEY_TYPE_PASSPHRASE
"""USM key material type - plain-text pass phrase (:RFC:`3414#section-2.6`)"""

USM_KEY_TYPE_MASTER = config.USM_KEY_TYPE_MASTER
"""USM key material type - hashed pass-phrase AKA master key (:RFC:`3414#section-2.6`)"""

USM_KEY_TYPE_LOCALIZED = config.USM_KEY_TYPE_LOCALIZED
"""USM key material type - hashed pass-phrase hashed with Context SNMP Engine ID (:RFC:`3414#section-2.6`)"""


class UsmUserData:
    """Creates SNMP v3 User Security Model (USM) configuration entry.

    This object can be used by
    :py:class:`~pysnmp.hlapi.v3arch.asyncio.AsyncCommandGenerator` or
    :py:class:`~pysnmp.hlapi.v3arch.asyncio.AsyncNotificationOriginator`
    and their derivatives for adding new entries to Local Configuration
    Datastore (LCD) managed by :py:class:`~pysnmp.hlapi.v3arch.asyncio.SnmpEngine`
    class instance.

    See :RFC:`3414#section-5` for more information on the
    *SNMP-USER-BASED-SM-MIB::usmUserTable*.

    Parameters
    ----------
    userName: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        A human readable string representing the name of the SNMP USM user.

    Other Parameters
    ----------------
    authKey: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        Initial value of the secret authentication key.  If not set,
        :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_NONE`
        is implied.  If set and no *authProtocol* is specified,
        :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_HMAC96_MD5`
        takes effect.

    privKey: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        Initial value of the secret encryption key.  If not set,
        :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_NONE`
        is implied.  If set and no *privProtocol* is specified,
        :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_CBC56_DES`
        takes effect.

    authProtocol: :py:class:`tuple`, :py:class:`~pysnmp.proto.rfc1902.ObjectIdentifier`
        An indication of whether messages sent on behalf of this USM user
        can be authenticated, and if so, the type of authentication protocol
        which is used.

        Supported authentication protocol identifiers are:

        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_NONE` (default is *authKey* not given)
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_HMAC96_MD5` (default if *authKey* is given)
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_HMAC96_SHA`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_HMAC128_SHA224`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_HMAC192_SHA256`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_HMAC256_SHA384`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_AUTH_HMAC384_SHA512`


    securityEngineId: :py:class:`~pysnmp.proto.rfc1902.OctetString`
        The snmpEngineID of the authoritative SNMP engine to which a
        dateRequest message is to be sent. Will be automatically
        discovered from peer if not given, unless localized keys
        are used. In the latter case *securityEngineId* must be
        specified.

        See :RFC:`3414#section-2.5.1` for technical explanation.

    securityName: :py:class:`str`, :py:class:`~pysnmp.proto.rfc1902.OctetString`
        Together with the snmpEngineID it identifies a row in the
        *SNMP-USER-BASED-SM-MIB::usmUserTable* that is to be used
        for securing the message.

        See :RFC:`3414#section-2.5.1` for technical explanation.

    privProtocol: :py:class:`tuple`, :py:class:`~pysnmp.proto.rfc1902.ObjectIdentifier`
        An indication of whether messages sent on behalf of this USM user
        be encrypted, and if so, the type of encryption protocol which is used.

        Supported encryption protocol identifiers are:

        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_NONE` (default is *privKey* not given)
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_CBC56_DES` (default if *privKey* is given)
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_CBC168_3DES`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_CFB128_AES`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_CFB192_AES`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_PRIV_CFB256_AES`


    authKeyType: :py:class:`int`
        Type of `authKey` material. See :RFC:`3414#section-2.6` for
        technical explanation.

        Supported key types are:

        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_KEY_TYPE_PASSPHRASE` (default)
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_KEY_TYPE_MASTER`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_KEY_TYPE_LOCALIZED`

    privKeyType: :py:class:`int`
        Type of `privKey` material. See :RFC:`3414#section-2.6` for
        technical explanation.

        Supported key types are:

        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_KEY_TYPE_PASSPHRASE` (default)
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_KEY_TYPE_MASTER`
        * :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_KEY_TYPE_LOCALIZED`

    Notes
    -----
    If :py:class:`~pysnmp.hlapi.v3arch.asyncio.USM_KEY_TYPE_LOCALIZED` is used when
    running a non-authoritative SNMP engine, USM key localization
    mechanism is not invoked. As a consequence, local SNMP engine
    configuration won't get automatically populated with remote SNMP
    engine's *securityEngineId*.

    Therefore peer SNMP engine's *securityEngineId* must be added
    to local configuration and associated with its localized keys.

    Alternatively, the magic *securityEngineId* value of five zeros
    (*0x0000000000*) can be used to refer to the localized keys that
    should be used with any unknown remote SNMP engine. This feature
    is specific to pysnmp.

    Examples
    --------
    >>> from pysnmp.hlapi.v3arch.asyncio import UsmUserData
    >>> UsmUserData('testuser', authKey='authenticationkey')
    UsmUserData(userName='testuser', authKey=<AUTHKEY>, privKey=<PRIVKEY>, authProtocol=(1,3,6,1,6,3,10,1,1,2), privProtocol=(1,3,6,1,6,3,10,1,2,1))
    >>> UsmUserData('testuser', authKey='authenticationkey', privKey='encryptionkey')
    UsmUserData(userName='testuser', authKey=<AUTHKEY>, privKey=<PRIVKEY>, authProtocol=(1,3,6,1,6,3,10,1,1,2), privProtocol=(1,3,6,1,6,3,10,1,2,2))
    >>>

    """

    authentication_key = privacy_key = None
    authentication_protocol = config.USM_AUTH_NONE
    privacy_protocol = config.USM_PRIV_NONE
    security_level = "noAuthNoPriv"
    security_model = 3
    message_processing_model = 3
    context_name = b""

    def __init__(
        self,
        userName,
        authKey=None,
        privKey=None,
        authProtocol=None,
        privProtocol=None,
        securityEngineId=None,
        securityName=None,
        authKeyType=USM_KEY_TYPE_PASSPHRASE,
        privKeyType=USM_KEY_TYPE_PASSPHRASE,
    ):
        """Create a USM user data object."""
        self.userName = userName
        if securityName is None:
            self.securityName = userName
        else:
            self.securityName = securityName

        if authKey is not None:
            self.authentication_key = authKey
            if authProtocol is None:
                self.authentication_protocol = config.USM_AUTH_HMAC96_MD5
            else:
                self.authentication_protocol = authProtocol
            if self.security_level != "authPriv":
                self.security_level = "authNoPriv"

        if privKey is not None:
            self.privacy_key = privKey
            if self.authentication_protocol == config.USM_AUTH_NONE:
                raise error.PySnmpError("Privacy implies authenticity")
            self.security_level = "authPriv"
            if privProtocol is None:
                self.privacy_protocol = config.USM_PRIV_CBC56_DES
            else:
                self.privacy_protocol = privProtocol

        self.securityEngineId = securityEngineId
        self.authKeyType = authKeyType
        self.privKeyType = privKeyType

    def __hash__(self):
        """Return a hash value of the object."""
        raise TypeError("%s is not hashable" % self.__class__.__name__)

    def __repr__(self):
        """Return a string representation of the object."""
        return "{}(userName={!r}, authKey=<AUTHKEY>, privKey=<PRIVKEY>, authProtocol={!r}, privProtocol={!r}, securityEngineId={!r}, securityName={!r}, authKeyType={!r}, privKeyType={!r})".format(
            self.__class__.__name__,
            self.userName,
            self.authentication_protocol,
            self.privacy_protocol,
            self.securityEngineId is None and "<DEFAULT>" or self.securityEngineId,
            self.securityName,
            self.authKeyType,
            self.privKeyType,
        )

    def clone(
        self,
        userName=None,
        authKey=None,
        privKey=None,
        authProtocol=None,
        privProtocol=None,
        securityEngineId=None,
        securityName=None,
        authKeyType=None,
        privKeyType=None,
    ):
        """Clone the object with new parameters."""
        return self.__class__(
            userName is None and self.userName or userName,
            authKey is None and self.authentication_key or authKey,
            privKey is None and self.privacy_key or privKey,
            authProtocol is None and self.authentication_protocol or authProtocol,
            privProtocol is None and self.privacy_protocol or privProtocol,
            securityEngineId is None and self.securityEngineId or securityEngineId,
            securityName is None and self.securityName or securityName,
            authKeyType is None and self.authKeyType or USM_KEY_TYPE_PASSPHRASE,
            privKeyType is None and self.privKeyType or USM_KEY_TYPE_PASSPHRASE,
        )

    # Compatibility API
    deprecated_attributes = {
        "authKey": "authentication_key",
        "privKey": "privacy_key",
        "authProtocol": "authentication_protocol",
        "privProtocol": "privacy_protocol",
    }

    def __getattr__(self, attr: str):
        """Handle deprecated attributes."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return getattr(self, new_attr)

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{attr}'"
        )


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
    "usmHMACMD5AuthProtocol": "USM_AUTH_HMAC96_MD5",
    "usmHMACSHAAuthProtocol": "USM_AUTH_HMAC96_SHA",
    "usmHMAC128SHA224AuthProtocol": "USM_AUTH_HMAC128_SHA224",
    "usmHMAC192SHA256AuthProtocol": "USM_AUTH_HMAC192_SHA256",
    "usmHMAC256SHA384AuthProtocol": "USM_AUTH_HMAC256_SHA384",
    "usmHMAC384SHA512AuthProtocol": "USM_AUTH_HMAC384_SHA512",
    "usmNoAuthProtocol": "USM_AUTH_NONE",
    "usmDESPrivProtocol": "USM_PRIV_CBC56_DES",
    "usm3DESEDEPrivProtocol": "USM_PRIV_CBC168_3DES",
    "usmAesCfb128Protocol": "USM_PRIV_CFB128_AES",
    "usmAesBlumenthalCfb192Protocol": "USM_PRIV_CFB192_AES_BLUMENTHAL",
    "usmAesBlumenthalCfb256Protocol": "USM_PRIV_CFB256_AES_BLUMENTHAL",
    "usmAesCfb192Protocol": "USM_PRIV_CFB192_AES",
    "usmAesCfb256Protocol": "USM_PRIV_CFB256_AES",
    "usmNoPrivProtocol": "USM_PRIV_NONE",
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


__all__.extend(
    [
        "usm3DESEDEPrivProtocol",
        "usmAesCfb128Protocol",
        "usmAesCfb192Protocol",
        "usmAesCfb256Protocol",
        "usmAesBlumenthalCfb192Protocol",
        "usmAesBlumenthalCfb256Protocol",
        "usmDESPrivProtocol",
        "usmHMACMD5AuthProtocol",
        "usmHMACSHAAuthProtocol",
        "usmHMAC128SHA224AuthProtocol",
        "usmHMAC192SHA256AuthProtocol",
        "usmHMAC256SHA384AuthProtocol",
        "usmHMAC384SHA512AuthProtocol",
        "usmNoAuthProtocol",
        "usmNoPrivProtocol",
        "usmKeyTypePassphrase",
        "usmKeyTypeMaster",
        "usmKeyTypeLocalized",
    ]
)
