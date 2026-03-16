#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import warnings


from pysnmp import debug, error
from pysnmp.carrier.asyncio.dgram import udp, udp6
from pysnmp.carrier.base import AbstractTransport
from pysnmp.entity.engine import SnmpEngine
from pysnmp.proto import rfc1902, rfc1905
from pysnmp.proto.secmod.eso.priv import aes192, aes256, des3
from pysnmp.proto.secmod.rfc3414.auth import hmacmd5, hmacsha, noauth
from pysnmp.proto.secmod.rfc3414.priv import des, nopriv
from pysnmp.proto.secmod.rfc3414.service import SnmpUSMSecurityModel
from pysnmp.proto.secmod.rfc3826.priv import aes
from pysnmp.proto.secmod.rfc7860.auth import hmacsha2

# Old to new attribute mapping
deprecated_attributes = {
    "snmpUDPDomain": "SNMP_UDP_DOMAIN",
    "snmpUDP6Domain": "SNMP_UDP6_DOMAIN",
    "snmpLocalDomain": "SNMP_LOCAL_DOMAIN",
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
    "authServices": "AUTH_SERVICES",
    "privServices": "PRIV_SERVICES",
    "addV1System": "add_v1_system",
    "delV1System": "delete_v1_system",
    "addV3User": "add_v3_user",
    "delV3User": "delete_v3_user",
    "addTransport": "add_transport",
    "delTransport": "delete_transport",
    "addTargetParams": "add_target_parameters",
    "delTargetParams": "delete_target_parameters",
    "addTargetAddr": "add_target_address",
    "delTargetAddr": "delete_target_address",
    "addVacmUser": "add_vacm_user",
    "delVacmUser": "delete_vacm_user",
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


# A shortcut to popular constants

# Transports
SNMP_UDP_DOMAIN = udp.SNMP_UDP_DOMAIN
SNMP_UDP6_DOMAIN = udp6.SNMP_UDP6_DOMAIN

# Auth protocol
USM_AUTH_HMAC96_MD5 = hmacmd5.HmacMd5.SERVICE_ID
USM_AUTH_HMAC96_SHA = hmacsha.HmacSha.SERVICE_ID
USM_AUTH_HMAC128_SHA224 = hmacsha2.HmacSha2.SHA224_SERVICE_ID
USM_AUTH_HMAC192_SHA256 = hmacsha2.HmacSha2.SHA256_SERVICE_ID
USM_AUTH_HMAC256_SHA384 = hmacsha2.HmacSha2.SAH384_SERVICE_ID
USM_AUTH_HMAC384_SHA512 = hmacsha2.HmacSha2.SHA512_SERVICE_ID

USM_AUTH_NONE = noauth.NoAuth.SERVICE_ID
"""No authentication service"""

# Privacy protocol
USM_PRIV_CBC56_DES = des.Des.SERVICE_ID
USM_PRIV_CBC168_3DES = des3.Des3.SERVICE_ID
USM_PRIV_CFB128_AES = aes.Aes.SERVICE_ID
USM_PRIV_CFB192_AES_BLUMENTHAL = (
    aes192.AesBlumenthal192.SERVICE_ID
)  # semi-standard but not widely used
USM_PRIV_CFB256_AES_BLUMENTHAL = (
    aes256.AesBlumenthal256.SERVICE_ID
)  # semi-standard but not widely used
USM_PRIV_CFB192_AES = aes192.Aes192.SERVICE_ID  # non-standard but used by many vendors
USM_PRIV_CFB256_AES = aes256.Aes256.SERVICE_ID  # non-standard but used by many vendors
USM_PRIV_NONE = nopriv.NoPriv.SERVICE_ID

# USM key types (PYSNMP-USM-MIB::pysnmpUsmKeyType)
USM_KEY_TYPE_PASSPHRASE = 0
USM_KEY_TYPE_MASTER = 1
USM_KEY_TYPE_LOCALIZED = 2

# Auth services
AUTH_SERVICES = SnmpUSMSecurityModel.AUTH_SERVICES

# Privacy services
PRIV_SERVICES = SnmpUSMSecurityModel.PRIV_SERVICES


def __cook_v1_system_info(snmpEngine: SnmpEngine, communityIndex):
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpEngineID,) = mibBuilder.import_symbols("__SNMP-FRAMEWORK-MIB", "snmpEngineID")  # type: ignore
    (snmpCommunityEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-COMMUNITY-MIB", "snmpCommunityEntry"
    )
    tblIdx = snmpCommunityEntry.getInstIdFromIndices(communityIndex)
    return snmpCommunityEntry, tblIdx, snmpEngineID


def add_v1_system(
    snmpEngine: SnmpEngine,
    communityIndex: str,
    communityName: str,
    contextEngineId=None,
    contextName=None,
    transportTag=None,
    securityName=None,
):
    """Register SNMPv1 and v2c community."""
    (snmpCommunityEntry, tblIdx, snmpEngineID) = __cook_v1_system_info(
        snmpEngine, communityIndex
    )

    if contextEngineId is None:
        contextEngineId = snmpEngineID.syntax
    else:
        contextEngineId = snmpEngineID.syntax.clone(contextEngineId)

    if contextName is None:
        contextName = b""

    securityName = securityName is not None and securityName or communityIndex

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpCommunityEntry.name + (8,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpCommunityEntry.name + (1,) + tblIdx, communityIndex),
        (snmpCommunityEntry.name + (2,) + tblIdx, communityName),
        (
            snmpCommunityEntry.name + (3,) + tblIdx,
            securityName is not None and securityName or communityIndex,
        ),
        (snmpCommunityEntry.name + (4,) + tblIdx, contextEngineId),
        (snmpCommunityEntry.name + (5,) + tblIdx, contextName),
        (snmpCommunityEntry.name + (6,) + tblIdx, transportTag),
        (snmpCommunityEntry.name + (7,) + tblIdx, "nonVolatile"),
        (snmpCommunityEntry.name + (8,) + tblIdx, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )

    debug.logger & debug.FLAG_SM and debug.logger(
        "addV1System: added new table entry "
        'communityIndex "%s" communityName "%s" securityName "%s" '
        'contextEngineId "%s" contextName "%s" transportTag '
        '"%s"'
        % (
            communityIndex,
            communityName,
            securityName,
            contextEngineId,
            contextName,
            transportTag,
        )
    )


def delete_v1_system(snmpEngine: SnmpEngine, communityIndex):
    """Unregister SNMPv1 and v2c community."""
    (snmpCommunityEntry, tblIdx, snmpEngineID) = __cook_v1_system_info(
        snmpEngine, communityIndex
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpCommunityEntry.name + (8,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )

    debug.logger & debug.FLAG_SM and debug.logger(
        "delV1System: deleted table entry by communityIndex " '"%s"' % (communityIndex,)
    )


def __cook_v3_user_info(snmpEngine: SnmpEngine, securityName, securityEngineId):
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpEngineID,) = mibBuilder.import_symbols("__SNMP-FRAMEWORK-MIB", "snmpEngineID")  # type: ignore

    if securityEngineId is None:
        securityEngineId = snmpEngineID.syntax
    else:
        securityEngineId = snmpEngineID.syntax.clone(securityEngineId)

    (usmUserEntry,) = mibBuilder.import_symbols("SNMP-USER-BASED-SM-MIB", "usmUserEntry")  # type: ignore
    tblIdx1 = usmUserEntry.getInstIdFromIndices(securityEngineId, securityName)

    (pysnmpUsmSecretEntry,) = mibBuilder.import_symbols(  # type: ignore
        "PYSNMP-USM-MIB", "pysnmpUsmSecretEntry"
    )
    tblIdx2 = pysnmpUsmSecretEntry.getInstIdFromIndices(securityName)

    return securityEngineId, usmUserEntry, tblIdx1, pysnmpUsmSecretEntry, tblIdx2


def add_v3_user(
    snmpEngine: SnmpEngine,
    userName,
    authProtocol=USM_AUTH_NONE,
    authKey=None,
    privProtocol=USM_PRIV_NONE,
    privKey=None,
    securityEngineId=None,
    securityName=None,
    authKeyType=USM_KEY_TYPE_PASSPHRASE,
    privKeyType=USM_KEY_TYPE_PASSPHRASE,
):
    """Register SNMPv3 user."""
    mibBuilder = snmpEngine.get_mib_builder()

    if securityName is None:
        securityName = userName

    (
        securityEngineId,
        usmUserEntry,
        tblIdx1,
        pysnmpUsmSecretEntry,
        tblIdx2,
    ) = __cook_v3_user_info(snmpEngine, securityName, securityEngineId)

    # Load augmenting table before creating new row in base one
    (pysnmpUsmKeyEntry,) = mibBuilder.import_symbols(  # type: ignore
        "PYSNMP-USM-MIB", "pysnmpUsmKeyEntry"
    )

    # Load clone-from (may not be needed)
    (zeroDotZero,) = mibBuilder.import_symbols("SNMPv2-SMI", "zeroDotZero")  # type: ignore

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (usmUserEntry.name + (13,) + tblIdx1, "destroy"), **dict(snmpEngine=snmpEngine)
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (usmUserEntry.name + (2,) + tblIdx1, userName),
        (usmUserEntry.name + (3,) + tblIdx1, securityName),
        (usmUserEntry.name + (4,) + tblIdx1, zeroDotZero.name),
        (usmUserEntry.name + (5,) + tblIdx1, authProtocol),
        (usmUserEntry.name + (8,) + tblIdx1, privProtocol),
        (usmUserEntry.name + (13,) + tblIdx1, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )

    if authProtocol not in AUTH_SERVICES:
        raise error.PySnmpError(f"Unknown auth protocol {authProtocol}")

    if privProtocol not in PRIV_SERVICES:
        raise error.PySnmpError(f"Unknown privacy protocol {privProtocol}")

    (pysnmpUsmKeyType,) = mibBuilder.import_symbols(  # type: ignore
        "__PYSNMP-USM-MIB", "pysnmpUsmKeyType"
    )

    authKeyType = pysnmpUsmKeyType.syntax.clone(authKeyType)

    # Localize authentication key unless given

    authKey = authKey and rfc1902.OctetString(authKey)

    masterAuthKey = localAuthKey = authKey

    if authKeyType < USM_KEY_TYPE_MASTER:  # pass phrase is given
        masterAuthKey = AUTH_SERVICES[authProtocol].hash_passphrase(authKey or b"")

    if authKeyType < USM_KEY_TYPE_LOCALIZED:  # pass phrase or master key is given
        localAuthKey = AUTH_SERVICES[authProtocol].localize_key(
            masterAuthKey, securityEngineId
        )

    # Localize privacy key unless given

    privKeyType = pysnmpUsmKeyType.syntax.clone(privKeyType)

    privKey = privKey and rfc1902.OctetString(privKey)

    masterPrivKey = localPrivKey = privKey

    if privKeyType < USM_KEY_TYPE_MASTER:  # pass phrase is given
        masterPrivKey = PRIV_SERVICES[privProtocol].hash_passphrase(
            authProtocol, privKey or b""
        )

    if privKeyType < USM_KEY_TYPE_LOCALIZED:  # pass phrase or master key is given
        localPrivKey = PRIV_SERVICES[privProtocol].localize_key(
            authProtocol, masterPrivKey, securityEngineId
        )

    # Commit only the keys we have

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (pysnmpUsmKeyEntry.name + (1,) + tblIdx1, localAuthKey),
        (pysnmpUsmKeyEntry.name + (2,) + tblIdx1, localPrivKey),
        **dict(snmpEngine=snmpEngine),
    )

    if authKeyType < USM_KEY_TYPE_LOCALIZED:
        snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
            (pysnmpUsmKeyEntry.name + (3,) + tblIdx1, masterAuthKey),
            **dict(snmpEngine=snmpEngine),
        )

    if privKeyType < USM_KEY_TYPE_LOCALIZED:
        snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
            (pysnmpUsmKeyEntry.name + (4,) + tblIdx1, masterPrivKey),
            **dict(snmpEngine=snmpEngine),
        )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (pysnmpUsmSecretEntry.name + (4,) + tblIdx2, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )

    # Commit plain-text pass-phrases if we have them

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (pysnmpUsmSecretEntry.name + (4,) + tblIdx2, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )

    if authKeyType < USM_KEY_TYPE_MASTER:
        snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
            (pysnmpUsmSecretEntry.name + (1,) + tblIdx2, userName),
            (pysnmpUsmSecretEntry.name + (2,) + tblIdx2, authKey),
            **dict(snmpEngine=snmpEngine),
        )

    if privKeyType < USM_KEY_TYPE_MASTER:
        snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
            (pysnmpUsmSecretEntry.name + (1,) + tblIdx2, userName),
            (pysnmpUsmSecretEntry.name + (3,) + tblIdx2, privKey),
            **dict(snmpEngine=snmpEngine),
        )

    debug.logger & debug.FLAG_SM and debug.logger(
        "addV3User: added new table entries "
        'userName "%s" securityName "%s" authProtocol %s '
        'privProtocol %s localAuthKey "%s" localPrivKey "%s" '
        'masterAuthKey "%s" masterPrivKey "%s" authKey "%s" '
        'privKey "%s" by index securityName "%s" securityEngineId '
        '"%s"'
        % (
            userName,
            securityName,
            authProtocol,
            privProtocol,
            localAuthKey and localAuthKey.prettyPrint(),
            localPrivKey and localPrivKey.prettyPrint(),
            masterAuthKey and masterAuthKey.prettyPrint(),
            masterPrivKey and masterPrivKey.prettyPrint(),
            authKey and authKey.prettyPrint(),
            privKey and privKey.prettyPrint(),
            securityName,
            securityEngineId.prettyPrint(),
        )
    )


def delete_v3_user(
    snmpEngine: SnmpEngine,
    userName,
    securityEngineId=None,
):
    """Unregister SNMPv3 user."""
    (
        securityEngineId,
        usmUserEntry,
        tblIdx1,
        pysnmpUsmSecretEntry,
        tblIdx2,
    ) = __cook_v3_user_info(snmpEngine, userName, securityEngineId)

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (usmUserEntry.name + (13,) + tblIdx1, "destroy"), **dict(snmpEngine=snmpEngine)
    )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (pysnmpUsmSecretEntry.name + (4,) + tblIdx2, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )

    debug.logger & debug.FLAG_SM and debug.logger(
        "delV3User: deleted table entries by index "
        'userName "%s" securityEngineId '
        '"%s"' % (userName, securityEngineId.prettyPrint())
    )

    # Drop all derived rows
    varBinds = initialVarBinds = (
        (usmUserEntry.name + (1,), None),  # usmUserEngineID
        (usmUserEntry.name + (2,), None),  # usmUserName
        (usmUserEntry.name + (4,), None),  # usmUserCloneFrom
    )

    while varBinds:
        varBinds = (
            snmpEngine.message_dispatcher.mib_instrum_controller.read_next_variables(
                *varBinds, **dict(snmpEngine=snmpEngine)
            )
        )
        if varBinds[0][1].isSameTypeWith(rfc1905.endOfMibView):
            break
        if varBinds[0][0][: len(initialVarBinds[0][0])] != initialVarBinds[0][0]:
            break
        elif varBinds[2][1] == tblIdx1:  # cloned from this entry
            delete_v3_user(snmpEngine, varBinds[1][1], varBinds[0][1])
            varBinds = initialVarBinds


def __cook_target_parameters_info(snmpEngine: SnmpEngine, name):
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpTargetParamsEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-TARGET-MIB", "snmpTargetParamsEntry"
    )
    tblIdx = snmpTargetParamsEntry.getInstIdFromIndices(name)
    return snmpTargetParamsEntry, tblIdx


# mpModel: 0 == SNMPv1, 1 == SNMPv2c, 3 == SNMPv3
def add_target_parameters(
    snmpEngine: SnmpEngine, name, securityName, securityLevel, mpModel=3
):
    """Register target parameters."""
    if mpModel == 0:
        securityModel = 1
    elif mpModel in (1, 2):
        securityModel = 2
    elif mpModel == 3:
        securityModel = 3
    else:
        raise error.PySnmpError("Unknown MP model %s" % mpModel)

    snmpTargetParamsEntry, tblIdx = __cook_target_parameters_info(snmpEngine, name)

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpTargetParamsEntry.name + (7,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpTargetParamsEntry.name + (1,) + tblIdx, name),
        (snmpTargetParamsEntry.name + (2,) + tblIdx, mpModel),
        (snmpTargetParamsEntry.name + (3,) + tblIdx, securityModel),
        (snmpTargetParamsEntry.name + (4,) + tblIdx, securityName),
        (snmpTargetParamsEntry.name + (5,) + tblIdx, securityLevel),
        (snmpTargetParamsEntry.name + (7,) + tblIdx, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )


def delete_target_parameters(snmpEngine: SnmpEngine, name: str):
    """Delete target parameters."""
    snmpTargetParamsEntry, tblIdx = __cook_target_parameters_info(snmpEngine, name)
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpTargetParamsEntry.name + (7,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )


def __cook_target_address_info(snmpEngine: SnmpEngine, addrName: str):
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpTargetAddrEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-TARGET-MIB", "snmpTargetAddrEntry"
    )
    (snmpSourceAddrEntry,) = mibBuilder.import_symbols(  # type: ignore
        "PYSNMP-SOURCE-MIB", "snmpSourceAddrEntry"
    )
    tblIdx = snmpTargetAddrEntry.getInstIdFromIndices(addrName)
    return snmpTargetAddrEntry, snmpSourceAddrEntry, tblIdx


def add_target_address(
    snmpEngine: SnmpEngine,
    addrName: str,
    transportDomain: "tuple[int, ...]",
    transportAddress: "tuple[str, int]",
    params: str,
    timeout: "float | None" = None,
    retryCount: "int | None" = None,
    tagList=b"",
    sourceAddress=None,
):
    """Register target address."""
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpTargetAddrEntry, snmpSourceAddrEntry, tblIdx) = __cook_target_address_info(
        snmpEngine, addrName
    )

    if transportDomain[: len(SNMP_UDP_DOMAIN)] == SNMP_UDP_DOMAIN:
        (SnmpUDPAddress,) = mibBuilder.import_symbols("SNMPv2-TM", "SnmpUDPAddress")  # type: ignore
        transportAddress = SnmpUDPAddress(transportAddress)
        if sourceAddress is None:
            sourceAddress = ("0.0.0.0", 0)
        sourceAddress = SnmpUDPAddress(sourceAddress)
    elif transportDomain[: len(SNMP_UDP6_DOMAIN)] == SNMP_UDP6_DOMAIN:
        (TransportAddressIPv6,) = mibBuilder.import_symbols(  # type: ignore
            "TRANSPORT-ADDRESS-MIB", "TransportAddressIPv6"
        )
        transportAddress = TransportAddressIPv6(transportAddress)
        if sourceAddress is None:
            sourceAddress = ("::", 0)
        sourceAddress = TransportAddressIPv6(sourceAddress)

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpTargetAddrEntry.name + (9,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpTargetAddrEntry.name + (1,) + tblIdx, addrName),
        (snmpTargetAddrEntry.name + (2,) + tblIdx, transportDomain),
        (snmpTargetAddrEntry.name + (3,) + tblIdx, transportAddress),
        (snmpTargetAddrEntry.name + (4,) + tblIdx, timeout),
        (snmpTargetAddrEntry.name + (5,) + tblIdx, retryCount),
        (snmpTargetAddrEntry.name + (6,) + tblIdx, tagList),
        (snmpTargetAddrEntry.name + (7,) + tblIdx, params),
        (snmpSourceAddrEntry.name + (1,) + tblIdx, sourceAddress),
        (snmpTargetAddrEntry.name + (9,) + tblIdx, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )


def delete_target_address(snmpEngine: SnmpEngine, addrName: str):
    """Delete target address."""
    (snmpTargetAddrEntry, snmpSourceAddrEntry, tblIdx) = __cook_target_address_info(
        snmpEngine, addrName
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpTargetAddrEntry.name + (9,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )


def add_transport(
    snmpEngine: SnmpEngine,
    transportDomain: "tuple[int, ...]",
    transport: AbstractTransport,
):
    """Register transport with dispatcher."""
    if snmpEngine.transport_dispatcher:
        if not transport.is_compatible_with_dispatcher(snmpEngine.transport_dispatcher):
            raise error.PySnmpError(
                f"Transport {transport!r} is not compatible with dispatcher {snmpEngine.transport_dispatcher!r}"
            )
    else:
        protoTransportDispatcher = transport.PROTO_TRANSPORT_DISPATCHER
        if protoTransportDispatcher is not None:
            snmpEngine.register_transport_dispatcher(protoTransportDispatcher())
            # here we note that we have created transportDispatcher automatically
            snmpEngine.set_user_context(automaticTransportDispatcher=0)

    if snmpEngine.transport_dispatcher:
        snmpEngine.transport_dispatcher.register_transport(transportDomain, transport)
        automaticTransportDispatcher = snmpEngine.get_user_context(
            "automaticTransportDispatcher"
        )
        if automaticTransportDispatcher is not None:
            snmpEngine.set_user_context(
                automaticTransportDispatcher=automaticTransportDispatcher + 1
            )


def get_transport(snmpEngine: SnmpEngine, transportDomain: "tuple[int, ...]"):
    """Return transport from dispatcher."""
    if not snmpEngine.transport_dispatcher:
        return
    try:
        return snmpEngine.transport_dispatcher.get_transport(transportDomain)
    except error.PySnmpError:
        return


def delete_transport(snmpEngine: SnmpEngine, transportDomain: "tuple[int, ...]"):
    """Remove transport from dispatcher."""
    if not snmpEngine.transport_dispatcher:
        return
    transport = get_transport(snmpEngine, transportDomain)
    snmpEngine.transport_dispatcher.unregister_transport(transportDomain)
    # automatically shutdown automatically created transportDispatcher
    automaticTransportDispatcher = snmpEngine.get_user_context(
        "automaticTransportDispatcher"
    )
    if automaticTransportDispatcher is not None:
        automaticTransportDispatcher -= 1
        snmpEngine.set_user_context(
            automaticTransportDispatcher=automaticTransportDispatcher
        )
        if not automaticTransportDispatcher:
            snmpEngine.close_dispatcher()
            snmpEngine.delete_user_context(automaticTransportDispatcher)
    return transport


addSocketTransport = add_transport  # noqa: N816
delSocketTransport = delete_transport  # noqa: N816


# VACM shortcuts


def __cook_vacm_context_info(snmpEngine: SnmpEngine, contextName):
    mibBuilder = snmpEngine.get_mib_builder()
    (vacmContextEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-VIEW-BASED-ACM-MIB", "vacmContextEntry"
    )
    tblIdx = vacmContextEntry.getInstIdFromIndices(contextName)
    return vacmContextEntry, tblIdx


def add_context(snmpEngine: SnmpEngine, contextName):
    """Setup VACM context."""
    vacmContextEntry, tblIdx = __cook_vacm_context_info(snmpEngine, contextName)

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmContextEntry.name + (2,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmContextEntry.name + (1,) + tblIdx, contextName),
        (vacmContextEntry.name + (2,) + tblIdx, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )


def delete_context(snmpEngine: SnmpEngine, contextName):
    """Delete VACM context."""
    vacmContextEntry, tblIdx = __cook_vacm_context_info(snmpEngine, contextName)

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmContextEntry.name + (2,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )


def __cook_vacm_group_info(snmpEngine: SnmpEngine, securityModel, securityName):
    mibBuilder = snmpEngine.get_mib_builder()

    (vacmSecurityToGroupEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-VIEW-BASED-ACM-MIB", "vacmSecurityToGroupEntry"
    )
    tblIdx = vacmSecurityToGroupEntry.getInstIdFromIndices(securityModel, securityName)
    return vacmSecurityToGroupEntry, tblIdx


def add_vacm_group(snmpEngine: SnmpEngine, groupName, securityModel, securityName):
    """Setup VACM group."""
    (vacmSecurityToGroupEntry, tblIdx) = __cook_vacm_group_info(
        snmpEngine, securityModel, securityName
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmSecurityToGroupEntry.name + (5,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmSecurityToGroupEntry.name + (1,) + tblIdx, securityModel),
        (vacmSecurityToGroupEntry.name + (2,) + tblIdx, securityName),
        (vacmSecurityToGroupEntry.name + (3,) + tblIdx, groupName),
        (vacmSecurityToGroupEntry.name + (5,) + tblIdx, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )


def delete_vacm_group(snmpEngine: SnmpEngine, securityModel, securityName):
    """Delete VACM group."""
    vacmSecurityToGroupEntry, tblIdx = __cook_vacm_group_info(
        snmpEngine, securityModel, securityName
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmSecurityToGroupEntry.name + (5,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )


def __cook_vacm_access_info(
    snmpEngine: SnmpEngine, groupName, contextName, securityModel, securityLevel
):
    mibBuilder = snmpEngine.get_mib_builder()

    (vacmAccessEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-VIEW-BASED-ACM-MIB", "vacmAccessEntry"
    )
    tblIdx = vacmAccessEntry.getInstIdFromIndices(
        groupName, contextName, securityModel, securityLevel
    )
    return vacmAccessEntry, tblIdx


def add_vacm_access(
    snmpEngine: SnmpEngine,
    groupName,
    contextPrefix,
    securityModel,
    securityLevel,
    contextMatch,
    readView,
    writeView,
    notifyView,
):
    """Setup VACM access."""
    vacmAccessEntry, tblIdx = __cook_vacm_access_info(
        snmpEngine, groupName, contextPrefix, securityModel, securityLevel
    )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmAccessEntry.name + (9,) + tblIdx, "destroy"), **dict(snmpEngine=snmpEngine)
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmAccessEntry.name + (1,) + tblIdx, contextPrefix),
        (vacmAccessEntry.name + (2,) + tblIdx, securityModel),
        (vacmAccessEntry.name + (3,) + tblIdx, securityLevel),
        (vacmAccessEntry.name + (4,) + tblIdx, contextMatch),
        (vacmAccessEntry.name + (5,) + tblIdx, readView),
        (vacmAccessEntry.name + (6,) + tblIdx, writeView),
        (vacmAccessEntry.name + (7,) + tblIdx, notifyView),
        (vacmAccessEntry.name + (9,) + tblIdx, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )


def delete_vacm_access(
    snmpEngine: SnmpEngine, groupName, contextPrefix, securityModel, securityLevel
):
    """Delete VACM access."""
    vacmAccessEntry, tblIdx = __cook_vacm_access_info(
        snmpEngine, groupName, contextPrefix, securityModel, securityLevel
    )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmAccessEntry.name + (9,) + tblIdx, "destroy"), **dict(snmpEngine=snmpEngine)
    )


def __cook_vacm_view_info(snmpEngine: SnmpEngine, viewName, subTree):
    mibBuilder = snmpEngine.get_mib_builder()

    (vacmViewTreeFamilyEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-VIEW-BASED-ACM-MIB", "vacmViewTreeFamilyEntry"
    )
    tblIdx = vacmViewTreeFamilyEntry.getInstIdFromIndices(viewName, subTree)
    return vacmViewTreeFamilyEntry, tblIdx


def add_vacm_view(snmpEngine: SnmpEngine, viewName, viewType, subTree, subTreeMask):
    """Setup VACM view."""
    vacmViewTreeFamilyEntry, tblIdx = __cook_vacm_view_info(
        snmpEngine, viewName, subTree
    )

    # Allow bitmask specification in form of an OID
    if rfc1902.OctetString(".").asOctets() in rfc1902.OctetString(subTreeMask):
        subTreeMask = rfc1902.ObjectIdentifier(subTreeMask)

    if isinstance(subTreeMask, rfc1902.ObjectIdentifier):
        subTreeMask = tuple(subTreeMask)
        if len(subTreeMask) < len(subTree):
            subTreeMask += (1,) * (len(subTree) - len(subTreeMask))

        subTreeMask = rfc1902.OctetString.fromBinaryString(
            "".join(str(x) for x in subTreeMask)
        )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmViewTreeFamilyEntry.name + (6,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmViewTreeFamilyEntry.name + (1,) + tblIdx, viewName),
        (vacmViewTreeFamilyEntry.name + (2,) + tblIdx, subTree),
        (vacmViewTreeFamilyEntry.name + (3,) + tblIdx, subTreeMask),
        (vacmViewTreeFamilyEntry.name + (4,) + tblIdx, viewType),
        (vacmViewTreeFamilyEntry.name + (6,) + tblIdx, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )


def delete_vacm_view(snmpEngine: SnmpEngine, viewName, subTree):
    """Delete VACM view."""
    vacmViewTreeFamilyEntry, tblIdx = __cook_vacm_view_info(
        snmpEngine, viewName, subTree
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (vacmViewTreeFamilyEntry.name + (6,) + tblIdx, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )


# VACM simplicity wrappers


def __cook_vacm_user_info(
    snmpEngine: SnmpEngine, securityModel, securityName, securityLevel
):
    mibBuilder = snmpEngine.get_mib_builder()

    groupName = "v-%s-%d" % (hash(securityName), securityModel)
    (SnmpSecurityLevel,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-FRAMEWORK-MIB", "SnmpSecurityLevel"
    )
    securityLevel = SnmpSecurityLevel(securityLevel)
    return (groupName, securityLevel, "r" + groupName, "w" + groupName, "n" + groupName)


def add_vacm_user(
    snmpEngine: SnmpEngine,
    securityModel: int,
    securityName: str,
    securityLevel: str,
    readSubTree=(),
    writeSubTree=(),
    notifySubTree=(),
    contextName=b"",
):
    """Setup VACM user."""
    (groupName, securityLevel, readView, writeView, notifyView) = __cook_vacm_user_info(
        snmpEngine, securityModel, securityName, securityLevel
    )
    add_context(snmpEngine, contextName)
    add_vacm_group(snmpEngine, groupName, securityModel, securityName)
    add_vacm_access(
        snmpEngine,
        groupName,
        contextName,
        securityModel,
        securityLevel,
        "exact",
        readView,
        writeView,
        notifyView,
    )
    if readSubTree:
        add_vacm_view(snmpEngine, readView, "included", readSubTree, b"")
    if writeSubTree:
        add_vacm_view(snmpEngine, writeView, "included", writeSubTree, b"")
    if notifySubTree:
        add_vacm_view(snmpEngine, notifyView, "included", notifySubTree, b"")


def delete_vacm_user(
    snmpEngine: SnmpEngine,
    securityModel,
    securityName,
    securityLevel,
    readSubTree=(),
    writeSubTree=(),
    notifySubTree=(),
    contextName=b"",
):
    """Delete VACM user."""
    (groupName, securityLevel, readView, writeView, notifyView) = __cook_vacm_user_info(
        snmpEngine, securityModel, securityName, securityLevel
    )
    delete_context(snmpEngine, contextName)
    delete_vacm_group(snmpEngine, securityModel, securityName)
    delete_vacm_access(snmpEngine, groupName, contextName, securityModel, securityLevel)
    if readSubTree:
        delete_vacm_view(snmpEngine, readView, readSubTree)
    if writeSubTree:
        delete_vacm_view(snmpEngine, writeView, writeSubTree)
    if notifySubTree:
        delete_vacm_view(snmpEngine, notifyView, notifySubTree)


# Notification target setup


def __cook_notification_target_info(
    snmpEngine: SnmpEngine, notificationName, paramsName, filterSubtree=None
):
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpNotifyEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-NOTIFICATION-MIB", "snmpNotifyEntry"
    )
    tblIdx1 = snmpNotifyEntry.getInstIdFromIndices(notificationName)

    (snmpNotifyFilterProfileEntry,) = mibBuilder.import_symbols(
        "SNMP-NOTIFICATION-MIB", "snmpNotifyFilterProfileEntry"
    )
    tblIdx2 = snmpNotifyFilterProfileEntry.getInstIdFromIndices(paramsName)

    profileName = "%s-filter" % hash(notificationName)

    if filterSubtree:
        (snmpNotifyFilterEntry,) = mibBuilder.import_symbols(
            "SNMP-NOTIFICATION-MIB", "snmpNotifyFilterEntry"
        )
        tblIdx3 = snmpNotifyFilterEntry.getInstIdFromIndices(profileName, filterSubtree)
    else:
        snmpNotifyFilterEntry = tblIdx3 = None

    return (
        snmpNotifyEntry,
        tblIdx1,
        snmpNotifyFilterProfileEntry,
        tblIdx2,
        profileName,
        snmpNotifyFilterEntry,
        tblIdx3,
    )


def add_notification_target(
    snmpEngine: SnmpEngine,
    notificationName,
    paramsName,
    transportTag,
    notifyType=None,
    filterSubtree=None,
    filterMask=None,
    filterType=None,
):
    """Setup notification target."""
    (
        snmpNotifyEntry,
        tblIdx1,
        snmpNotifyFilterProfileEntry,
        tblIdx2,
        profileName,
        snmpNotifyFilterEntry,
        tblIdx3,
    ) = __cook_notification_target_info(
        snmpEngine, notificationName, paramsName, filterSubtree
    )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyEntry.name + (5,) + tblIdx1, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyEntry.name + (2,) + tblIdx1, transportTag),
        (snmpNotifyEntry.name + (3,) + tblIdx1, notifyType),
        (snmpNotifyEntry.name + (5,) + tblIdx1, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyFilterProfileEntry.name + (3,) + tblIdx2, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyFilterProfileEntry.name + (1,) + tblIdx2, profileName),
        (snmpNotifyFilterProfileEntry.name + (3,) + tblIdx2, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )

    if not snmpNotifyFilterEntry:
        return

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyFilterEntry.name + (5,) + tblIdx3, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )
    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyFilterEntry.name + (1,) + tblIdx3, filterSubtree),
        (snmpNotifyFilterEntry.name + (2,) + tblIdx3, filterMask),
        (snmpNotifyFilterEntry.name + (3,) + tblIdx3, filterType),
        (snmpNotifyFilterEntry.name + (5,) + tblIdx3, "createAndGo"),
        **dict(snmpEngine=snmpEngine),
    )


def delete_notification_target(
    snmpEngine: SnmpEngine, notificationName, paramsName, filterSubtree=None
):
    """Remove notification target."""
    (
        snmpNotifyEntry,
        tblIdx1,
        snmpNotifyFilterProfileEntry,
        tblIdx2,
        profileName,
        snmpNotifyFilterEntry,
        tblIdx3,
    ) = __cook_notification_target_info(
        snmpEngine, notificationName, paramsName, filterSubtree
    )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyEntry.name + (5,) + tblIdx1, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyFilterProfileEntry.name + (3,) + tblIdx2, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )

    if not snmpNotifyFilterEntry:
        return

    snmpEngine.message_dispatcher.mib_instrum_controller.write_variables(
        (snmpNotifyFilterEntry.name + (5,) + tblIdx3, "destroy"),
        **dict(snmpEngine=snmpEngine),
    )


# rfc3415: A.1
def set_initial_vacm_parameters(snmpEngine: SnmpEngine):
    """Set initial VACM parameters as per SNMP-FRAMEWORK-MIB."""
    # rfc3415: A.1.1 --> initial-semi-security-configuration

    # rfc3415: A.1.2
    add_context(snmpEngine, "")

    # rfc3415: A.1.3
    add_vacm_group(snmpEngine, "initial", 3, "initial")

    # rfc3415: A.1.4
    add_vacm_access(
        snmpEngine,
        "initial",
        "",
        3,
        "noAuthNoPriv",
        "exact",
        "restricted",
        None,
        "restricted",
    )
    add_vacm_access(
        snmpEngine,
        "initial",
        "",
        3,
        "authNoPriv",
        "exact",
        "internet",
        "internet",
        "internet",
    )
    add_vacm_access(
        snmpEngine,
        "initial",
        "",
        3,
        "authPriv",
        "exact",
        "internet",
        "internet",
        "internet",
    )

    # rfc3415: A.1.5 (semi-secure)
    add_vacm_view(snmpEngine, "internet", "included", (1, 3, 6, 1), "")
    add_vacm_view(snmpEngine, "restricted", "included", (1, 3, 6, 1, 2, 1, 1), "")
    add_vacm_view(snmpEngine, "restricted", "included", (1, 3, 6, 1, 2, 1, 11), "")
    add_vacm_view(
        snmpEngine, "restricted", "included", (1, 3, 6, 1, 6, 3, 10, 2, 1), ""
    )
    add_vacm_view(
        snmpEngine, "restricted", "included", (1, 3, 6, 1, 6, 3, 11, 2, 1), ""
    )
    add_vacm_view(
        snmpEngine, "restricted", "included", (1, 3, 6, 1, 6, 3, 15, 1, 1), ""
    )
