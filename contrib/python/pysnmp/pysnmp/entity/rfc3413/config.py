#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import Any

from pysnmp.entity import config
from pysnmp.entity.engine import SnmpEngine
from pysnmp.error import PySnmpError
from pysnmp.smi.error import NoSuchInstanceError, SmiError


def get_target_address(snmpEngine: SnmpEngine, snmpTargetAddrName):
    """Return transport endpoint information for a given target."""
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpTargetAddrEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-TARGET-MIB", "snmpTargetAddrEntry"
    )

    cache: "dict[str, Any] | None" = snmpEngine.get_user_context("getTargetAddr")
    if cache is None:
        cache = {"id": -1}
        snmpEngine.set_user_context(getTargetAddr=cache)

    if cache["id"] != snmpTargetAddrEntry.branchVersionId:
        cache["nameToTargetMap"] = {}

    nameToTargetMap = cache["nameToTargetMap"]

    if snmpTargetAddrName not in nameToTargetMap:
        (
            snmpTargetAddrTDomain,
            snmpTargetAddrTAddress,
            snmpTargetAddrTimeout,
            snmpTargetAddrRetryCount,
            snmpTargetAddrParams,
        ) = mibBuilder.import_symbols(  # type: ignore
            "SNMP-TARGET-MIB",
            "snmpTargetAddrTDomain",
            "snmpTargetAddrTAddress",
            "snmpTargetAddrTimeout",
            "snmpTargetAddrRetryCount",
            "snmpTargetAddrParams",
        )
        (snmpSourceAddrTAddress,) = mibBuilder.import_symbols("PYSNMP-SOURCE-MIB", "snmpSourceAddrTAddress")  # type: ignore

        tblIdx = snmpTargetAddrEntry.getInstIdFromIndices(snmpTargetAddrName)

        try:
            snmpTargetAddrTDomain = snmpTargetAddrTDomain.getNode(
                snmpTargetAddrTDomain.name + tblIdx
            ).syntax
            snmpTargetAddrTAddress = snmpTargetAddrTAddress.getNode(
                snmpTargetAddrTAddress.name + tblIdx
            ).syntax
            snmpTargetAddrTimeout = snmpTargetAddrTimeout.getNode(
                snmpTargetAddrTimeout.name + tblIdx
            ).syntax
            snmpTargetAddrRetryCount = snmpTargetAddrRetryCount.getNode(
                snmpTargetAddrRetryCount.name + tblIdx
            ).syntax
            snmpTargetAddrParams = snmpTargetAddrParams.getNode(
                snmpTargetAddrParams.name + tblIdx
            ).syntax
            snmpSourceAddrTAddress = snmpSourceAddrTAddress.getNode(
                snmpSourceAddrTAddress.name + tblIdx
            ).syntax
        except NoSuchInstanceError:
            raise SmiError("Target %s not configured to LCD" % snmpTargetAddrName)

        if snmpEngine.transport_dispatcher is None:
            raise PySnmpError("TransportDispatcher not set")
        transport = snmpEngine.transport_dispatcher.get_transport(snmpTargetAddrTDomain)

        if (
            snmpTargetAddrTDomain[: len(config.SNMP_UDP_DOMAIN)]
            == config.SNMP_UDP_DOMAIN
        ):
            (
                SnmpUDPAddress,
            ) = snmpEngine.get_mib_builder().import_symbols(  # type: ignore
                "SNMPv2-TM", "SnmpUDPAddress"
            )
            addr = transport.ADDRESS_TYPE(  # type: ignore
                SnmpUDPAddress(snmpTargetAddrTAddress)
            ).set_local_address(SnmpUDPAddress(snmpSourceAddrTAddress))
        elif (
            snmpTargetAddrTDomain[: len(config.SNMP_UDP6_DOMAIN)]
            == config.SNMP_UDP6_DOMAIN
        ):
            (TransportAddressIPv6,) = snmpEngine.get_mib_builder().import_symbols(  # type: ignore
                "TRANSPORT-ADDRESS-MIB", "TransportAddressIPv6"
            )
            addr = transport.ADDRESS_TYPE(  # type: ignore
                TransportAddressIPv6(snmpTargetAddrTAddress)
            ).set_local_address(TransportAddressIPv6(snmpSourceAddrTAddress))

        nameToTargetMap[snmpTargetAddrName] = (
            snmpTargetAddrTDomain,
            addr,
            snmpTargetAddrTimeout,
            snmpTargetAddrRetryCount,
            snmpTargetAddrParams,
        )

        cache["id"] = snmpTargetAddrEntry.branchVersionId

    return nameToTargetMap[snmpTargetAddrName]


def get_target_parameters(snmpEngine: SnmpEngine, paramsName):
    """Return security parameters for a given target."""
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpTargetParamsEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-TARGET-MIB", "snmpTargetParamsEntry"
    )

    cache: "dict[str, Any] | None" = snmpEngine.get_user_context("getTargetParams")
    if cache is None:
        cache = {"id": -1}
        snmpEngine.set_user_context(getTargetParams=cache)

    if cache["id"] != snmpTargetParamsEntry.branchVersionId:
        cache["nameToParamsMap"] = {}

    nameToParamsMap = cache["nameToParamsMap"]

    if paramsName not in nameToParamsMap:
        (
            snmpTargetParamsMPModel,
            snmpTargetParamsSecurityModel,
            snmpTargetParamsSecurityName,
            snmpTargetParamsSecurityLevel,
        ) = mibBuilder.import_symbols(  # type: ignore
            "SNMP-TARGET-MIB",
            "snmpTargetParamsMPModel",
            "snmpTargetParamsSecurityModel",
            "snmpTargetParamsSecurityName",
            "snmpTargetParamsSecurityLevel",
        )

        tblIdx = snmpTargetParamsEntry.getInstIdFromIndices(paramsName)

        try:
            snmpTargetParamsMPModel = snmpTargetParamsMPModel.getNode(
                snmpTargetParamsMPModel.name + tblIdx
            ).syntax
            snmpTargetParamsSecurityModel = snmpTargetParamsSecurityModel.getNode(
                snmpTargetParamsSecurityModel.name + tblIdx
            ).syntax
            snmpTargetParamsSecurityName = snmpTargetParamsSecurityName.getNode(
                snmpTargetParamsSecurityName.name + tblIdx
            ).syntax
            snmpTargetParamsSecurityLevel = snmpTargetParamsSecurityLevel.getNode(
                snmpTargetParamsSecurityLevel.name + tblIdx
            ).syntax
        except NoSuchInstanceError:
            raise SmiError("Parameters %s not configured at LCD" % paramsName)

        nameToParamsMap[paramsName] = (
            snmpTargetParamsMPModel,
            snmpTargetParamsSecurityModel,
            snmpTargetParamsSecurityName,
            snmpTargetParamsSecurityLevel,
        )

        cache["id"] = snmpTargetParamsEntry.branchVersionId

    return nameToParamsMap[paramsName]


def get_target_info(snmpEngine: SnmpEngine, snmpTargetAddrName):
    """Return transport endpoint and security parameters for a given target."""
    # Transport endpoint
    (
        snmpTargetAddrTDomain,
        snmpTargetAddrTAddress,
        snmpTargetAddrTimeout,
        snmpTargetAddrRetryCount,
        snmpTargetAddrParams,
    ) = get_target_address(snmpEngine, snmpTargetAddrName)

    (
        snmpTargetParamsMPModel,
        snmpTargetParamsSecurityModel,
        snmpTargetParamsSecurityName,
        snmpTargetParamsSecurityLevel,
    ) = get_target_parameters(snmpEngine, snmpTargetAddrParams)

    return (
        snmpTargetAddrTDomain,
        snmpTargetAddrTAddress,
        snmpTargetAddrTimeout,
        snmpTargetAddrRetryCount,
        snmpTargetParamsMPModel,
        snmpTargetParamsSecurityModel,
        snmpTargetParamsSecurityName,
        snmpTargetParamsSecurityLevel,
    )


def get_notification_info(snmpEngine: SnmpEngine, notificationTarget):
    """Return notification tag and type for a given target."""
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpNotifyEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-NOTIFICATION-MIB", "snmpNotifyEntry"
    )

    cache: "dict[str, Any] | None" = snmpEngine.get_user_context("getNotificationInfo")
    if cache is None:
        cache = {"id": -1}
        snmpEngine.set_user_context(getNotificationInfo=cache)

    if cache["id"] != snmpNotifyEntry.branchVersionId:
        cache["targetToNotifyMap"] = {}  # type: ignore

    targetToNotifyMap = cache["targetToNotifyMap"]

    if notificationTarget not in targetToNotifyMap:
        (snmpNotifyTag, snmpNotifyType) = mibBuilder.import_symbols(  # type: ignore
            "SNMP-NOTIFICATION-MIB", "snmpNotifyTag", "snmpNotifyType"
        )

        tblIdx = snmpNotifyEntry.getInstIdFromIndices(notificationTarget)

        try:
            snmpNotifyTag = snmpNotifyTag.getNode(snmpNotifyTag.name + tblIdx).syntax
            snmpNotifyType = snmpNotifyType.getNode(snmpNotifyType.name + tblIdx).syntax

        except NoSuchInstanceError:
            raise SmiError("Target %s not configured at LCD" % notificationTarget)

        targetToNotifyMap[notificationTarget] = (snmpNotifyTag, snmpNotifyType)

        cache["id"] = snmpNotifyEntry.branchVersionId

    return targetToNotifyMap[notificationTarget]


def get_target_names(snmpEngine: SnmpEngine, tag):
    """Return a list of target names associated with a given tag."""
    mibBuilder = snmpEngine.get_mib_builder()

    (snmpTargetAddrEntry,) = mibBuilder.import_symbols(  # type: ignore
        "SNMP-TARGET-MIB", "snmpTargetAddrEntry"
    )

    cache: "dict[str, Any] | None" = snmpEngine.get_user_context("getTargetNames")
    if cache is None:
        cache = {"id": -1}
        snmpEngine.set_user_context(getTargetNames=cache)

    if cache["id"] == snmpTargetAddrEntry.branchVersionId:
        tagToTargetsMap = cache["tagToTargetsMap"]
    else:
        cache["tagToTargetsMap"] = {}

        tagToTargetsMap = cache["tagToTargetsMap"]

        (
            SnmpTagValue,
            snmpTargetAddrName,
            snmpTargetAddrTagList,
        ) = mibBuilder.import_symbols(  # type: ignore
            "SNMP-TARGET-MIB",
            "SnmpTagValue",
            "snmpTargetAddrName",
            "snmpTargetAddrTagList",
        )
        mibNode = snmpTargetAddrTagList
        while True:
            try:
                mibNode = snmpTargetAddrTagList.getNextNode(mibNode.name)
            except NoSuchInstanceError:
                break

            idx = mibNode.name[len(snmpTargetAddrTagList.name) :]

            _snmpTargetAddrName = snmpTargetAddrName.getNode(
                snmpTargetAddrName.name + idx
            ).syntax

            for _tag in mibNode.syntax.asOctets().split():
                _tag = SnmpTagValue(_tag)
                if _tag not in tagToTargetsMap:
                    tagToTargetsMap[_tag] = []
                tagToTargetsMap[_tag].append(_snmpTargetAddrName)

        cache["id"] = snmpTargetAddrEntry.branchVersionId

    if tag not in tagToTargetsMap:
        raise SmiError("Transport tag %s not configured at LCD" % tag)

    return tagToTargetsMap[tag]


# convert cmdrsp/cmdgen into this api
