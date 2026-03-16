#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys
from typing import TYPE_CHECKING


from pyasn1.codec.ber import encoder
from pyasn1.error import PyAsn1Error
from pysnmp import debug
from pysnmp.carrier.asyncio.dgram import udp, udp6
from pysnmp.proto import errind, error
from pysnmp.proto.secmod import base
from pysnmp.smi.error import NoSuchInstanceError

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine


class SnmpV1SecurityModel(base.AbstractSecurityModel):
    """Create SNMPv1 security model."""

    SECURITY_MODEL_ID = 1

    # According to rfc2576, community name <-> contextEngineId/contextName
    # mapping is up to MP module for notifications but belongs to secmod
    # responsibility for other PDU types. Since I do not yet understand
    # the reason for this de-coupling, I've moved this code from MP-scope
    # in here.

    def __init__(self):
        """Create SNMPv1 security model instance."""
        self.__transportBranchId = (
            self.__paramsBranchId
        ) = self.__communityBranchId = self.__securityBranchId = -1
        base.AbstractSecurityModel.__init__(self)

    def _close(self):
        """
        Close the security model to test memory leak.

        This method is intended for unit testing purposes only.
        It closes the security model and checks if all associated resources are released.
        """
        pass

    def _sec2com(
        self, snmpEngine: "SnmpEngine", securityName, contextEngineId, contextName
    ):
        (snmpTargetParamsSecurityName,) = snmpEngine.get_mib_builder().import_symbols(
            "SNMP-TARGET-MIB", "snmpTargetParamsSecurityName"
        )
        if self.__paramsBranchId != snmpTargetParamsSecurityName.branchVersionId:
            (
                snmpTargetParamsSecurityModel,
            ) = snmpEngine.get_mib_builder().import_symbols(
                "SNMP-TARGET-MIB", "snmpTargetParamsSecurityModel"
            )

            self.__nameToModelMap = {}

            nextMibNode = snmpTargetParamsSecurityName

            while True:
                try:
                    nextMibNode = snmpTargetParamsSecurityName.getNextNode(
                        nextMibNode.name
                    )

                except NoSuchInstanceError:
                    break

                instId = nextMibNode.name[len(snmpTargetParamsSecurityName.name) :]

                mibNode = snmpTargetParamsSecurityModel.getNode(
                    snmpTargetParamsSecurityModel.name + instId
                )

                try:
                    if mibNode.syntax not in self.__nameToModelMap:
                        self.__nameToModelMap[nextMibNode.syntax] = set()

                    self.__nameToModelMap[nextMibNode.syntax].add(mibNode.syntax)

                except PyAsn1Error:
                    debug.logger & debug.FLAG_SM and debug.logger(
                        "_sec2com: table entries {!r}/{!r} hashing failed".format(
                            nextMibNode.syntax, mibNode.syntax
                        )
                    )
                    continue

            self.__paramsBranchId = snmpTargetParamsSecurityName.branchVersionId

            # invalidate next map as it include this one
            self.__securityBranchId = -1

        (snmpCommunityName,) = snmpEngine.get_mib_builder().import_symbols(
            "SNMP-COMMUNITY-MIB", "snmpCommunityName"
        )
        if self.__securityBranchId != snmpCommunityName.branchVersionId:
            (
                snmpCommunitySecurityName,
                snmpCommunityContextEngineId,
                snmpCommunityContextName,
            ) = snmpEngine.get_mib_builder().import_symbols(
                "SNMP-COMMUNITY-MIB",
                "snmpCommunitySecurityName",
                "snmpCommunityContextEngineID",
                "snmpCommunityContextName",
            )

            self.__securityMap = {}

            nextMibNode = snmpCommunityName

            while True:
                try:
                    nextMibNode = snmpCommunityName.getNextNode(nextMibNode.name)

                except NoSuchInstanceError:
                    break

                instId = nextMibNode.name[len(snmpCommunityName.name) :]

                _securityName = snmpCommunitySecurityName.getNode(
                    snmpCommunitySecurityName.name + instId
                ).syntax

                _contextEngineId = snmpCommunityContextEngineId.getNode(
                    snmpCommunityContextEngineId.name + instId
                ).syntax

                _contextName = snmpCommunityContextName.getNode(
                    snmpCommunityContextName.name + instId
                ).syntax

                try:
                    self.__securityMap[
                        (_securityName, _contextEngineId, _contextName)
                    ] = nextMibNode.syntax

                except PyAsn1Error:
                    debug.logger & debug.FLAG_SM and debug.logger(
                        "_sec2com: table entries {!r}/{!r}/{!r} hashing failed".format(
                            _securityName, _contextEngineId, _contextName
                        )
                    )
                    continue

            self.__securityBranchId = snmpCommunityName.branchVersionId

            debug.logger & debug.FLAG_SM and debug.logger(
                "_sec2com: built securityName to communityName map, version {}: {}".format(
                    self.__securityBranchId, self.__securityMap
                )
            )

        try:
            return self.__securityMap[(securityName, contextEngineId, contextName)]

        except KeyError:
            raise error.StatusInformation(errorIndication=errind.unknownCommunityName)

    def _com2sec(self, snmpEngine: "SnmpEngine", communityName, transportInformation):
        (snmpTargetAddrTAddress,) = snmpEngine.get_mib_builder().import_symbols(
            "SNMP-TARGET-MIB", "snmpTargetAddrTAddress"
        )
        if self.__transportBranchId != snmpTargetAddrTAddress.branchVersionId:
            (
                SnmpTagValue,
                snmpTargetAddrTDomain,
                snmpTargetAddrTagList,
            ) = snmpEngine.get_mib_builder().import_symbols(
                "SNMP-TARGET-MIB",
                "SnmpTagValue",
                "snmpTargetAddrTDomain",
                "snmpTargetAddrTagList",
            )

            self.__emptyTag = SnmpTagValue("")

            self.__transportToTagMap = {}

            nextMibNode = snmpTargetAddrTagList

            while True:
                try:
                    nextMibNode = snmpTargetAddrTagList.getNextNode(nextMibNode.name)

                except NoSuchInstanceError:
                    break

                instId = nextMibNode.name[len(snmpTargetAddrTagList.name) :]
                targetAddrTDomain = snmpTargetAddrTDomain.getNode(
                    snmpTargetAddrTDomain.name + instId
                ).syntax
                targetAddrTAddress = snmpTargetAddrTAddress.getNode(
                    snmpTargetAddrTAddress.name + instId
                ).syntax

                targetAddrTDomain = tuple(targetAddrTDomain)

                if targetAddrTDomain[: len(udp.SNMP_UDP_DOMAIN)] == udp.SNMP_UDP_DOMAIN:
                    (SnmpUDPAddress,) = snmpEngine.get_mib_builder().import_symbols(
                        "SNMPv2-TM", "SnmpUDPAddress"
                    )
                    targetAddrTAddress = tuple(SnmpUDPAddress(targetAddrTAddress))
                elif (
                    targetAddrTDomain[: len(udp6.SNMP_UDP6_DOMAIN)]
                    == udp6.SNMP_UDP6_DOMAIN
                ):
                    (
                        TransportAddressIPv6,
                    ) = snmpEngine.get_mib_builder().import_symbols(
                        "TRANSPORT-ADDRESS-MIB", "TransportAddressIPv6"
                    )
                    targetAddrTAddress = tuple(TransportAddressIPv6(targetAddrTAddress))
                targetAddr = targetAddrTDomain, targetAddrTAddress
                targetAddrTagList = snmpTargetAddrTagList.getNode(
                    snmpTargetAddrTagList.name + instId
                ).syntax

                if targetAddr not in self.__transportToTagMap:
                    self.__transportToTagMap[targetAddr] = set()

                try:
                    if targetAddrTagList:
                        self.__transportToTagMap[targetAddr].update(
                            [
                                SnmpTagValue(x)
                                for x in targetAddrTagList.asOctets().split()
                            ]
                        )

                    else:
                        self.__transportToTagMap[targetAddr].add(self.__emptyTag)

                except PyAsn1Error:
                    debug.logger & debug.FLAG_SM and debug.logger(
                        "_com2sec: table entries {!r}/{!r} hashing failed".format(
                            targetAddr, targetAddrTagList
                        )
                    )
                    continue

            self.__transportBranchId = snmpTargetAddrTAddress.branchVersionId

            debug.logger & debug.FLAG_SM and debug.logger(
                "_com2sec: built transport-to-tag map version {}: {}".format(
                    self.__transportBranchId, self.__transportToTagMap
                )
            )

        (snmpTargetParamsSecurityName,) = snmpEngine.get_mib_builder().import_symbols(
            "SNMP-TARGET-MIB", "snmpTargetParamsSecurityName"
        )

        if self.__paramsBranchId != snmpTargetParamsSecurityName.branchVersionId:
            (
                snmpTargetParamsSecurityModel,
            ) = snmpEngine.get_mib_builder().import_symbols(
                "SNMP-TARGET-MIB", "snmpTargetParamsSecurityModel"
            )

            self.__nameToModelMap = {}

            nextMibNode = snmpTargetParamsSecurityName

            while True:
                try:
                    nextMibNode = snmpTargetParamsSecurityName.getNextNode(
                        nextMibNode.name
                    )

                except NoSuchInstanceError:
                    break

                instId = nextMibNode.name[len(snmpTargetParamsSecurityName.name) :]

                mibNode = snmpTargetParamsSecurityModel.getNode(
                    snmpTargetParamsSecurityModel.name + instId
                )

                try:
                    if nextMibNode.syntax not in self.__nameToModelMap:
                        self.__nameToModelMap[nextMibNode.syntax] = set()

                    self.__nameToModelMap[nextMibNode.syntax].add(mibNode.syntax)

                except PyAsn1Error:
                    debug.logger & debug.FLAG_SM and debug.logger(
                        "_com2sec: table entries {!r}/{!r} hashing failed".format(
                            nextMibNode.syntax, mibNode.syntax
                        )
                    )
                    continue

            self.__paramsBranchId = snmpTargetParamsSecurityName.branchVersionId

            # invalidate next map as it include this one
            self.__communityBranchId = -1

            debug.logger & debug.FLAG_SM and debug.logger(
                "_com2sec: built securityName to securityModel map, version {}: {}".format(
                    self.__paramsBranchId, self.__nameToModelMap
                )
            )

        (snmpCommunityName,) = snmpEngine.get_mib_builder().import_symbols(
            "SNMP-COMMUNITY-MIB", "snmpCommunityName"
        )
        if self.__communityBranchId != snmpCommunityName.branchVersionId:
            (
                snmpCommunitySecurityName,
                snmpCommunityContextEngineId,
                snmpCommunityContextName,
                snmpCommunityTransportTag,
            ) = snmpEngine.get_mib_builder().import_symbols(
                "SNMP-COMMUNITY-MIB",
                "snmpCommunitySecurityName",
                "snmpCommunityContextEngineID",
                "snmpCommunityContextName",
                "snmpCommunityTransportTag",
            )

            self.__communityToTagMap = {}
            self.__tagAndCommunityToSecurityMap = {}

            nextMibNode = snmpCommunityName

            while True:
                try:
                    nextMibNode = snmpCommunityName.getNextNode(nextMibNode.name)

                except NoSuchInstanceError:
                    break

                instId = nextMibNode.name[len(snmpCommunityName.name) :]

                securityName = snmpCommunitySecurityName.getNode(
                    snmpCommunitySecurityName.name + instId
                ).syntax

                contextEngineId = snmpCommunityContextEngineId.getNode(
                    snmpCommunityContextEngineId.name + instId
                ).syntax

                contextName = snmpCommunityContextName.getNode(
                    snmpCommunityContextName.name + instId
                ).syntax

                transportTag = snmpCommunityTransportTag.getNode(
                    snmpCommunityTransportTag.name + instId
                ).syntax

                _tagAndCommunity = transportTag, nextMibNode.syntax

                try:
                    if _tagAndCommunity not in self.__tagAndCommunityToSecurityMap:
                        self.__tagAndCommunityToSecurityMap[_tagAndCommunity] = set()

                    self.__tagAndCommunityToSecurityMap[_tagAndCommunity].add(
                        (securityName, contextEngineId, contextName)
                    )

                    if nextMibNode.syntax not in self.__communityToTagMap:
                        self.__communityToTagMap[nextMibNode.syntax] = set()

                    self.__communityToTagMap[nextMibNode.syntax].add(transportTag)

                except PyAsn1Error:
                    debug.logger & debug.FLAG_SM and debug.logger(
                        "_com2sec: table entries {!r}/{!r} hashing failed".format(
                            _tagAndCommunity, nextMibNode.syntax
                        )
                    )
                    continue

            self.__communityBranchId = snmpCommunityName.branchVersionId

            debug.logger & debug.FLAG_SM and debug.logger(
                "_com2sec: built communityName to tag map (securityModel {}), version {}: {}".format(
                    self.SECURITY_MODEL_ID,
                    self.__communityBranchId,
                    self.__communityToTagMap,
                )
            )

            debug.logger & debug.FLAG_SM and debug.logger(
                "_com2sec: built tag & community to securityName map (securityModel {}), version {}: {}".format(
                    self.SECURITY_MODEL_ID,
                    self.__communityBranchId,
                    self.__tagAndCommunityToSecurityMap,
                )
            )

        if communityName in self.__communityToTagMap:
            if transportInformation in self.__transportToTagMap:
                tags = self.__transportToTagMap[transportInformation].intersection(
                    self.__communityToTagMap[communityName]
                )
            elif self.__emptyTag in self.__communityToTagMap[communityName]:
                tags = [self.__emptyTag]
            else:
                raise error.StatusInformation(
                    errorIndication=errind.unknownCommunityName
                )

            candidateSecurityNames = []

            for x in [
                self.__tagAndCommunityToSecurityMap[(t, communityName)] for t in tags
            ]:
                candidateSecurityNames.extend(list(x))

            # 5.2.1 (row selection in snmpCommunityTable)
            # Picks first match but favors entries already in targets table
            if candidateSecurityNames:
                candidateSecurityNames.sort(
                    key=lambda x, m=self.__nameToModelMap, v=self.SECURITY_MODEL_ID: (
                        not int(x[0] in m and v in m[x[0]]),
                        str(x[0]),
                    )
                )
                chosenSecurityName = candidateSecurityNames[0]  # min()
                debug.logger & debug.FLAG_SM and debug.logger(
                    "_com2sec: securityName candidates for communityName '{}' are {}; choosing securityName '{}'".format(
                        communityName, candidateSecurityNames, chosenSecurityName[0]
                    )
                )
                return chosenSecurityName

        raise error.StatusInformation(errorIndication=errind.unknownCommunityName)

    def generate_request_message(
        self,
        snmpEngine: "SnmpEngine",
        messageProcessingModel,
        globalData,
        maxMessageSize,
        securityModel,
        securityEngineId,
        securityName,
        securityLevel,
        scopedPDU,
    ):
        """Generate a request message."""
        (msg,) = globalData
        contextEngineId, contextName, pdu = scopedPDU

        # rfc2576: 5.2.3
        communityName = self._sec2com(
            snmpEngine, securityName, contextEngineId, contextName
        )

        debug.logger & debug.FLAG_SM and debug.logger(
            "generateRequestMsg: using community {!r} for securityModel {!r}, securityName {!r}, contextEngineId {!r} contextName {!r}".format(
                communityName, securityModel, securityName, contextEngineId, contextName
            )
        )

        securityParameters = communityName

        msg.setComponentByPosition(1, securityParameters)
        msg.setComponentByPosition(2)
        msg.getComponentByPosition(2).setComponentByType(
            pdu.tagSet,
            pdu,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        debug.logger & debug.FLAG_MP and debug.logger(
            f"generateRequestMsg: {msg.prettyPrint()}"
        )

        try:
            return securityParameters, encoder.encode(msg)

        except PyAsn1Error:
            debug.logger & debug.FLAG_MP and debug.logger(
                "generateRequestMsg: serialization failure: %s" % sys.exc_info()[1]
            )
            raise error.StatusInformation(errorIndication=errind.serializationError)

    def generate_response_message(
        self,
        snmpEngine: "SnmpEngine",
        messageProcessingModel,
        globalData,
        maxMessageSize,
        securityModel,
        securityEngineID,
        securityName,
        securityLevel,
        scopedPDU,
        securityStateReference,
        ctx,
    ):
        """Generate a response message."""
        # rfc2576: 5.2.2
        (msg,) = globalData
        contextEngineId, contextName, pdu = scopedPDU
        cachedSecurityData = self._cache.pop(securityStateReference)
        communityName = cachedSecurityData["communityName"]

        debug.logger & debug.FLAG_SM and debug.logger(
            "generateResponseMsg: recovered community {!r} by securityStateReference {}".format(
                communityName, securityStateReference
            )
        )

        msg.setComponentByPosition(1, communityName)
        msg.setComponentByPosition(2)
        msg.getComponentByPosition(2).setComponentByType(
            pdu.tagSet,
            pdu,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        debug.logger & debug.FLAG_MP and debug.logger(
            f"generateResponseMsg: {msg.prettyPrint()}"
        )

        try:
            return communityName, encoder.encode(msg)

        except PyAsn1Error:
            debug.logger & debug.FLAG_MP and debug.logger(
                "generateResponseMsg: serialization failure: %s" % sys.exc_info()[1]
            )
            raise error.StatusInformation(errorIndication=errind.serializationError)

    def process_incoming_message(
        self,
        snmpEngine: "SnmpEngine",
        messageProcessingModel,
        maxMessageSize,
        securityParameters,
        securityModel,
        securityLevel,
        wholeMsg,
        msg,
    ):
        """Process an incoming message."""
        # rfc2576: 5.2.1
        communityName, transportInformation = securityParameters

        scope = dict(
            communityName=communityName, transportInformation=transportInformation
        )

        snmpEngine.observer.store_execution_context(
            snmpEngine, "rfc2576.processIncomingMsg:writable", scope
        )
        snmpEngine.observer.clear_execution_context(
            snmpEngine, "rfc2576.processIncomingMsg:writable"
        )

        try:
            securityName, contextEngineId, contextName = self._com2sec(
                snmpEngine,
                scope.get("communityName", communityName),
                scope.get("transportInformation", transportInformation),
            )

        except error.StatusInformation:
            (snmpInBadCommunityNames,) = snmpEngine.get_mib_builder().import_symbols(
                "__SNMPv2-MIB", "snmpInBadCommunityNames"
            )
            snmpInBadCommunityNames.syntax += 1
            raise error.StatusInformation(
                errorIndication=errind.unknownCommunityName, communityName=communityName
            )

        (snmpEngineID,) = snmpEngine.get_mib_builder().import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
        )

        securityEngineID = snmpEngineID.syntax

        snmpEngine.observer.store_execution_context(
            snmpEngine,
            "rfc2576.processIncomingMsg",
            dict(
                transportInformation=transportInformation,
                securityEngineId=securityEngineID,
                securityName=securityName,
                communityName=communityName,
                contextEngineId=contextEngineId,
                contextName=contextName,
            ),
        )
        snmpEngine.observer.clear_execution_context(
            snmpEngine, "rfc2576.processIncomingMsg"
        )

        debug.logger & debug.FLAG_SM and debug.logger(
            "processIncomingMsg: looked up securityName {!r} securityModel {!r} contextEngineId {!r} contextName {!r} by communityName {!r} AND transportInformation {!r}".format(
                securityName,
                self.SECURITY_MODEL_ID,
                contextEngineId,
                contextName,
                communityName,
                transportInformation,
            )
        )

        stateReference = self._cache.push(communityName=communityName)

        scopedPDU = (
            contextEngineId,
            contextName,
            msg.getComponentByPosition(2).getComponent(),
        )
        maxSizeResponseScopedPDU = maxMessageSize - 128
        securityStateReference = stateReference

        debug.logger & debug.FLAG_SM and debug.logger(
            "processIncomingMsg: generated maxSizeResponseScopedPDU {} securityStateReference {}".format(
                maxSizeResponseScopedPDU, securityStateReference
            )
        )

        return (
            securityEngineID,
            securityName,
            scopedPDU,
            maxSizeResponseScopedPDU,
            securityStateReference,
        )


class SnmpV2cSecurityModel(SnmpV1SecurityModel):
    """Create SNMPv2c security model."""

    SECURITY_MODEL_ID = 2


# XXX
# contextEngineId/contextName goes to globalData
