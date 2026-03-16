#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from pyasn1.codec.ber import decoder, eoo
from pyasn1.type import constraint, namedtype, univ
from pysnmp import debug
from pysnmp.proto import api, errind, error, rfc1905, rfc3411
from pysnmp.proto.mpmod.base import AbstractMessageProcessingModel
from pysnmp.proto.secmod.base import AbstractSecurityModel

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine


# API to rfc1905 protocol objects
pMod = api.PROTOCOL_MODULES[api.SNMP_VERSION_2C]  # noqa: N816


# SNMPv3 message format


class ScopedPDU(univ.Sequence):
    """Scoped PDU syntax."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("contextEngineId", univ.OctetString()),
        namedtype.NamedType("contextName", univ.OctetString()),
        namedtype.NamedType("data", rfc1905.PDUs()),
    )


class ScopedPduData(univ.Choice):
    """Scoped PDU data syntax."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("plaintext", ScopedPDU()),
        namedtype.NamedType("encryptedPDU", univ.OctetString()),
    )


class HeaderData(univ.Sequence):
    """SNMPv3 message header data syntax."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType(
            "msgID",
            univ.Integer().subtype(
                subtypeSpec=constraint.ValueRangeConstraint(0, 2147483647)
            ),
        ),
        namedtype.NamedType(
            "msgMaxSize",
            univ.Integer().subtype(
                subtypeSpec=constraint.ValueRangeConstraint(484, 2147483647)
            ),
        ),
        namedtype.NamedType(
            "msgFlags",
            univ.OctetString().subtype(
                subtypeSpec=constraint.ValueSizeConstraint(1, 1)
            ),
        ),
        # NOTE (etingof): constrain SNMPv3 message to only USM+ security models
        # because SNMPv1/v2c seems incompatible in pysnmp implementation, not sure
        # if it's intended by the SNMP standard at all...
        namedtype.NamedType(
            "msgSecurityModel",
            univ.Integer().subtype(
                subtypeSpec=constraint.ValueRangeConstraint(3, 2147483647)
            ),
        ),
    )


class SNMPv3Message(univ.Sequence):
    """SNMPv3 message syntax."""

    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType(
            "msgVersion",
            univ.Integer().subtype(
                subtypeSpec=constraint.ValueRangeConstraint(0, 2147483647)
            ),
        ),
        namedtype.NamedType("msgGlobalData", HeaderData()),
        namedtype.NamedType("msgSecurityParameters", univ.OctetString()),
        namedtype.NamedType("msgData", ScopedPduData()),
    )


# XXX move somewhere?
_snmpErrors = {  # noqa: N816
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 1, 0): errind.unsupportedSecurityLevel,
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 2, 0): errind.notInTimeWindow,
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 3, 0): errind.unknownUserName,
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 4, 0): errind.unknownEngineID,
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 5, 0): errind.wrongDigest,
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 6, 0): errind.decryptionError,
}


class SnmpV3MessageProcessingModel(AbstractMessageProcessingModel):
    """Implement SNMPv3 message processing model."""

    MESSAGE_PROCESSING_MODEL_ID = univ.Integer(3)  # SNMPv3
    SNMP_MSG_SPEC = SNMPv3Message
    _empty_string = univ.OctetString("")
    _message_flags = {
        0: univ.OctetString("\x00"),
        1: univ.OctetString("\x01"),
        3: univ.OctetString("\x03"),
        4: univ.OctetString("\x04"),
        5: univ.OctetString("\x05"),
        7: univ.OctetString("\x07"),
    }

    def __init__(self):
        """Create SNMPv3 message processing model instance."""
        AbstractMessageProcessingModel.__init__(self)
        self.__scopedPDU = ScopedPDU()
        self.__engineIdCache = {}
        self.__engineIdCacheExpQueue = {}
        self.__expirationTimer = 0

    def get_peer_engine_info(self, transportDomain, transportAddress):
        """Return cached peer SNMP engine data."""
        k = transportDomain, transportAddress
        if k in self.__engineIdCache:
            return (
                self.__engineIdCache[k]["securityEngineId"],
                self.__engineIdCache[k]["contextEngineId"],
                self.__engineIdCache[k]["contextName"],
            )
        else:
            return None, None, None

    # 7.1.1a
    def prepare_outgoing_message(
        self,
        snmpEngine: SnmpEngine,
        transportDomain,
        transportAddress,
        messageProcessingModel,
        securityModel,
        securityName,
        securityLevel,
        contextEngineId,
        contextName,
        pduVersion,
        pdu,
        expectResponse,
        sendPduHandle,
    ):
        """Prepare SNMP message for dispatch."""
        (snmpEngineID,) = snmpEngine.get_mib_builder().import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
        )
        snmpEngineID = snmpEngineID.syntax

        # 7.1.1b
        msgID = self._cache.new_message_id()

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareOutgoingMessage: new msgID %s" % msgID
        )

        k = (transportDomain, transportAddress)
        if k in self.__engineIdCache:
            peerSnmpEngineData = self.__engineIdCache[k]
        else:
            peerSnmpEngineData = None

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareOutgoingMessage: peer SNMP engine data {} for transport {}, address {}".format(
                peerSnmpEngineData, transportDomain, transportAddress
            )
        )

        # 7.1.4
        if contextEngineId is None:
            if peerSnmpEngineData is None:
                contextEngineId = snmpEngineID
            else:
                contextEngineId = peerSnmpEngineData["contextEngineId"]
                # Defaulting contextEngineID to securityEngineId should
                # probably be done on Agent side (see 7.1.3.d.2,) so this
                # is a sort of workaround.
                if not contextEngineId:
                    contextEngineId = peerSnmpEngineData["securityEngineId"]
        # 7.1.5
        if not contextName:
            contextName = self._empty_string

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareOutgoingMessage: using contextEngineId {contextEngineId!r}, contextName {contextName!r}"
        )

        # 7.1.6
        scopedPDU = self.__scopedPDU
        scopedPDU.setComponentByPosition(0, contextEngineId)
        scopedPDU.setComponentByPosition(1, contextName)
        scopedPDU.setComponentByPosition(2)
        scopedPDU.getComponentByPosition(2).setComponentByType(
            pdu.tagSet,
            pdu,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # 7.1.7
        msg = self._snmpMsgSpec

        # 7.1.7a
        msg.setComponentByPosition(
            0,
            self.MESSAGE_PROCESSING_MODEL_ID,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        headerData = msg.setComponentByPosition(1).getComponentByPosition(1)

        # 7.1.7b
        headerData.setComponentByPosition(
            0, msgID, verifyConstraints=False, matchTags=False, matchConstraints=False
        )

        (snmpEngineMaxMessageSize,) = snmpEngine.get_mib_builder().import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineMaxMessageSize"
        )

        # 7.1.7c
        # XXX need to coerce MIB value as it has incompatible constraints set
        headerData.setComponentByPosition(
            1,
            snmpEngineMaxMessageSize.syntax,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # 7.1.7d
        msgFlags = 0
        if securityLevel == 1:
            pass
        elif securityLevel == 2:
            msgFlags |= 0x01
        elif securityLevel == 3:
            msgFlags |= 0x03
        else:
            raise error.ProtocolError("Unknown securityLevel %s" % securityLevel)

        if pdu.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            msgFlags |= 0x04

        headerData.setComponentByPosition(
            2,
            self._message_flags[msgFlags],
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # 7.1.7e
        # XXX need to coerce MIB value as it has incompatible constraints set
        headerData.setComponentByPosition(3, int(securityModel))

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareOutgoingMessage: {msg.prettyPrint()}"
        )

        if securityModel in snmpEngine.security_models:
            smHandler = snmpEngine.security_models[securityModel]
        else:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedSecurityModel
            )

        # 7.1.9.a
        if pdu.tagSet in rfc3411.UNCONFIRMED_CLASS_PDUS:
            securityEngineId = snmpEngineID

        else:
            if peerSnmpEngineData is None:
                debug.logger & debug.FLAG_MP and debug.logger(
                    "prepareOutgoingMessage: peer SNMP engine is not known"
                )

                securityEngineId = None

            else:
                securityEngineId = peerSnmpEngineData["securityEngineId"]

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareOutgoingMessage: securityModel {!r}, securityEngineId {!r}, securityName {!r}, securityLevel {!r}".format(
                securityModel, securityEngineId, securityName, securityLevel
            )
        )

        # 7.1.9.b
        (securityParameters, wholeMsg) = smHandler.generate_request_message(
            snmpEngine,
            self.MESSAGE_PROCESSING_MODEL_ID,
            msg,
            snmpEngineMaxMessageSize.syntax,
            securityModel,
            securityEngineId,
            securityName,
            securityLevel,
            scopedPDU,
        )

        # Message size constraint verification
        if len(wholeMsg) > snmpEngineMaxMessageSize.syntax:
            raise error.StatusInformation(errorIndication=errind.tooBig)

        # 7.1.9.c
        if pdu.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            # XXX rfc bug? why stateReference should be created?
            self._cache.push_by_message_id(
                msgID,
                sendPduHandle=sendPduHandle,
                msgID=msgID,
                snmpEngineID=snmpEngineID,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                transportDomain=transportDomain,
                transportAddress=transportAddress,
            )

        snmpEngine.observer.store_execution_context(
            snmpEngine,
            "rfc3412.prepareOutgoingMessage",
            dict(
                transportDomain=transportDomain,
                transportAddress=transportAddress,
                wholeMsg=wholeMsg,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                pdu=pdu,
            ),
        )
        snmpEngine.observer.clear_execution_context(
            snmpEngine, "rfc3412.prepareOutgoingMessage"
        )

        return transportDomain, transportAddress, wholeMsg

    def prepare_response_message(
        self,
        snmpEngine: SnmpEngine,
        messageProcessingModel,
        securityModel,
        securityName,
        securityLevel,
        contextEngineId,
        contextName,
        pduVersion,
        pdu,
        maxSizeResponseScopedPDU,
        stateReference,
        statusInformation,
    ):
        """Prepare SNMP message for response."""
        (snmpEngineID,) = snmpEngine.get_mib_builder().import_symbols(  # type: ignore
            "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
        )
        snmpEngineID = snmpEngineID.syntax

        # 7.1.2.b
        cachedParams = self._cache.pop_by_state_reference(stateReference)
        msgID = cachedParams["msgID"]
        contextEngineId = cachedParams["contextEngineId"]
        contextName = cachedParams["contextName"]
        securityModel = cachedParams["securityModel"]
        securityName = cachedParams["securityName"]
        securityLevel = cachedParams["securityLevel"]
        securityStateReference = cachedParams["securityStateReference"]
        reportableFlag = cachedParams["reportableFlag"]
        maxMessageSize = cachedParams["msgMaxSize"]
        transportDomain = cachedParams["transportDomain"]
        transportAddress = cachedParams["transportAddress"]

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareResponseMessage: stateReference %s" % stateReference
        )

        # 7.1.3
        if statusInformation is not None and "oid" in statusInformation:
            # 7.1.3a
            if pdu is None:
                pduType = None
            else:
                requestID = pdu.getComponentByPosition(0)
                pduType = pdu.tagSet

            # 7.1.3b
            if (
                pdu is None
                and not reportableFlag
                or pduType is not None
                and pduType not in rfc3411.CONFIRMED_CLASS_PDUS
            ):
                if securityModel in snmpEngine.security_models:
                    smHandler = snmpEngine.security_models[securityModel]
                    smHandler.release_state_information(securityStateReference)
                raise error.StatusInformation(errorIndication=errind.loopTerminated)

            # 7.1.3c
            reportPDU = rfc1905.ReportPDU()
            pMod.apiPDU.set_varbinds(
                reportPDU, ((statusInformation["oid"], statusInformation["val"]),)
            )
            pMod.apiPDU.set_error_status(reportPDU, 0)
            pMod.apiPDU.set_error_index(reportPDU, 0)
            if pdu is None:
                pMod.apiPDU.set_request_id(reportPDU, 0)
            else:
                # noinspection PyUnboundLocalVariable
                pMod.apiPDU.set_request_id(reportPDU, requestID)

            # 7.1.3d.1
            if "securityLevel" in statusInformation:
                securityLevel = statusInformation["securityLevel"]
            else:
                securityLevel = 1

            # 7.1.3d.2
            if "contextEngineId" in statusInformation:
                contextEngineId = statusInformation["contextEngineId"]
            else:
                contextEngineId = snmpEngineID

            # 7.1.3d.3
            if "contextName" in statusInformation:
                contextName = statusInformation["contextName"]
            else:
                contextName = ""

            # 7.1.3e
            pdu = reportPDU

            debug.logger & debug.FLAG_MP and debug.logger(
                "prepareResponseMessage: prepare report PDU for statusInformation %s"
                % statusInformation
            )
        # 7.1.4
        if not contextEngineId:
            contextEngineId = snmpEngineID  # XXX impl-dep manner

        # 7.1.5
        if not contextName:
            contextName = self._empty_string

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareResponseMessage: using contextEngineId {contextEngineId!r}, contextName {contextName!r}"
        )

        # 7.1.6
        scopedPDU = self.__scopedPDU
        scopedPDU.setComponentByPosition(0, contextEngineId)
        scopedPDU.setComponentByPosition(1, contextName)
        scopedPDU.setComponentByPosition(2)
        scopedPDU.getComponentByPosition(2).setComponentByType(
            pdu.tagSet,
            pdu,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # 7.1.7
        msg = self._snmpMsgSpec

        # 7.1.7a
        msg.setComponentByPosition(
            0,
            self.MESSAGE_PROCESSING_MODEL_ID,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        headerData = msg.setComponentByPosition(1).getComponentByPosition(1)

        # 7.1.7b
        headerData.setComponentByPosition(
            0, msgID, verifyConstraints=False, matchTags=False, matchConstraints=False
        )

        (snmpEngineMaxMessageSize,) = snmpEngine.get_mib_builder().import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineMaxMessageSize"
        )

        # 7.1.7c
        # XXX need to coerce MIB value as it has incompatible constraints set
        headerData.setComponentByPosition(
            1,
            snmpEngineMaxMessageSize.syntax,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # 7.1.7d
        msgFlags = 0
        if securityLevel == 1:
            pass
        elif securityLevel == 2:
            msgFlags |= 0x01
        elif securityLevel == 3:
            msgFlags |= 0x03
        else:
            raise error.ProtocolError("Unknown securityLevel %s" % securityLevel)

        if pdu.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:  # XXX not needed?
            msgFlags |= 0x04

        headerData.setComponentByPosition(
            2,
            self._message_flags[msgFlags],
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # 7.1.7e
        headerData.setComponentByPosition(
            3,
            securityModel,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareResponseMessage: {msg.prettyPrint()}"
        )

        if securityModel in snmpEngine.security_models:
            smHandler = snmpEngine.security_models[securityModel]
        else:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedSecurityModel
            )

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareResponseMessage: securityModel {!r}, securityEngineId {!r}, securityName {!r}, securityLevel {!r}".format(
                securityModel, snmpEngineID, securityName, securityLevel
            )
        )

        # 7.1.8a
        try:
            (securityParameters, wholeMsg) = smHandler.generate_response_message(
                snmpEngine,
                self.MESSAGE_PROCESSING_MODEL_ID,
                msg,
                snmpEngineMaxMessageSize.syntax,
                securityModel,
                snmpEngineID,
                securityName,
                securityLevel,
                scopedPDU,
                securityStateReference,
                statusInformation.get(
                    "errorIndication"
                ),  # Use get() to handle missing errorIndication
            )  # type: ignore
        except error.StatusInformation:
            # 7.1.8.b
            raise

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareResponseMessage: SM finished"
        )

        # Message size constraint verification
        if len(wholeMsg) > min(snmpEngineMaxMessageSize.syntax, maxMessageSize):
            raise error.StatusInformation(errorIndication=errind.tooBig)

        snmpEngine.observer.store_execution_context(
            snmpEngine,
            "rfc3412.prepareResponseMessage",
            dict(
                transportDomain=transportDomain,
                transportAddress=transportAddress,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                securityEngineId=snmpEngineID,
                pdu=pdu,
            ),
        )
        snmpEngine.observer.clear_execution_context(
            snmpEngine, "rfc3412.prepareResponseMessage"
        )

        return transportDomain, transportAddress, wholeMsg

    # 7.2.1

    def prepare_data_elements(
        self, snmpEngine: SnmpEngine, transportDomain, transportAddress, wholeMsg
    ):
        """Prepare SNMP message data elements."""
        # 7.2.2
        msg, restOfwholeMsg = decoder.decode(wholeMsg, asn1Spec=self._snmpMsgSpec)

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareDataElements: {msg.prettyPrint()}"
        )

        if eoo.endOfOctets.isSameTypeWith(msg):
            raise error.StatusInformation(errorIndication=errind.parseError)

        # 7.2.3
        headerData = msg.getComponentByPosition(1)
        msgVersion = messageProcessingModel = msg.getComponentByPosition(0)
        msgID = headerData.getComponentByPosition(0)
        (msgFlags,) = headerData.getComponentByPosition(2).asNumbers()
        maxMessageSize = headerData.getComponentByPosition(1)
        securityModel = headerData.getComponentByPosition(3)
        securityParameters = msg.getComponentByPosition(2)

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareDataElements: msg data msgVersion {} msgID {} securityModel {}".format(
                msgVersion, msgID, securityModel
            )
        )

        # 7.2.4
        if securityModel not in snmpEngine.security_models:
            (snmpUnknownSecurityModels,) = snmpEngine.get_mib_builder().import_symbols(
                "__SNMP-MPD-MIB", "snmpUnknownSecurityModels"
            )
            snmpUnknownSecurityModels.syntax += 1
            raise error.StatusInformation(
                errorIndication=errind.unsupportedSecurityModel
            )

        # 7.2.5
        if msgFlags & 0x03 == 0x00:
            securityLevel = 1  # noAuthNoPriv
        elif (msgFlags & 0x03) == 0x01:
            securityLevel = 2  # authNoPriv
        elif (msgFlags & 0x03) == 0x03:
            securityLevel = 3  # authPriv
        else:
            (snmpInvalidMsgs,) = snmpEngine.get_mib_builder().import_symbols(
                "__SNMP-MPD-MIB", "snmpInvalidMsgs"
            )
            snmpInvalidMsgs.syntax += 1
            raise error.StatusInformation(errorIndication=errind.invalidMsg)

        if msgFlags & 0x04:
            reportableFlag = 1
        else:
            reportableFlag = 0

        # 7.2.6
        smHandler: AbstractSecurityModel = snmpEngine.security_models[securityModel]
        try:
            (
                securityEngineId,
                securityName,
                scopedPDU,
                maxSizeResponseScopedPDU,
                securityStateReference,
            ) = smHandler.process_incoming_message(
                snmpEngine,
                messageProcessingModel,
                maxMessageSize,
                securityParameters,
                securityModel,
                securityLevel,
                wholeMsg,
                msg,
            )  # type: ignore
            debug.logger & debug.FLAG_MP and debug.logger(
                "prepareDataElements: SM succeeded"
            )

        except error.StatusInformation:
            statusInformation, origTraceback = sys.exc_info()[1:3]

            debug.logger & debug.FLAG_MP and debug.logger(
                "prepareDataElements: SM failed, statusInformation %s"
                % statusInformation
            )

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc3412.prepareDataElements:sm-failure",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityLevel=securityLevel,
                    securityParameters=securityParameters,
                    statusInformation=statusInformation,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc3412.prepareDataElements:sm-failure"
            )

            if "errorIndication" in statusInformation:
                # 7.2.6a
                if "oid" in statusInformation:
                    # 7.2.6a1
                    securityStateReference = statusInformation["securityStateReference"]
                    contextEngineId = statusInformation["contextEngineId"]
                    contextName = statusInformation["contextName"]
                    if "scopedPDU" in statusInformation:
                        scopedPDU = statusInformation["scopedPDU"]
                        pdu = scopedPDU.getComponentByPosition(2).getComponent()
                    else:
                        pdu = None
                    maxSizeResponseScopedPDU = statusInformation[
                        "maxSizeResponseScopedPDU"
                    ]
                    securityName = None  # XXX secmod cache used

                    # 7.2.6a2
                    stateReference = self._cache.new_state_reference()
                    self._cache.push_by_state_reference(
                        stateReference,
                        msgVersion=messageProcessingModel,
                        msgID=msgID,
                        contextEngineId=contextEngineId,
                        contextName=contextName,
                        securityModel=securityModel,
                        securityName=securityName,
                        securityLevel=securityLevel,
                        securityStateReference=securityStateReference,
                        reportableFlag=reportableFlag,
                        msgMaxSize=maxMessageSize,
                        maxSizeResponseScopedPDU=maxSizeResponseScopedPDU,
                        transportDomain=transportDomain,
                        transportAddress=transportAddress,
                    )

                    # 7.2.6a3
                    try:
                        snmpEngine.message_dispatcher.return_response_pdu(
                            snmpEngine,
                            3,
                            securityModel,
                            securityName,
                            securityLevel,
                            contextEngineId,
                            contextName,
                            1,
                            pdu,
                            maxSizeResponseScopedPDU,
                            stateReference,
                            statusInformation,
                        )
                    except error.StatusInformation:
                        pass

                    debug.logger & debug.FLAG_MP and debug.logger(
                        "prepareDataElements: error reported"
                    )

            # 7.2.6b
            try:
                raise statusInformation.with_traceback(origTraceback)
            finally:
                # Break cycle between locals and traceback object
                # (seems to be irrelevant on Py3 but just in case)
                del origTraceback
        else:
            # Sniff for engineIdCache
            k = (transportDomain, transportAddress)
            if k not in self.__engineIdCache:
                contextEngineId = scopedPDU[0]
                contextName = scopedPDU[1]
                pdus = scopedPDU[2]
                pdu = pdus.getComponent()
                # Here we assume that authentic/default EngineIDs
                # come only in the course of engine-to-engine communication.
                if pdu.tagSet in rfc3411.INTERNAL_CLASS_PDUS:
                    self.__engineIdCache[k] = {
                        "securityEngineId": securityEngineId,
                        "contextEngineId": contextEngineId,
                        "contextName": contextName,
                    }

                    expireAt = int(
                        self.__expirationTimer
                        + 300 / snmpEngine.transport_dispatcher.get_timer_resolution()
                    )
                    if expireAt not in self.__engineIdCacheExpQueue:
                        self.__engineIdCacheExpQueue[expireAt] = []
                    self.__engineIdCacheExpQueue[expireAt].append(k)

                    debug.logger & debug.FLAG_MP and debug.logger(
                        "prepareDataElements: cache securityEngineId {!r} for {!r} {!r}".format(
                            securityEngineId, transportDomain, transportAddress
                        )
                    )

        (snmpEngineID,) = snmpEngine.get_mib_builder().import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
        )
        snmpEngineID = snmpEngineID.syntax

        # 7.2.7 XXX PDU would be parsed here?
        contextEngineId = scopedPDU[0]
        contextName = scopedPDU[1]
        pdu = scopedPDU[2]

        pdu = pdu.getComponent()  # PDUs

        # 7.2.8
        pduVersion = api.SNMP_VERSION_2C

        # 7.2.9
        pduType = pdu.tagSet

        # 7.2.10
        if (
            pduType in rfc3411.RESPONSE_CLASS_PDUS
            or pduType in rfc3411.INTERNAL_CLASS_PDUS
        ):
            # 7.2.10a
            try:
                cachedReqParams = self._cache.pop_by_message_id(msgID)
            except error.ProtocolError:
                smHandler.release_state_information(securityStateReference)
                raise error.StatusInformation(errorIndication=errind.dataMismatch)
            # 7.2.10b
            sendPduHandle = cachedReqParams["sendPduHandle"]
        else:
            sendPduHandle = None

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareDataElements: using sendPduHandle {sendPduHandle} for msgID {msgID}"
        )

        # 7.2.11
        if pduType in rfc3411.INTERNAL_CLASS_PDUS:
            # 7.2.11a
            varBinds = pMod.apiPDU.get_varbinds(pdu)
            if varBinds:
                statusInformation = error.StatusInformation(
                    errorIndication=_snmpErrors.get(
                        varBinds[0][0],
                        errind.ReportPduReceived(varBinds[0][0].prettyPrint()),
                    ),
                    oid=varBinds[0][0],
                    val=varBinds[0][1],
                    sendPduHandle=sendPduHandle,
                )
            else:
                statusInformation = error.StatusInformation(sendPduHandle=sendPduHandle)

            # 7.2.11b (incomplete implementation)

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc3412.prepareDataElements:internal",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    securityEngineId=securityEngineId,
                    pdu=pdu,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc3412.prepareDataElements:internal"
            )

            # 7.2.11c
            smHandler.release_state_information(securityStateReference)

            # 7.2.11d
            # no-op

            # 7.2.11e XXX may need to pass Reports up to app in some cases...
            raise statusInformation

        statusInformation = None  # no errors ahead

        # 7.2.12
        if pduType in rfc3411.RESPONSE_CLASS_PDUS:
            # 7.2.12a -> no-op

            # 7.2.12b
            # noinspection PyUnboundLocalVariable
            if (
                securityModel != cachedReqParams["securityModel"]
                or securityName != cachedReqParams["securityName"]
                or securityLevel != cachedReqParams["securityLevel"]
                or contextEngineId != cachedReqParams["contextEngineId"]
                or contextName != cachedReqParams["contextName"]
            ):
                smHandler.release_state_information(securityStateReference)
                raise error.StatusInformation(errorIndication=errind.dataMismatch)

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc3412.prepareDataElements:response",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    securityEngineId=securityEngineId,
                    pdu=pdu,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc3412.prepareDataElements:response"
            )

            # 7.2.12c
            smHandler.release_state_information(securityStateReference)
            stateReference = None

            # 7.2.12d
            return (
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                pdu,
                pduType,
                sendPduHandle,
                maxSizeResponseScopedPDU,
                statusInformation,
                stateReference,
            )

        # 7.2.13
        if pduType in rfc3411.CONFIRMED_CLASS_PDUS:
            # 7.2.13a
            if securityEngineId != snmpEngineID:
                smHandler.release_state_information(securityStateReference)
                raise error.StatusInformation(errorIndication=errind.engineIDMismatch)

            # 7.2.13b
            stateReference = self._cache.new_state_reference()
            self._cache.push_by_state_reference(
                stateReference,
                msgVersion=messageProcessingModel,
                msgID=msgID,
                contextEngineId=contextEngineId,
                contextName=contextName,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                securityStateReference=securityStateReference,
                reportableFlag=reportableFlag,
                msgMaxSize=maxMessageSize,
                maxSizeResponseScopedPDU=maxSizeResponseScopedPDU,
                transportDomain=transportDomain,
                transportAddress=transportAddress,
            )

            debug.logger & debug.FLAG_MP and debug.logger(
                "prepareDataElements: new stateReference %s" % stateReference
            )

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc3412.prepareDataElements:confirmed",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    securityEngineId=securityEngineId,
                    pdu=pdu,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc3412.prepareDataElements:confirmed"
            )

            # 7.2.13c
            return (
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                pdu,
                pduType,
                sendPduHandle,
                maxSizeResponseScopedPDU,
                statusInformation,
                stateReference,
            )

        # 7.2.14
        if pduType in rfc3411.UNCONFIRMED_CLASS_PDUS:
            # Pass new stateReference to let app browse request details
            stateReference = self._cache.new_state_reference()

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc3412.prepareDataElements:unconfirmed",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    securityEngineId=securityEngineId,
                    pdu=pdu,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc3412.prepareDataElements:unconfirmed"
            )

            # This is not specified explicitly in RFC
            smHandler.release_state_information(securityStateReference)

            return (
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                pdu,
                pduType,
                sendPduHandle,
                maxSizeResponseScopedPDU,
                statusInformation,
                stateReference,
            )

        smHandler.release_state_information(securityStateReference)
        raise error.StatusInformation(errorIndication=errind.unsupportedPDUtype)

    def __expire_engines_info(self):
        if self.__expirationTimer in self.__engineIdCacheExpQueue:
            for engineKey in self.__engineIdCacheExpQueue[self.__expirationTimer]:
                del self.__engineIdCache[engineKey]
                debug.logger & debug.FLAG_MP and debug.logger(
                    f"__expireEnginesInfo: expiring {engineKey!r}"
                )
            del self.__engineIdCacheExpQueue[self.__expirationTimer]
        self.__expirationTimer += 1

    def receive_timer_tick(self, snmpEngine: SnmpEngine, timeNow):
        """Process periodic timer tick."""
        self.__expire_engines_info()
        AbstractMessageProcessingModel.receive_timer_tick(self, snmpEngine, timeNow)
