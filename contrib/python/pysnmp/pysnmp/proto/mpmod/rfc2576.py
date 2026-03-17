#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys
from typing import TYPE_CHECKING


from pyasn1.codec.ber import decoder, eoo
from pyasn1.type import univ
from pysnmp import debug
from pysnmp.proto import errind, error, rfc3411
from pysnmp.proto.api import v1, v2c
from pysnmp.proto.mpmod.base import AbstractMessageProcessingModel
from pysnmp.proto.secmod.base import AbstractSecurityModel

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine

# Since I have not found a detailed reference to v1MP/v2cMP
# inner workings, the following has been patterned from v3MP. Most
# references here goes to RFC3412.


class SnmpV1MessageProcessingModel(AbstractMessageProcessingModel):
    """Create a message processing model object."""

    MESSAGE_PROCESSING_MODEL_ID = univ.Integer(0)  # SNMPv1
    SNMP_MSG_SPEC = v1.Message

    # rfc3412: 7.1
    def prepare_outgoing_message(
        self,
        snmpEngine: "SnmpEngine",
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
        mibBuilder = snmpEngine.get_mib_builder()

        (snmpEngineId,) = mibBuilder.import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
        )
        snmpEngineId = snmpEngineId.syntax

        # rfc3412: 7.1.1b
        if pdu.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            # serve unique PDU request-id
            msgID = self._cache.new_message_id()
            reqID = pdu.getComponentByPosition(0)
            debug.logger & debug.FLAG_MP and debug.logger(
                f"prepareOutgoingMessage: PDU request-id {reqID} replaced with unique ID {msgID}"
            )

        # rfc3412: 7.1.4
        # Since there's no SNMP engine identification in v1/2c,
        # set destination contextEngineId to ours
        if not contextEngineId:
            contextEngineId = snmpEngineId

        # rfc3412: 7.1.5
        if not contextName:
            contextName = b""

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareOutgoingMessage: using contextEngineId {contextEngineId!r} contextName {contextName!r}"
        )

        # rfc3412: 7.1.6
        scopedPDU = (contextEngineId, contextName, pdu)

        msg = self._snmpMsgSpec
        msg.setComponentByPosition(0, self.MESSAGE_PROCESSING_MODEL_ID)
        msg.setComponentByPosition(2)
        msg.getComponentByPosition(2).setComponentByType(
            pdu.tagSet,
            pdu,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # rfc3412: 7.1.7
        globalData = (msg,)

        k = int(securityModel)
        if k in snmpEngine.security_models:
            smHandler = snmpEngine.security_models[k]
        else:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedSecurityModel
            )

        # rfc3412: 7.1.9.a & rfc2576: 5.2.1 --> no-op

        (snmpEngineMaxMessageSize,) = mibBuilder.import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineMaxMessageSize"
        )

        # fix unique request-id right prior PDU serialization
        if pdu.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            # noinspection PyUnboundLocalVariable
            pdu.setComponentByPosition(0, msgID)

        # rfc3412: 7.1.9.b
        (securityParameters, wholeMsg) = smHandler.generate_request_message(
            snmpEngine,
            self.MESSAGE_PROCESSING_MODEL_ID,
            globalData,
            snmpEngineMaxMessageSize.syntax,
            securityModel,
            snmpEngineId,
            securityName,
            securityLevel,
            scopedPDU,
        )

        # return original request-id right after PDU serialization
        if pdu.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            # noinspection PyUnboundLocalVariable
            pdu.setComponentByPosition(0, reqID)

        # rfc3412: 7.1.9.c
        if pdu.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            # XXX rfc bug? why stateReference should be created?
            self._cache.push_by_message_id(
                int(msgID),
                sendPduHandle=sendPduHandle,
                reqID=reqID,
                snmpEngineId=snmpEngineId,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                transportDomain=transportDomain,
                transportAddress=transportAddress,
            )

        communityName = msg.getComponentByPosition(1)  # for observer

        snmpEngine.observer.store_execution_context(
            snmpEngine,
            "rfc2576.prepareOutgoingMessage",
            dict(
                transportDomain=transportDomain,
                transportAddress=transportAddress,
                wholeMsg=wholeMsg,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                communityName=communityName,
                pdu=pdu,
            ),
        )
        snmpEngine.observer.clear_execution_context(
            snmpEngine, "rfc2576.prepareOutgoingMessage"
        )

        return transportDomain, transportAddress, wholeMsg

    # rfc3412: 7.1
    def prepare_response_message(
        self,
        snmpEngine: "SnmpEngine",
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
        mibBuilder = snmpEngine.get_mib_builder()

        (snmpEngineId,) = mibBuilder.import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
        )
        snmpEngineId = snmpEngineId.syntax

        # rfc3412: 7.1.2.b
        if stateReference is None:
            raise error.StatusInformation(errorIndication=errind.nonReportable)

        cachedParams = self._cache.pop_by_state_reference(stateReference)
        msgID = cachedParams["msgID"]
        reqID = cachedParams["reqID"]
        contextEngineId = cachedParams["contextEngineId"]
        contextName = cachedParams["contextName"]
        securityModel = cachedParams["securityModel"]
        securityName = cachedParams["securityName"]
        securityLevel = cachedParams["securityLevel"]
        securityStateReference = cachedParams["securityStateReference"]
        maxMessageSize = cachedParams["msgMaxSize"]
        transportDomain = cachedParams["transportDomain"]
        transportAddress = cachedParams["transportAddress"]

        debug.logger & debug.FLAG_MP and debug.logger(
            "prepareResponseMessage: cache read msgID {} transportDomain {} transportAddress {} by stateReference {}".format(
                msgID, transportDomain, transportAddress, stateReference
            )
        )

        # rfc3412: 7.1.3
        if statusInformation:
            # rfc3412: 7.1.3a (N/A)

            # rfc3412: 7.1.3b (always discard)
            raise error.StatusInformation(errorIndication=errind.nonReportable)

        # rfc3412: 7.1.4
        # Since there's no SNMP engine identification in v1/2c,
        # set destination contextEngineId to ours
        if not contextEngineId:
            contextEngineId = snmpEngineId

        # rfc3412: 7.1.5
        if not contextName:
            contextName = b""

        # rfc3412: 7.1.6
        scopedPDU = (contextEngineId, contextName, pdu)

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareResponseMessage: using contextEngineId {contextEngineId!r} contextName {contextName!r}"
        )

        msg = self._snmpMsgSpec
        msg.setComponentByPosition(0, messageProcessingModel)
        msg.setComponentByPosition(2)
        msg.getComponentByPosition(2).setComponentByType(
            pdu.tagSet,
            pdu,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

        # att: msgId not set back to PDU as it's up to responder app

        # rfc3412: 7.1.7
        globalData = (msg,)

        k = int(securityModel)
        if k in snmpEngine.security_models:
            smHandler = snmpEngine.security_models[k]
        else:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedSecurityModel
            )

        # set original request-id right prior to PDU serialization
        pdu.setComponentByPosition(0, reqID)

        # rfc3412: 7.1.8.a
        (securityParameters, wholeMsg) = smHandler.generate_response_message(
            snmpEngine,
            self.MESSAGE_PROCESSING_MODEL_ID,
            globalData,
            maxMessageSize,
            securityModel,
            snmpEngineId,
            securityName,
            securityLevel,
            scopedPDU,
            securityStateReference,
            statusInformation.get("errorIndication"),
        )

        # recover unique request-id right after PDU serialization
        pdu.setComponentByPosition(0, msgID)

        snmpEngine.observer.store_execution_context(
            snmpEngine,
            "rfc2576.prepareResponseMessage",
            dict(
                transportDomain=transportDomain,
                transportAddress=transportAddress,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                securityEngineId=snmpEngineId,
                communityName=msg.getComponentByPosition(1),
                pdu=pdu,
            ),
        )
        snmpEngine.observer.clear_execution_context(
            snmpEngine, "rfc2576.prepareResponseMessage"
        )

        return transportDomain, transportAddress, wholeMsg

    # rfc3412: 7.2.1

    def prepare_data_elements(
        self, snmpEngine: "SnmpEngine", transportDomain, transportAddress, wholeMsg
    ):
        """Prepare SNMP message data elements."""
        mibBuilder = snmpEngine.get_mib_builder()

        # rfc3412: 7.2.2
        msg, restOfWholeMsg = decoder.decode(wholeMsg, asn1Spec=self._snmpMsgSpec)

        debug.logger & debug.FLAG_MP and debug.logger(
            f"prepareDataElements: {msg.prettyPrint()}"
        )

        if eoo.endOfOctets.isSameTypeWith(msg):
            raise error.StatusInformation(errorIndication=errind.parseError)

        # rfc3412: 7.2.3
        msgVersion = msg.getComponentByPosition(0)

        # rfc2576: 5.2.1
        (snmpEngineMaxMessageSize,) = mibBuilder.import_symbols(
            "__SNMP-FRAMEWORK-MIB", "snmpEngineMaxMessageSize"
        )
        communityName = msg.getComponentByPosition(1)
        # transportDomain identifies local endpoint
        securityParameters = (communityName, (transportDomain, transportAddress))
        messageProcessingModel = int(msg.getComponentByPosition(0))
        securityModel = messageProcessingModel + 1
        securityLevel = 1

        # rfc3412: 7.2.4 -- 7.2.5 -> no-op
        smHandler: AbstractSecurityModel
        try:
            try:
                smHandler = snmpEngine.security_models[securityModel]

            except KeyError:
                raise error.StatusInformation(
                    errorIndication=errind.unsupportedSecurityModel
                )

            # rfc3412: 7.2.6
            (
                securityEngineId,
                securityName,
                scopedPDU,
                maxSizeResponseScopedPDU,
                securityStateReference,
            ) = smHandler.process_incoming_message(
                snmpEngine,
                messageProcessingModel,
                snmpEngineMaxMessageSize.syntax,
                securityParameters,
                securityModel,
                securityLevel,
                wholeMsg,
                msg,
            )  # type: ignore

            debug.logger & debug.FLAG_MP and debug.logger(
                f"prepareDataElements: SM returned securityEngineId {securityEngineId!r} securityName {securityName!r}"
            )

        except error.StatusInformation:
            statusInformation = sys.exc_info()[1]

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc2576.prepareDataElements:sm-failure",
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
                snmpEngine, "rfc2576.prepareDataElements:sm-failure"
            )

            raise

        # rfc3412: 7.2.6a --> no-op

        # rfc3412: 7.2.7
        contextEngineId, contextName, pdu = scopedPDU

        # rfc2576: 5.2.1
        pduVersion = msgVersion
        pduType = pdu.tagSet

        # rfc3412: 7.2.8, 7.2.9 -> no-op

        # rfc3412: 7.2.10
        if pduType in rfc3411.RESPONSE_CLASS_PDUS:
            # get unique PDU request-id
            msgID = pdu.getComponentByPosition(0)

            # 7.2.10a
            try:
                cachedReqParams = self._cache.pop_by_message_id(int(msgID))
            except error.ProtocolError:
                smHandler.release_state_information(securityStateReference)
                raise error.StatusInformation(errorIndication=errind.dataMismatch)

            # recover original PDU request-id to return to app
            pdu.setComponentByPosition(0, cachedReqParams["reqID"])

            debug.logger & debug.FLAG_MP and debug.logger(
                "prepareDataElements: unique PDU request-id {} replaced with original ID {}".format(
                    msgID, cachedReqParams["reqID"]
                )
            )

            # 7.2.10b
            sendPduHandle = cachedReqParams["sendPduHandle"]
        else:
            sendPduHandle = None

        # no error by default
        statusInformation = None

        # rfc3412: 7.2.11 -> no-op

        # rfc3412: 7.2.12
        if pduType in rfc3411.RESPONSE_CLASS_PDUS:
            # rfc3412: 7.2.12a -> no-op
            # rfc3412: 7.2.12b
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

            stateReference = None

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc2576.prepareDataElements:response",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    securityEngineId=securityEngineId,
                    communityName=communityName,
                    pdu=pdu,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc2576.prepareDataElements:response"
            )

            # rfc3412: 7.2.12c
            smHandler.release_state_information(securityStateReference)

            # rfc3412: 7.2.12d
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

        # rfc3412: 7.2.13
        if pduType in rfc3411.CONFIRMED_CLASS_PDUS:
            # store original PDU request-id and replace it with a unique one
            reqID = pdu.getComponentByPosition(0)
            msgID = self._cache.new_message_id()
            pdu.setComponentByPosition(0, msgID)
            debug.logger & debug.FLAG_MP and debug.logger(
                f"prepareDataElements: received PDU request-id {reqID} replaced with unique ID {msgID}"
            )

            # rfc3412: 7.2.13a
            (snmpEngineId,) = mibBuilder.import_symbols(
                "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
            )
            if securityEngineId != snmpEngineId.syntax:
                smHandler.release_state_information(securityStateReference)
                raise error.StatusInformation(errorIndication=errind.engineIDMismatch)

            # rfc3412: 7.2.13b
            stateReference = self._cache.new_state_reference()
            self._cache.push_by_state_reference(
                stateReference,
                msgVersion=messageProcessingModel,
                msgID=msgID,
                reqID=reqID,
                contextEngineId=contextEngineId,
                contextName=contextName,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                securityStateReference=securityStateReference,
                msgMaxSize=snmpEngineMaxMessageSize.syntax,
                maxSizeResponseScopedPDU=maxSizeResponseScopedPDU,
                transportDomain=transportDomain,
                transportAddress=transportAddress,
            )

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc2576.prepareDataElements:confirmed",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    securityEngineId=securityEngineId,
                    communityName=communityName,
                    pdu=pdu,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc2576.prepareDataElements:confirmed"
            )

            debug.logger & debug.FLAG_MP and debug.logger(
                "prepareDataElements: cached by new stateReference %s" % stateReference
            )

            # rfc3412: 7.2.13c
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

        # rfc3412: 7.2.14
        if pduType in rfc3411.UNCONFIRMED_CLASS_PDUS:
            # Pass new stateReference to let app browse request details
            stateReference = self._cache.new_state_reference()

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc2576.prepareDataElements:unconfirmed",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    securityEngineId=securityEngineId,
                    communityName=communityName,
                    pdu=pdu,
                ),
            )
            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc2576.prepareDataElements:unconfirmed"
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


class SnmpV2cMessageProcessingModel(SnmpV1MessageProcessingModel):
    """Create a message processing model object."""

    MESSAGE_PROCESSING_MODEL_ID = univ.Integer(1)  # SNMPv2c
    SNMP_MSG_SPEC = v2c.Message
