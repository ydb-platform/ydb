#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys
import warnings
from typing import TYPE_CHECKING


from pyasn1.error import PyAsn1Error
from pysnmp import debug, nextid
from pysnmp.error import PySnmpError
from pysnmp.proto import cache, errind, error
from pysnmp.proto.api import verdec  # XXX
from pysnmp.proto.mpmod.base import AbstractMessageProcessingModel
from pysnmp.smi import builder, instrum

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine


class MsgAndPduDispatcher:
    r"""SNMP engine PDU & message dispatcher.

    Exchanges SNMP PDU's with
    applications and serialized messages with transport level.
    """

    mib_instrum_controller: instrum.MibInstrumController

    def __init__(
        self, mibInstrumController: "instrum.MibInstrumController | None" = None
    ):
        """Create a dispatcher object."""
        if mibInstrumController is None:
            self.mib_instrum_controller = instrum.MibInstrumController(
                builder.MibBuilder()
            )
        else:
            self.mib_instrum_controller = mibInstrumController

        self.mib_instrum_controller.get_mib_builder().load_modules(
            "SNMPv2-MIB",
            "SNMP-MPD-MIB",
            "SNMP-COMMUNITY-MIB",
            "SNMP-TARGET-MIB",
            "SNMP-USER-BASED-SM-MIB",
        )

        # Requests cache
        self.__cache = cache.Cache()

        # Registered context engine IDs
        self.__appsRegistration = {}

        # Source of sendPduHandle and cache of requesting apps
        self.__sendPduHandle = nextid.Integer(0xFFFFFF)

        # To pass transport info to app (legacy)
        self.__transportInfo = {}

    # legacy
    def get_transport_info(self, stateReference):
        """Return transport info for a given stateReference."""
        if stateReference in self.__transportInfo:
            return self.__transportInfo[stateReference]
        else:
            raise error.ProtocolError("No data for stateReference %s" % stateReference)

    # Application registration with dispatcher

    # 4.3.1
    def register_context_engine_id(self, contextEngineId, pduTypes, processPdu):
        """Register application with dispatcher."""
        # 4.3.2 -> no-op

        # 4.3.3
        for pduType in pduTypes:
            k = (contextEngineId, pduType)
            if k in self.__appsRegistration:
                raise error.ProtocolError(
                    f"Duplicate registration {contextEngineId!r}/{pduType}"
                )

            # 4.3.4
            self.__appsRegistration[k] = processPdu

        debug.logger & debug.FLAG_DSP and debug.logger(
            f"registerContextEngineId: contextEngineId {contextEngineId!r} pduTypes {pduTypes}"
        )

    # 4.4.1
    def unregister_context_engine_id(self, contextEngineId, pduTypes):
        """Unregister application with dispatcher."""
        # 4.3.4
        if contextEngineId is None:
            # Default to local snmpEngineId
            (contextEngineId,) = self.mib_instrum_controller.get_mib_builder().import_symbols(  # type: ignore
                "__SNMP-FRAMEWORK-MIB", "snmpEngineID"  # type: ignore
            )

        for pduType in pduTypes:
            k = (contextEngineId, pduType)
            if k in self.__appsRegistration:
                del self.__appsRegistration[k]

        debug.logger & debug.FLAG_DSP and debug.logger(
            f"unregisterContextEngineId: contextEngineId {contextEngineId!r} pduTypes {pduTypes}"
        )

    def get_registered_app(self, contextEngineId, pduType):
        """Return registered application."""
        k = (contextEngineId, pduType)
        if k in self.__appsRegistration:
            return self.__appsRegistration[k]
        k = (b"", pduType)
        if k in self.__appsRegistration:
            return self.__appsRegistration[k]  # wildcard

    # Dispatcher <-> application API

    # 4.1.1

    def send_pdu(
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
        PDU,
        expectResponse,
        timeout: float = 0,
        cbFun=None,
        cbCtx=None,
    ):
        """PDU dispatcher -- prepare and serialize a request or notification."""
        # 4.1.1.2
        k = int(messageProcessingModel)
        if k in snmpEngine.message_processing_subsystems:
            mpHandler = snmpEngine.message_processing_subsystems[k]
        else:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedMsgProcessingModel
            )

        debug.logger & debug.FLAG_DSP and debug.logger(
            f"sendPdu: securityName {securityName}, PDU\n{PDU.prettyPrint()}"
        )

        # 4.1.1.3
        sendPduHandle = self.__sendPduHandle()
        if expectResponse:
            self.__cache.add(
                sendPduHandle,
                messageProcessingModel=messageProcessingModel,
                sendPduHandle=sendPduHandle,
                timeout=timeout + snmpEngine.transport_dispatcher.get_timer_ticks(),
                cbFun=cbFun,
                cbCtx=cbCtx,
            )

            debug.logger & debug.FLAG_DSP and debug.logger(
                "sendPdu: current time %d ticks, one tick is %s seconds"
                % (
                    snmpEngine.transport_dispatcher.get_timer_ticks(),
                    snmpEngine.transport_dispatcher.get_timer_resolution(),
                )
            )

        debug.logger & debug.FLAG_DSP and debug.logger(
            f"sendPdu: new sendPduHandle {sendPduHandle}, timeout {timeout} ticks, cbFun {cbFun}"
        )

        origTransportDomain = transportDomain
        origTransportAddress = transportAddress

        # 4.1.1.4 & 4.1.1.5
        try:
            (
                transportDomain,
                transportAddress,
                outgoingMessage,
            ) = mpHandler.prepare_outgoing_message(
                snmpEngine,
                origTransportDomain,
                origTransportAddress,
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                PDU,
                expectResponse,
                sendPduHandle,
            )  # type: ignore

            debug.logger & debug.FLAG_DSP and debug.logger("sendPdu: MP succeeded")
        except PySnmpError:
            if expectResponse:
                self.__cache.pop(sendPduHandle)
                self.release_state_information(
                    snmpEngine, sendPduHandle, messageProcessingModel
                )
            raise

        # 4.1.1.6
        if snmpEngine.transport_dispatcher is None:
            if expectResponse:
                self.__cache.pop(sendPduHandle)

            raise error.PySnmpError("Transport dispatcher not set")

        snmpEngine.observer.store_execution_context(
            snmpEngine,
            "rfc3412.sendPdu",
            dict(
                transportDomain=transportDomain,
                transportAddress=transportAddress,
                outgoingMessage=outgoingMessage,
                messageProcessingModel=messageProcessingModel,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                pdu=PDU,
            ),
        )

        try:
            snmpEngine.transport_dispatcher.send_message(
                outgoingMessage, transportDomain, transportAddress
            )
        except PySnmpError:
            if expectResponse:
                self.__cache.pop(sendPduHandle)
            raise

        snmpEngine.observer.clear_execution_context(snmpEngine, "rfc3412.sendPdu")

        # Update cache with orignal req params (used for retrying)
        if expectResponse:
            self.__cache.update(
                sendPduHandle,
                transportDomain=origTransportDomain,
                transportAddress=origTransportAddress,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                pduVersion=pduVersion,
                PDU=PDU,
            )

        return sendPduHandle

    # 4.1.2.1
    def return_response_pdu(
        self,
        snmpEngine: "SnmpEngine",
        messageProcessingModel,
        securityModel,
        securityName,
        securityLevel,
        contextEngineId,
        contextName,
        pduVersion,
        PDU,
        maxSizeResponseScopedPDU,
        stateReference,
        statusInformation,
    ):
        """PDU dispatcher -- prepare and serialize a response."""
        # Extract input values and initialize defaults
        k = int(messageProcessingModel)
        if k in snmpEngine.message_processing_subsystems:
            mpHandler = snmpEngine.message_processing_subsystems[k]
        else:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedMsgProcessingModel
            )

        debug.logger & debug.FLAG_DSP and debug.logger(
            "returnResponsePdu: PDU {}".format(PDU and PDU.prettyPrint() or "<empty>")
        )

        # 4.1.2.2
        try:
            (
                transportDomain,
                transportAddress,
                outgoingMessage,
            ) = mpHandler.prepare_response_message(
                snmpEngine,
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                PDU,
                maxSizeResponseScopedPDU,
                stateReference,
                statusInformation,
            )  # type: ignore

            debug.logger & debug.FLAG_DSP and debug.logger(
                "returnResponsePdu: MP suceeded"
            )

        except error.StatusInformation:
            # 4.1.2.3
            raise

        # Handle oversized messages XXX transport constrains?
        (
            snmpEngineMaxMessageSize,
        ) = self.mib_instrum_controller.get_mib_builder().import_symbols(  # type: ignore
            "__SNMP-FRAMEWORK-MIB", "snmpEngineMaxMessageSize"  # type: ignore
        )
        if (
            snmpEngineMaxMessageSize.syntax
            and len(outgoingMessage) > snmpEngineMaxMessageSize.syntax
        ):
            (snmpSilentDrops,) = self.mib_instrum_controller.get_mib_builder().import_symbols("__SNMPv2-MIB", "snmpSilentDrops")  # type: ignore
            snmpSilentDrops.syntax += 1
            raise error.StatusInformation(errorIndication=errind.tooBig)

        snmpEngine.observer.store_execution_context(
            snmpEngine,
            "rfc3412.returnResponsePdu",
            dict(
                transportDomain=transportDomain,
                transportAddress=transportAddress,
                outgoingMessage=outgoingMessage,
                messageProcessingModel=messageProcessingModel,
                securityModel=securityModel,
                securityName=securityName,
                securityLevel=securityLevel,
                contextEngineId=contextEngineId,
                contextName=contextName,
                pdu=PDU,
            ),
        )

        # 4.1.2.4
        snmpEngine.transport_dispatcher.send_message(
            outgoingMessage, transportDomain, transportAddress
        )

        snmpEngine.observer.clear_execution_context(
            snmpEngine, "rfc3412.returnResponsePdu"
        )

    # 4.2.1
    def receive_message(
        self, snmpEngine: "SnmpEngine", transportDomain, transportAddress, wholeMsg
    ):
        """Message dispatcher -- de-serialize message into PDU."""
        # 4.2.1.1
        (snmpInPkts,) = self.mib_instrum_controller.get_mib_builder().import_symbols(  # type: ignore
            "__SNMPv2-MIB", "snmpInPkts"
        )
        snmpInPkts.syntax += 1

        # 4.2.1.2
        try:
            restOfWholeMsg = b""  # XXX fix decoder non-recursive return
            msgVersion = verdec.decode_message_version(wholeMsg)

        except error.ProtocolError:
            (
                snmpInASNParseErrs,
            ) = self.mib_instrum_controller.get_mib_builder().import_symbols(  # type: ignore
                "__SNMPv2-MIB", "snmpInASNParseErrs"  # type: ignore
            )
            snmpInASNParseErrs.syntax += 1
            return b""  # n.b the whole buffer gets dropped

        debug.logger & debug.FLAG_DSP and debug.logger(
            "receiveMessage: msgVersion %s, msg decoded" % msgVersion
        )

        messageProcessingModel = msgVersion
        mpHandler: "AbstractMessageProcessingModel"

        try:
            mpHandler = snmpEngine.message_processing_subsystems[messageProcessingModel]

        except KeyError:
            (snmpInBadVersions,) = self.mib_instrum_controller.get_mib_builder().import_symbols("__SNMPv2-MIB", "snmpInBadVersions")  # type: ignore
            snmpInBadVersions.syntax += 1
            return restOfWholeMsg

        # 4.2.1.3 -- no-op

        # 4.2.1.4
        try:
            (
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                PDU,
                pduType,
                sendPduHandle,
                maxSizeResponseScopedPDU,
                statusInformation,
                stateReference,
            ) = mpHandler.prepare_data_elements(
                snmpEngine, transportDomain, transportAddress, wholeMsg
            )  # type: ignore

            debug.logger & debug.FLAG_DSP and debug.logger(
                "receiveMessage: MP succeded"
            )

        except error.StatusInformation:
            statusInformation = sys.exc_info()[1]
            if "sendPduHandle" in statusInformation:  # type: ignore
                # Dropped REPORT -- re-run pending reqs queue as some
                # of them may be waiting for this REPORT
                debug.logger & debug.FLAG_DSP and debug.logger(
                    "receiveMessage: MP failed, statusInformation %s, forcing a retry"
                    % statusInformation
                )
                self.__expire_request(
                    statusInformation["sendPduHandle"],  # type: ignore
                    self.__cache.pop(statusInformation["sendPduHandle"]),  # type: ignore
                    snmpEngine,
                    statusInformation,
                )
            return restOfWholeMsg

        except PyAsn1Error:
            debug.logger & debug.FLAG_MP and debug.logger(
                f"receiveMessage: {sys.exc_info()[1]}"
            )
            (snmpInASNParseErrs,) = snmpEngine.get_mib_builder().import_symbols(  # type: ignore
                "__SNMPv2-MIB", "snmpInASNParseErrs"
            )
            snmpInASNParseErrs.syntax += 1

            return restOfWholeMsg

        debug.logger & debug.FLAG_DSP and debug.logger(
            "receiveMessage: PDU %s" % PDU.prettyPrint()
        )

        # 4.2.2
        if sendPduHandle is None:
            # 4.2.2.1 (request or notification)

            debug.logger & debug.FLAG_DSP and debug.logger(
                "receiveMessage: pduType %s" % pduType
            )
            # 4.2.2.1.1
            processPdu = self.get_registered_app(contextEngineId, pduType)

            # 4.2.2.1.2
            if processPdu is None:
                # 4.2.2.1.2.a
                (
                    snmpUnknownPDUHandlers,
                ) = self.mib_instrum_controller.get_mib_builder().import_symbols(
                    "__SNMP-MPD-MIB", "snmpUnknownPDUHandlers"  # type: ignore
                )
                snmpUnknownPDUHandlers.syntax += 1

                # 4.2.2.1.2.b
                statusInformation = {
                    "errorIndication": errind.unknownPDUHandler,
                    "oid": snmpUnknownPDUHandlers.name,
                    "val": snmpUnknownPDUHandlers.syntax,
                }

                debug.logger & debug.FLAG_DSP and debug.logger(
                    "receiveMessage: unhandled PDU type"
                )

                # 4.2.2.1.2.c
                try:
                    (
                        destTransportDomain,
                        destTransportAddress,
                        outgoingMessage,
                    ) = mpHandler.prepare_response_message(
                        snmpEngine,
                        messageProcessingModel,
                        securityModel,
                        securityName,
                        securityLevel,
                        contextEngineId,
                        contextName,
                        pduVersion,
                        PDU,
                        maxSizeResponseScopedPDU,
                        stateReference,
                        statusInformation,
                    )

                    snmpEngine.transport_dispatcher.send_message(
                        outgoingMessage, destTransportDomain, destTransportAddress
                    )

                except PySnmpError:
                    debug.logger & debug.FLAG_DSP and debug.logger(
                        "receiveMessage: report failed, statusInformation %s"
                        % sys.exc_info()[1]
                    )

                else:
                    debug.logger & debug.FLAG_DSP and debug.logger(
                        "receiveMessage: reporting succeeded"
                    )

                # 4.2.2.1.2.d
                return restOfWholeMsg

            else:
                snmpEngine.observer.store_execution_context(
                    snmpEngine,
                    "rfc3412.receiveMessage:request",
                    dict(
                        transportDomain=transportDomain,
                        transportAddress=transportAddress,
                        wholeMsg=wholeMsg,
                        messageProcessingModel=messageProcessingModel,
                        securityModel=securityModel,
                        securityName=securityName,
                        securityLevel=securityLevel,
                        contextEngineId=contextEngineId,
                        contextName=contextName,
                        pdu=PDU,
                    ),
                )

                # pass transport info to app (legacy)
                if stateReference is not None:
                    self.__transportInfo[stateReference] = (
                        transportDomain,
                        transportAddress,
                    )

                # 4.2.2.1.3
                processPdu(
                    snmpEngine,
                    messageProcessingModel,
                    securityModel,
                    securityName,
                    securityLevel,
                    contextEngineId,
                    contextName,
                    pduVersion,
                    PDU,
                    maxSizeResponseScopedPDU,
                    stateReference,
                )

                snmpEngine.observer.clear_execution_context(
                    snmpEngine, "rfc3412.receiveMessage:request"
                )

                # legacy
                if stateReference is not None:
                    del self.__transportInfo[stateReference]

                debug.logger & debug.FLAG_DSP and debug.logger(
                    "receiveMessage: processPdu succeeded"
                )
                return restOfWholeMsg
        else:
            # 4.2.2.2 (response)

            # 4.2.2.2.1
            cachedParams = self.__cache.pop(sendPduHandle)

            # 4.2.2.2.2
            if cachedParams is None:
                (
                    snmpUnknownPDUHandlers,
                ) = self.mib_instrum_controller.get_mib_builder().import_symbols(
                    "__SNMP-MPD-MIB", "snmpUnknownPDUHandlers"  # type: ignore
                )
                snmpUnknownPDUHandlers.syntax += 1
                return restOfWholeMsg

            debug.logger & debug.FLAG_DSP and debug.logger(
                "receiveMessage: cache read by sendPduHandle %s" % sendPduHandle
            )

            # 4.2.2.2.3
            # no-op ? XXX

            snmpEngine.observer.store_execution_context(
                snmpEngine,
                "rfc3412.receiveMessage:response",
                dict(
                    transportDomain=transportDomain,
                    transportAddress=transportAddress,
                    wholeMsg=wholeMsg,
                    messageProcessingModel=messageProcessingModel,
                    securityModel=securityModel,
                    securityName=securityName,
                    securityLevel=securityLevel,
                    contextEngineId=contextEngineId,
                    contextName=contextName,
                    pdu=PDU,
                ),
            )

            # 4.2.2.2.4
            processResponsePdu = cachedParams["cbFun"]

            processResponsePdu(
                snmpEngine,
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                PDU,
                statusInformation,
                cachedParams["sendPduHandle"],
                cachedParams["cbCtx"],
            )

            snmpEngine.observer.clear_execution_context(
                snmpEngine, "rfc3412.receiveMessage:response"
            )

            debug.logger & debug.FLAG_DSP and debug.logger(
                "receiveMessage: processResponsePdu succeeded"
            )

            return restOfWholeMsg

    def release_state_information(
        self, snmpEngine: "SnmpEngine", sendPduHandle, messageProcessingModel
    ):
        """Release state information."""
        k = int(messageProcessingModel)
        if k in snmpEngine.message_processing_subsystems:
            mpHandler = snmpEngine.message_processing_subsystems[k]
            mpHandler.release_state_information(sendPduHandle)

        self.__cache.pop(sendPduHandle)

    # Cache expiration stuff

    # noinspection PyUnusedLocal
    def __expire_request(
        self, cacheKey, cachedParams, snmpEngine: "SnmpEngine", statusInformation=None
    ):
        timeNow = snmpEngine.transport_dispatcher.get_timer_ticks()
        timeoutAt = cachedParams["timeout"]

        if statusInformation is None and timeNow < timeoutAt:
            return

        processResponsePdu = cachedParams["cbFun"]

        debug.logger & debug.FLAG_DSP and debug.logger(
            "__expireRequest: req cachedParams %s" % cachedParams
        )

        # Fail timed-out requests
        if not statusInformation:
            statusInformation = error.StatusInformation(
                errorIndication=errind.requestTimedOut
            )

        self.release_state_information(
            snmpEngine,
            cachedParams["sendPduHandle"],
            cachedParams["messageProcessingModel"],
        )

        processResponsePdu(
            snmpEngine,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            statusInformation,
            cachedParams["sendPduHandle"],
            cachedParams["cbCtx"],
        )
        return True

    # noinspection PyUnusedLocal
    def receive_timer_tick(self, snmpEngine: "SnmpEngine", timeNow):
        """Process cache timeouts."""
        self.__cache.expire(self.__expire_request, snmpEngine)

    # Old to new attribute mapping
    deprecated_attributes = {
        "mibInstrumController": "mib_instrum_controller",
        "registerContextEngineId": "register_context_engine_id",
        "unregisterContextEngineId": "unregister_context_engine_id",
        "getRegisteredApp": "get_registered_app",
        "sendPdu": "send_pdu",
        "returnResponsePdu": "return_response_pdu",
        "receiveMessage": "receive_message",
        "releaseStateInformation": "release_state_information",
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
