#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys


from pysnmp import debug
from pysnmp.entity.engine import SnmpEngine
from pysnmp.proto import error, rfc3411
from pysnmp.proto.api import v1, v2c  # backend is always SMIv2 compliant
from pysnmp.proto.proxy import rfc2576


# 3.4
class NotificationReceiver:
    """Notification receiver."""

    SUPPORTED_PDU_TYPES = (
        v1.TrapPDU.tagSet,
        v2c.SNMPv2TrapPDU.tagSet,
        v2c.InformRequestPDU.tagSet,
    )

    def __init__(self, snmpEngine: SnmpEngine, cbFun, cbCtx=None):
        """Creates a Notification receiver instance."""
        snmpEngine.message_dispatcher.register_context_engine_id(
            b"", self.SUPPORTED_PDU_TYPES, self.process_pdu  # '' is a wildcard
        )

        self.__snmpTrapCommunity = ""
        self.__cbFun = cbFun
        self.__cbCtx = cbCtx

        def store_snmp_trap_community(snmpEngine, execpoint, variables, cbCtx):
            self.__snmpTrapCommunity = variables.get("communityName", "")

        snmpEngine.observer.register_observer(
            store_snmp_trap_community, "rfc2576.processIncomingMsg"
        )

    def close(self, snmpEngine: SnmpEngine):
        """Unregisters a Notification receiver instance."""
        snmpEngine.message_dispatcher.unregister_context_engine_id(
            b"", self.SUPPORTED_PDU_TYPES
        )
        self.__cbFun = self.__cbCtx = None

    def process_pdu(
        self,
        snmpEngine: SnmpEngine,
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
    ):
        """Processes incoming SNMP PDU."""
        # Agent-side API complies with SMIv2
        if messageProcessingModel == 0:
            origPdu = PDU
            PDU = rfc2576.v1_to_v2(PDU, snmpTrapCommunity=self.__snmpTrapCommunity)
        else:
            origPdu = None

        errorStatus = "noError"
        errorIndex = 0
        varBinds = v2c.apiPDU.get_varbinds(PDU)

        debug.logger & debug.FLAG_APP and debug.logger(
            f"processPdu: stateReference {stateReference}, varBinds {varBinds}"
        )

        # 3.4
        if PDU.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            # 3.4.1 --> no-op

            rspPDU = v2c.apiPDU.get_response(PDU)

            # 3.4.2
            v2c.apiPDU.set_error_status(rspPDU, errorStatus)
            v2c.apiPDU.set_error_index(rspPDU, errorIndex)
            v2c.apiPDU.set_varbinds(rspPDU, varBinds)

            debug.logger & debug.FLAG_APP and debug.logger(
                f"processPdu: stateReference {stateReference}, confirm PDU {rspPDU.prettyPrint()}"
            )

            # Agent-side API complies with SMIv2
            if messageProcessingModel == 0:
                rspPDU = rfc2576.v2_to_v1(rspPDU, origPdu)

            statusInformation = {}

            # 3.4.3
            try:
                snmpEngine.message_dispatcher.return_response_pdu(
                    snmpEngine,
                    messageProcessingModel,
                    securityModel,
                    securityName,
                    securityLevel,
                    contextEngineId,
                    contextName,
                    pduVersion,
                    rspPDU,
                    maxSizeResponseScopedPDU,
                    stateReference,
                    statusInformation,
                )

            except error.StatusInformation:
                debug.logger & debug.FLAG_APP and debug.logger(
                    f"processPdu: stateReference {stateReference}, statusInformation {sys.exc_info()[1]}"
                )
                (snmpSilentDrops,) = snmpEngine.get_mib_builder().import_symbols(
                    "__SNMPv2-MIB", "snmpSilentDrops"
                )
                snmpSilentDrops.syntax += 1

        elif PDU.tagSet in rfc3411.UNCONFIRMED_CLASS_PDUS:
            pass
        else:
            raise error.ProtocolError("Unexpected PDU class %s" % PDU.tagSet)

        debug.logger & debug.FLAG_APP and debug.logger(
            "processPdu: stateReference {}, user cbFun {}, cbCtx {}, varBinds {}".format(
                stateReference, self.__cbFun, self.__cbCtx, varBinds
            )
        )

        self.__cbFun(
            snmpEngine,
            stateReference,
            contextEngineId,
            contextName,
            varBinds,
            self.__cbCtx,
        )
