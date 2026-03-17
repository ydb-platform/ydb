#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys

from pysnmp import debug
from pysnmp.entity.engine import SnmpEngine
from pysnmp.entity.rfc3413.context import SnmpContext
from pysnmp.proto import errind, error, rfc1902, rfc1905, rfc3411
from pysnmp.proto.api import v2c  # backend is always SMIv2 compliant
from pysnmp.proto.proxy import rfc2576
from pysnmp.smi import error as smi_error


# 3.2
class CommandResponderBase:
    """SNMP command responder base class."""

    ACM_ID = 3  # default MIB access control method to use
    SUPPORTED_PDU_TYPES = ()

    SMI_ERROR_MAP = {
        smi_error.TooBigError: "tooBig",
        smi_error.NoSuchNameError: "noSuchName",
        smi_error.BadValueError: "badValue",
        smi_error.ReadOnlyError: "readOnly",
        smi_error.GenError: "genErr",
        smi_error.NoAccessError: "noAccess",
        smi_error.WrongTypeError: "wrongType",
        smi_error.WrongLengthError: "wrongLength",
        smi_error.WrongEncodingError: "wrongEncoding",
        smi_error.WrongValueError: "wrongValue",
        smi_error.NoCreationError: "noCreation",
        smi_error.InconsistentValueError: "inconsistentValue",
        smi_error.ResourceUnavailableError: "resourceUnavailable",
        smi_error.CommitFailedError: "commitFailed",
        smi_error.UndoFailedError: "undoFailed",
        smi_error.AuthorizationError: "authorizationError",
        smi_error.NotWritableError: "notWritable",
        smi_error.InconsistentNameError: "inconsistentName",
    }

    def __init__(self, snmpEngine: SnmpEngine, snmpContext: SnmpContext, cbCtx=None):
        """Create a responder object."""
        snmpEngine.message_dispatcher.register_context_engine_id(
            snmpContext.contextEngineId, self.SUPPORTED_PDU_TYPES, self.process_pdu
        )
        self.snmpContext = snmpContext
        self.cbCtx = cbCtx
        self.__pendingReqs = {}

    def handle_management_operation(
        self, snmpEngine, stateReference, contextName, PDU, acCtx
    ):
        """Handle incoming SNMP PDU."""
        pass

    def close(self, snmpEngine: SnmpEngine):
        """Unregister responder object."""
        snmpEngine.message_dispatcher.unregister_context_engine_id(
            self.snmpContext.contextEngineId, self.SUPPORTED_PDU_TYPES
        )
        self.snmpContext = self.__pendingReqs = None

    def send_varbinds(
        self, snmpEngine: SnmpEngine, stateReference, errorStatus, errorIndex, varBinds
    ):
        """Send VarBinds."""
        (
            messageProcessingModel,
            securityModel,
            securityName,
            securityLevel,
            contextEngineId,
            contextName,
            pduVersion,
            PDU,
            origPdu,
            maxSizeResponseScopedPDU,
            statusInformation,
        ) = self.__pendingReqs[stateReference]

        v2c.apiPDU.set_error_status(PDU, errorStatus)
        v2c.apiPDU.set_error_index(PDU, errorIndex)
        v2c.apiPDU.set_varbinds(PDU, varBinds)

        debug.logger & debug.FLAG_APP and debug.logger(
            "sendVarBinds: stateReference {}, errorStatus {}, errorIndex {}, varBinds {}".format(
                stateReference, errorStatus, errorIndex, varBinds
            )
        )

        self.send_pdu(snmpEngine, stateReference, PDU)

    def send_pdu(self, snmpEngine: SnmpEngine, stateReference, PDU):
        """Send PDU."""
        (
            messageProcessingModel,
            securityModel,
            securityName,
            securityLevel,
            contextEngineId,
            contextName,
            pduVersion,
            _,
            origPdu,
            maxSizeResponseScopedPDU,
            statusInformation,
        ) = self.__pendingReqs[stateReference]

        # Agent-side API complies with SMIv2
        if messageProcessingModel == 0:
            PDU = rfc2576.v2_to_v1(PDU, origPdu)

        # 3.2.6
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
                PDU,
                maxSizeResponseScopedPDU,
                stateReference,
                statusInformation,
            )

        except error.StatusInformation:
            debug.logger & debug.FLAG_APP and debug.logger(
                f"sendPdu: stateReference {stateReference}, statusInformation {sys.exc_info()[1]}"
            )
            (snmpSilentDrops,) = snmpEngine.get_mib_builder().import_symbols(  # type: ignore
                "__SNMPv2-MIB", "snmpSilentDrops"
            )
            snmpSilentDrops.syntax += 1

    _get_request_type = rfc1905.GetRequestPDU.tagSet
    _next_request_type = rfc1905.GetNextRequestPDU.tagSet
    _set_request_type = rfc1905.SetRequestPDU.tagSet
    _counter64_type = rfc1902.Counter64.tagSet

    def release_state_information(self, stateReference):
        """Release state information."""
        if stateReference in self.__pendingReqs:
            del self.__pendingReqs[stateReference]

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
        """Process incoming PDU."""
        # Agent-side API complies with SMIv2
        if messageProcessingModel == 0:
            origPdu = PDU
            PDU = rfc2576.v1_to_v2(PDU)
        else:
            origPdu = None

        # 3.2.1
        if (
            PDU.tagSet not in rfc3411.READ_CLASS_PDUS
            and PDU.tagSet not in rfc3411.WRITE_CLASS_PDUS
        ):
            raise error.ProtocolError("Unexpected PDU class %s" % PDU.tagSet)

        # 3.2.2 --> no-op

        # 3.2.4
        rspPDU = v2c.apiPDU.get_response(PDU)

        statusInformation = {}

        self.__pendingReqs[stateReference] = (
            messageProcessingModel,
            securityModel,
            securityName,
            securityLevel,
            contextEngineId,
            contextName,
            pduVersion,
            rspPDU,
            origPdu,
            maxSizeResponseScopedPDU,
            statusInformation,
        )

        # 3.2.5
        varBinds = v2c.apiPDU.get_varbinds(PDU)

        debug.logger & debug.FLAG_APP and debug.logger(
            f"processPdu: stateReference {stateReference}, varBinds {varBinds}"
        )

        try:
            self.handle_management_operation(
                snmpEngine, stateReference, contextName, PDU
            )

        # SNMPv2 SMI exceptions
        except smi_error.SmiError:
            errorIndication = sys.exc_info()[1]

            debug.logger & debug.FLAG_APP and debug.logger(
                f"processPdu: stateReference {stateReference}, errorIndication {errorIndication}"
            )
            if "oid" in errorIndication:
                # Request REPORT generation
                statusInformation["oid"] = errorIndication["oid"]
                statusInformation["val"] = errorIndication["val"]

            errorStatus = self.SMI_ERROR_MAP.get(errorIndication.__class__, "genErr")

            try:
                errorIndex = errorIndication["idx"] + 1

            except KeyError:
                errorIndex = 1

            if len(varBinds) > errorIndex:
                errorIndex = 1

            # rfc1905: 4.2.1.3
            if errorStatus == "tooBig":
                errorIndex = 0
                varBinds = []

            # Report error
            self.send_varbinds(
                snmpEngine, stateReference, errorStatus, errorIndex, varBinds
            )

        except smi_error.PySnmpError:
            debug.logger & debug.FLAG_APP and debug.logger(
                "processPdu: stateReference %s, error "
                "%s" % (stateReference, sys.exc_info()[1])
            )

        self.release_state_information(stateReference)

    @classmethod
    def verify_access(cls, viewType, varBind, **context) -> "bool | None":
        """Verify access rights for a single OID-value pair."""
        name, val = varBind

        snmpEngine: SnmpEngine = context["snmpEngine"]

        execCtx = snmpEngine.observer.get_execution_context(
            "rfc3412.receiveMessage:request"
        )
        (securityModel, securityName, securityLevel, contextName, pduType) = (
            execCtx["securityModel"],
            execCtx["securityName"],
            execCtx["securityLevel"],
            execCtx["contextName"],
            execCtx["pdu"].getTagSet(),
        )

        try:
            snmpEngine.access_control_model[cls.ACM_ID].is_access_allowed(
                snmpEngine,
                securityModel,
                securityName,
                securityLevel,
                viewType,
                contextName,
                name,
            )

        # Map ACM errors onto SMI ones
        except error.StatusInformation:
            statusInformation = sys.exc_info()[1]
            debug.logger & debug.FLAG_APP and debug.logger(
                f"__verifyAccess: name {name}, statusInformation {statusInformation}"
            )
            errorIndication = statusInformation["errorIndication"]
            # 3.2.5...
            if (
                errorIndication == errind.noSuchView
                or errorIndication == errind.noAccessEntry
                or errorIndication == errind.noGroupName
            ):
                raise smi_error.AuthorizationError(name=name, idx=context.get("idx"))

            elif errorIndication == errind.otherError:
                raise smi_error.GenError(name=name, idx=context.get("idx"))

            elif errorIndication == errind.noSuchContext:
                (snmpUnknownContexts,) = snmpEngine.get_mib_builder().import_symbols(
                    "__SNMP-TARGET-MIB", "snmpUnknownContexts"
                )
                snmpUnknownContexts.syntax += 1
                # Request REPORT generation
                raise smi_error.GenError(
                    name=name,
                    idx=context.get("idx"),
                    oid=snmpUnknownContexts.name,
                    val=snmpUnknownContexts.syntax,
                )

            elif errorIndication == errind.notInView:
                return True

            else:
                raise error.ProtocolError("Unknown ACM error %s" % errorIndication)
        else:
            # rfc2576: 4.1.2.1
            if (
                securityModel == 1
                and val is not None
                and cls._counter64_type == val.getTagSet()
                and cls._next_request_type == pduType
            ):
                # This will cause MibTree to skip this OID-value
                raise smi_error.NoAccessError(name=name, idx=context.get("idx"))


class GetCommandResponder(CommandResponderBase):
    """SNMP GET command responder."""

    SUPPORTED_PDU_TYPES = (rfc1905.GetRequestPDU.tagSet,)

    # rfc1905: 4.2.1
    def handle_management_operation(
        self, snmpEngine: SnmpEngine, stateReference, contextName, PDU
    ):
        """Handle incoming SNMP GetRequest-PDU."""
        # rfc1905: 4.2.1.1
        mgmtFun = self.snmpContext.get_mib_instrum(contextName).read_variables
        varBinds = v2c.apiPDU.get_varbinds(PDU)

        context = dict(
            snmpEngine=snmpEngine, acFun=self.verify_access, cbCtx=self.cbCtx
        )

        rspVarBinds = mgmtFun(*varBinds, **context)

        self.send_varbinds(snmpEngine, stateReference, 0, 0, rspVarBinds)
        self.release_state_information(stateReference)


class NextCommandResponder(CommandResponderBase):
    """SNMP GETNEXT command responder."""

    SUPPORTED_PDU_TYPES = (rfc1905.GetNextRequestPDU.tagSet,)

    # rfc1905: 4.2.2
    def handle_management_operation(
        self, snmpEngine: SnmpEngine, stateReference, contextName, PDU
    ):
        """Handle incoming SNMP GetNextRequest-PDU."""
        # rfc1905: 4.2.2.1
        mgmtFun = self.snmpContext.get_mib_instrum(contextName).read_next_variables

        varBinds = v2c.apiPDU.get_varbinds(PDU)

        context = dict(
            snmpEngine=snmpEngine, acFun=self.verify_access, cbCtx=self.cbCtx
        )

        while True:
            rspVarBinds = mgmtFun(*varBinds, **context)

            try:
                self.send_varbinds(snmpEngine, stateReference, 0, 0, rspVarBinds)

            except error.StatusInformation:
                idx = sys.exc_info()[1]["idx"]
                varBinds[idx] = (rspVarBinds[idx][0], varBinds[idx][1])
            else:
                break

        self.release_state_information(stateReference)


class BulkCommandResponder(CommandResponderBase):
    """SNMP GETBULK command responder."""

    SUPPORTED_PDU_TYPES = (rfc1905.GetBulkRequestPDU.tagSet,)
    max_varbinds = 64

    # rfc1905: 4.2.3
    def handle_management_operation(
        self, snmpEngine: SnmpEngine, stateReference, contextName, PDU
    ):
        """Handle incoming SNMP GetBulkRequest-PDU."""
        nonRepeaters = v2c.apiBulkPDU.get_non_repeaters(PDU)
        if nonRepeaters < 0:
            nonRepeaters = 0

        maxRepetitions = v2c.apiBulkPDU.get_max_repetitions(PDU)
        if maxRepetitions < 0:
            maxRepetitions = 0

        reqVarBinds = v2c.apiPDU.get_varbinds(PDU)

        N = min(int(nonRepeaters), len(reqVarBinds))
        M = int(maxRepetitions)
        R = max(len(reqVarBinds) - N, 0)

        if R:
            M = min(M, self.max_varbinds // R)

        debug.logger & debug.FLAG_APP and debug.logger(
            "handleMgmtOperation: N %d, M %d, R %d" % (N, M, R)
        )

        mgmtFun = self.snmpContext.get_mib_instrum(contextName).read_next_variables

        context = dict(
            snmpEngine=snmpEngine, acFun=self.verify_access, cbCtx=self.cbCtx
        )

        if N:
            # TODO(etingof): manage all PDU var-binds in a single call
            rspVarBinds = mgmtFun(*reqVarBinds[:N], **context)

        else:
            rspVarBinds = []

        varBinds = reqVarBinds[-R:]

        while M and R:
            rspVarBinds.extend(mgmtFun(*varBinds, **context))
            varBinds = rspVarBinds[-R:]
            M -= 1

        if len(rspVarBinds):
            self.send_varbinds(snmpEngine, stateReference, 0, 0, rspVarBinds)
            self.release_state_information(stateReference)
        else:
            raise smi_error.SmiError()


class SetCommandResponder(CommandResponderBase):
    """SNMP SET command responder."""

    SUPPORTED_PDU_TYPES = (rfc1905.SetRequestPDU.tagSet,)

    # rfc1905: 4.2.5
    def handle_management_operation(
        self, snmpEngine: SnmpEngine, stateReference, contextName, PDU
    ):
        """Handle incoming SNMP SetRequest-PDU."""
        mgmtFun = self.snmpContext.get_mib_instrum(contextName).write_variables

        varBinds = v2c.apiPDU.get_varbinds(PDU)

        instrumError = None

        context = dict(
            snmpEngine=snmpEngine, acFun=self.verify_access, cbCtx=self.cbCtx
        )

        # rfc1905: 4.2.5.1-13
        try:
            rspVarBinds = mgmtFun(*varBinds, **context)

        except (
            smi_error.NoSuchObjectError,
            smi_error.NoSuchInstanceError,
        ):
            instrumError = smi_error.NotWritableError()
            instrumError.update(sys.exc_info()[1])

        else:
            self.send_varbinds(snmpEngine, stateReference, 0, 0, rspVarBinds)

        self.release_state_information(stateReference)

        if instrumError:
            raise instrumError
