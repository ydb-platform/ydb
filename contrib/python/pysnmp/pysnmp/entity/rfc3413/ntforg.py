#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys


from pysnmp import debug, nextid
from pysnmp.entity.engine import SnmpEngine
from pysnmp.entity.rfc3413 import config
from pysnmp.proto import errind, error, rfc3411
from pysnmp.proto.api import v2c
from pysnmp.proto.proxy import rfc2576
from pysnmp.smi.rfc1902 import ObjectType


getNextHandle = nextid.Integer(0x7FFFFFFF)  # noqa: N816


class NotificationOriginator:
    """Notification originator."""

    ACM_ID = 3  # default MIB access control method to use

    def __init__(self, **options):
        """Create a Notification Originator application."""
        self.__pendingReqs = {}
        self.__pendingNotifications = {}
        self.snmpContext = options.pop("snmpContext", None)  # this is deprecated
        self.__options = options

    def process_response_pdu(
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
        statusInformation,
        sendPduHandle,
        cbInfo,
    ):
        """Process SNMP PDU response."""
        sendRequestHandle, cbFun, cbCtx = cbInfo

        # 3.3.6d
        if sendPduHandle not in self.__pendingReqs:
            raise error.ProtocolError("Missing sendPduHandle %s" % sendPduHandle)

        (
            origTransportDomain,
            origTransportAddress,
            origMessageProcessingModel,
            origSecurityModel,
            origSecurityName,
            origSecurityLevel,
            origContextEngineId,
            origContextName,
            origPdu,
            origTimeout,
            origRetryCount,
            origRetries,
            origDiscoveryRetries,
        ) = self.__pendingReqs.pop(sendPduHandle)

        snmpEngine.transport_dispatcher.job_finished(id(self))

        if statusInformation:
            debug.logger & debug.FLAG_APP and debug.logger(
                "processResponsePdu: sendRequestHandle {}, sendPduHandle {} statusInformation {}".format(
                    sendRequestHandle, sendPduHandle, statusInformation
                )
            )

            errorIndication = statusInformation["errorIndication"]

            if errorIndication in (errind.notInTimeWindow, errind.unknownEngineID):
                origDiscoveryRetries += 1
                origRetries = 0
            else:
                origDiscoveryRetries = 0
                origRetries += 1

            if (
                origRetries > origRetryCount
                or origDiscoveryRetries > self.__options.get("discoveryRetries", 4)
            ):
                debug.logger & debug.FLAG_APP and debug.logger(
                    "processResponsePdu: sendRequestHandle %s, sendPduHandle %s retry count %d exceeded"
                    % (sendRequestHandle, sendPduHandle, origRetries)
                )
                cbFun(snmpEngine, sendRequestHandle, errorIndication, None, cbCtx)
                return

            # Convert timeout in seconds into timeout in timer ticks
            timeoutInTicks = (
                float(origTimeout)
                / 100
                / snmpEngine.transport_dispatcher.get_timer_resolution()
            )

            # User-side API assumes SMIv2
            if messageProcessingModel == 0:
                reqPDU = rfc2576.v2_to_v1(origPdu)
                pduVersion = 0
            else:
                reqPDU = origPdu
                pduVersion = 1

            # 3.3.6a
            try:
                sendPduHandle = snmpEngine.message_dispatcher.send_pdu(
                    snmpEngine,
                    origTransportDomain,
                    origTransportAddress,
                    origMessageProcessingModel,
                    origSecurityModel,
                    origSecurityName,
                    origSecurityLevel,
                    origContextEngineId,
                    origContextName,
                    pduVersion,
                    reqPDU,
                    True,
                    timeoutInTicks,
                    self.process_response_pdu,
                    (sendRequestHandle, cbFun, cbCtx),
                )
            except error.StatusInformation:
                statusInformation = sys.exc_info()[1]
                debug.logger & debug.FLAG_APP and debug.logger(
                    "processResponsePdu: sendRequestHandle {}: sendPdu() failed with {!r} ".format(
                        sendRequestHandle, statusInformation
                    )
                )
                cbFun(
                    snmpEngine,
                    sendRequestHandle,
                    statusInformation["errorIndication"],
                    None,
                    cbCtx,
                )
                return

            snmpEngine.transport_dispatcher.job_started(id(self))

            debug.logger & debug.FLAG_APP and debug.logger(
                "processResponsePdu: sendRequestHandle %s, sendPduHandle %s, timeout %d, retry %d of %d"
                % (
                    sendRequestHandle,
                    sendPduHandle,
                    origTimeout,
                    origRetries,
                    origRetryCount,
                )
            )

            # 3.3.6b
            self.__pendingReqs[sendPduHandle] = (
                origTransportDomain,
                origTransportAddress,
                origMessageProcessingModel,
                origSecurityModel,
                origSecurityName,
                origSecurityLevel,
                origContextEngineId,
                origContextName,
                origPdu,
                origTimeout,
                origRetryCount,
                origRetries,
                origDiscoveryRetries,
            )
            return

        # 3.3.6c
        # User-side API assumes SMIv2
        if messageProcessingModel == 0:
            PDU = rfc2576.v1_to_v2(PDU, origPdu)

        cbFun(snmpEngine, sendRequestHandle, None, PDU, cbCtx)

    def send_pdu(
        self,
        snmpEngine: SnmpEngine,
        targetName,
        contextEngineId,
        contextName,
        pdu,
        cbFun=None,
        cbCtx=None,
    ):
        """Send SNMP PDU to a target."""
        (
            transportDomain,
            transportAddress,
            timeout,
            retryCount,
            params,
        ) = config.get_target_address(snmpEngine, targetName)

        (
            messageProcessingModel,
            securityModel,
            securityName,
            securityLevel,
        ) = config.get_target_parameters(snmpEngine, params)

        # User-side API assumes SMIv2
        if messageProcessingModel == 0:
            reqPDU = rfc2576.v2_to_v1(pdu)
            pduVersion = 0
        else:
            reqPDU = pdu
            pduVersion = 1

        # 3.3.5
        if reqPDU.tagSet in rfc3411.CONFIRMED_CLASS_PDUS:
            # Convert timeout in seconds into timeout in timer ticks
            timeoutInTicks = (
                float(timeout)
                / 100
                / snmpEngine.transport_dispatcher.get_timer_resolution()
            )

            sendRequestHandle = getNextHandle()

            # 3.3.6a
            sendPduHandle = snmpEngine.message_dispatcher.send_pdu(
                snmpEngine,
                transportDomain,
                transportAddress,
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                reqPDU,
                True,
                timeoutInTicks,
                self.process_response_pdu,
                (sendRequestHandle, cbFun, cbCtx),
            )

            debug.logger & debug.FLAG_APP and debug.logger(
                "sendPdu: sendPduHandle %s, timeout %d" % (sendPduHandle, timeout)
            )

            # 3.3.6b
            self.__pendingReqs[sendPduHandle] = (
                transportDomain,
                transportAddress,
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pdu,
                timeout,
                retryCount,
                0,
                0,
            )
            snmpEngine.transport_dispatcher.job_started(id(self))
        else:
            snmpEngine.message_dispatcher.send_pdu(
                snmpEngine,
                transportDomain,
                transportAddress,
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
                contextEngineId,
                contextName,
                pduVersion,
                reqPDU,
                False,
            )

            sendRequestHandle = None

            debug.logger & debug.FLAG_APP and debug.logger("sendPdu: message sent")

        return sendRequestHandle

    def process_response_varbinds(
        self, snmpEngine: SnmpEngine, sendRequestHandle, errorIndication, pdu, cbCtx
    ):
        """Process SNMP PDU response."""
        notificationHandle, cbFun, cbCtx = cbCtx

        self.__pendingNotifications[notificationHandle].remove(sendRequestHandle)

        debug.logger & debug.FLAG_APP and debug.logger(
            "processResponseVarBinds: notificationHandle {}, sendRequestHandle {}, errorIndication {}, pending requests {}".format(
                notificationHandle,
                sendRequestHandle,
                errorIndication,
                self.__pendingNotifications[notificationHandle],
            )
        )

        if not self.__pendingNotifications[notificationHandle]:
            debug.logger & debug.FLAG_APP and debug.logger(
                "processResponseVarBinds: notificationHandle {}, sendRequestHandle {} -- completed".format(
                    notificationHandle, sendRequestHandle
                )
            )
            del self.__pendingNotifications[notificationHandle]
            cbFun(
                snmpEngine,
                sendRequestHandle,
                errorIndication,
                pdu and v2c.apiPDU.get_error_status(pdu) or 0,
                pdu and v2c.apiPDU.get_error_index(pdu, muteErrors=True) or 0,
                pdu and v2c.apiPDU.get_varbinds(pdu) or (),
                cbCtx,
            )

    #
    # Higher-level API to Notification Originator. Supports multiple
    # targets, automatic var-binding formation and is fully LCD-driven.
    #
    def send_varbinds(
        self,
        snmpEngine: SnmpEngine,
        notificationTarget,
        contextEngineId,
        contextName,
        varBinds: "tuple[ObjectType, ...]" = (),
        cbFun=None,
        cbCtx=None,
    ):
        """Send SNMP notification to a target.

        Args:
            snmpEngine (SnmpEngine): SNMP engine that is sending the notification.
            notificationTarget (str): Target name as configured at the SNMP engine.
            contextEngineId (str): SNMP engine ID of the context to send notification in.
            contextName (str): SNMP context name to send notification in.
            varBinds (tuple): SNMP variable bindings to send.
            cbFun (callable): Callback function to call when response is received.
            cbCtx (object): General-purpose data passed to the callback function.

        Returns:
            int: Notification handle that can be used to cancel the notification.

        Raises:
            PySnmpError: If SNMP notification PDU requires SNMPv2-MIB::snmpTrapOID.0 to be present.
        """
        debug.logger & debug.FLAG_APP and debug.logger(
            'sendVarBinds: notificationTarget {}, contextEngineId {}, contextName "{}", varBinds {}'.format(
                notificationTarget,
                contextEngineId or "<default>",
                contextName,
                varBinds,
            )
        )

        mibBuilder = snmpEngine.get_mib_builder()

        if contextName:
            (__SnmpAdminString,) = mibBuilder.import_symbols(
                "SNMP-FRAMEWORK-MIB", "SnmpAdminString"
            )
            contextName = __SnmpAdminString(contextName)

        # 3.3
        notifyTag, notifyType = config.get_notification_info(
            snmpEngine, notificationTarget
        )

        notificationHandle = getNextHandle()

        debug.logger & debug.FLAG_APP and debug.logger(
            "sendVarBinds: notificationHandle %s, notifyTag %s, "
            "notifyType %s" % (notificationHandle, notifyTag, notifyType)
        )

        inputVarBinds = [(v2c.ObjectIdentifier(x), y) for (x, y) in varBinds]

        # 3.3.2 & 3.3.3
        snmpTrapOID, sysUpTime = mibBuilder.import_symbols(
            "__SNMPv2-MIB", "snmpTrapOID", "sysUpTime"
        )

        snmpTrapOID = snmpTrapOID.getName()
        sysUpTime, uptime = sysUpTime.getName(), sysUpTime.getSyntax()

        # Add sysUpTime if not present already
        if not inputVarBinds or inputVarBinds[0][0] != sysUpTime:
            inputVarBinds.insert(0, (v2c.ObjectIdentifier(sysUpTime), uptime.clone()))

        # Search for and reposition sysUpTime if it's elsewhere
        for idx, varBind in enumerate(inputVarBinds[1:]):
            if varBind[0] == sysUpTime:
                inputVarBinds[0] = varBind
                del inputVarBinds[idx + 1]
                break

        if len(inputVarBinds) < 2:
            raise error.PySnmpError(
                "SNMP notification PDU requires "
                "SNMPv2-MIB::snmpTrapOID.0 to be present"
            )

        # Search for and reposition snmpTrapOID if it's elsewhere
        for idx, varBind in enumerate(inputVarBinds[2:]):
            if varBind[0] == snmpTrapOID:
                del inputVarBinds[idx + 2]
                if inputVarBinds[1][0] == snmpTrapOID:
                    inputVarBinds[1] = varBind
                else:
                    inputVarBinds.insert(1, varBind)
                break

        if inputVarBinds[1][0] != snmpTrapOID:
            raise error.PySnmpError(
                "SNMP notification PDU requires "
                "SNMPv2-MIB::snmpTrapOID.0 to be present"
            )

        sendRequestHandle = -1

        debug.logger & debug.FLAG_APP and debug.logger(
            f"sendVarBinds: final varBinds {varBinds}"
        )

        for targetAddrName in config.get_target_names(snmpEngine, notifyTag):
            (
                transportDomain,
                transportAddress,
                timeout,
                retryCount,
                params,
            ) = config.get_target_address(snmpEngine, targetAddrName)
            (
                messageProcessingModel,
                securityModel,
                securityName,
                securityLevel,
            ) = config.get_target_parameters(snmpEngine, params)

            # 3.3.1 XXX
            # XXX filtering's yet to be implemented
            #             filterProfileName = config.getNotifyFilterProfile(params)

            #             (filterSubtree, filterMask,
            #              filterType) = config.getNotifyFilter(filterProfileName)

            debug.logger & debug.FLAG_APP and debug.logger(
                "sendVarBinds: notificationHandle {}, notifyTag {} yields: transportDomain {}, transportAddress {!r}, securityModel {}, securityName {}, securityLevel {}".format(
                    notificationHandle,
                    notifyTag,
                    transportDomain,
                    transportAddress,
                    securityModel,
                    securityName,
                    securityLevel,
                )
            )

            for varName, varVal in inputVarBinds:
                if varName in (sysUpTime, snmpTrapOID):
                    continue
                try:
                    snmpEngine.access_control_model[self.ACM_ID].is_access_allowed(
                        snmpEngine,
                        securityModel,
                        securityName,
                        securityLevel,
                        "notify",
                        contextName,
                        varName,
                    )

                    debug.logger & debug.FLAG_APP and debug.logger(
                        f"sendVarBinds: ACL succeeded for OID {varName} securityName {securityName}"
                    )

                except error.StatusInformation:
                    debug.logger & debug.FLAG_APP and debug.logger(
                        "sendVarBinds: ACL denied access for OID {} securityName {}, droppping notification".format(
                            varName, securityName
                        )
                    )
                    return

            # 3.3.4
            if notifyType == 1:
                pdu = v2c.SNMPv2TrapPDU()
            elif notifyType == 2:
                pdu = v2c.InformRequestPDU()
            else:
                raise error.ProtocolError("Unknown notify-type %r", notifyType)

            v2c.apiPDU.set_defaults(pdu)
            v2c.apiPDU.set_varbinds(pdu, inputVarBinds)

            # 3.3.5
            try:
                sendRequestHandle = self.send_pdu(
                    snmpEngine,
                    targetAddrName,
                    contextEngineId,
                    contextName,
                    pdu,
                    self.process_response_varbinds,
                    (notificationHandle, cbFun, cbCtx),
                )

            except error.StatusInformation:
                statusInformation = sys.exc_info()[1]
                debug.logger & debug.FLAG_APP and debug.logger(
                    "sendVarBinds: sendRequestHandle {}: sendPdu() failed with {!r}".format(
                        sendRequestHandle, statusInformation
                    )
                )
                if (
                    notificationHandle not in self.__pendingNotifications
                    or not self.__pendingNotifications[notificationHandle]
                ):
                    if notificationHandle in self.__pendingNotifications:
                        del self.__pendingNotifications[notificationHandle]
                    if cbFun:
                        cbFun(
                            snmpEngine,
                            notificationHandle,
                            statusInformation["errorIndication"],
                            0,
                            0,
                            (),
                            cbCtx,
                        )
                return notificationHandle

            debug.logger & debug.FLAG_APP and debug.logger(
                "sendVarBinds: notificationHandle %s, sendRequestHandle %s, timeout %d"
                % (notificationHandle, sendRequestHandle, timeout)
            )

            if notifyType == 2:
                if notificationHandle not in self.__pendingNotifications:
                    self.__pendingNotifications[notificationHandle] = set()
                self.__pendingNotifications[notificationHandle].add(sendRequestHandle)

        debug.logger & debug.FLAG_APP and debug.logger(
            "sendVarBinds: notificationHandle {}, sendRequestHandle {}, notification(s) sent".format(
                notificationHandle, sendRequestHandle
            )
        )

        return notificationHandle


# XXX
# move/group/implement config setting/retrieval at a stand-alone module
