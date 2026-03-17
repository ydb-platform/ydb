#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import TYPE_CHECKING

from pysnmp.proto import error
from pysnmp.proto.mpmod import cache

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine


class AbstractMessageProcessingModel:
    """Create a message processing model object."""

    SNMP_MSG_SPEC = NotImplementedError

    def __init__(self):
        """Create a message processing model object."""
        self._snmpMsgSpec = self.SNMP_MSG_SPEC()  # local copy
        self._cache = cache.Cache()

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
        raise error.ProtocolError("method not implemented")

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
        raise error.ProtocolError("method not implemented")

    def prepare_data_elements(
        self, snmpEngine: "SnmpEngine", transportDomain, transportAddress, wholeMsg
    ):
        """Prepare SNMP message data elements."""
        raise error.ProtocolError("method not implemented")

    def release_state_information(self, sendPduHandle):
        """Release state information."""
        try:
            self._cache.pop_by_send_pdu_handle(sendPduHandle)
        except error.ProtocolError:
            pass  # XXX maybe these should all follow some scheme?

    def receive_timer_tick(self, snmpEngine: "SnmpEngine", timeNow):
        """Process a timer tick."""
        self._cache.expire_caches()
