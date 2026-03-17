#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#

from typing import TYPE_CHECKING


from pysnmp.proto import error
from pysnmp.proto.errind import ErrorIndication
from pysnmp.proto.secmod import cache

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine


class AbstractSecurityModel:
    """Abstract security model class."""

    SECURITY_MODEL_ID = None
    _cache: cache.Cache

    def __init__(self):
        """Create a security model object."""
        self._cache = cache.Cache()

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
        raise error.ProtocolError("Security model %s not implemented" % self)

    def generate_request_message(
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
    ):
        """Generate a request message."""
        raise error.ProtocolError("Security model %s not implemented" % self)

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
        ctx: ErrorIndication,
    ):
        """Generate a response message."""
        raise error.ProtocolError("Security model %s not implemented" % self)

    def release_state_information(self, stateReference):
        """Release state information."""
        self._cache.pop(stateReference)

    def receive_timer_tick(self, snmpEngine: "SnmpEngine", timeNow):
        """Process a timer tick."""
        pass

    def _close(self):
        """
        Close the security model to test memory leak.

        This method is intended for unit testing purposes only.
        It closes the security model and checks if all associated resources are released.
        """
        raise error.ProtocolError("Security model %s not implemented" % self)
