#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import warnings
from time import time
from typing import Any

from pyasn1.codec.ber import decoder, encoder
from pysnmp import debug
from pysnmp.carrier.base import AbstractTransportDispatcher
from pysnmp.entity.engine import SnmpEngine
from pysnmp.hlapi.transport import AbstractTransportTarget
from pysnmp.proto import api
from pysnmp.proto import errind, error
from pysnmp.proto.api import verdec

__all__ = []


class AbstractSnmpDispatcher:
    """Creates SNMP message dispatcher object.

    `SnmpDispatcher` object manages send and receives SNMP PDU
    messages through underlying transport dispatcher and dispatches
    them to the callers.

    `SnmpDispatcher` is the only stateful object, all `hlapi.v1arch` SNMP
    operations require an instance of `SnmpDispatcher`. Users do not normally
    request services directly from `SnmpDispather`, but pass it around to
    other `hlapi.v1arch` interfaces.

    It is possible to run multiple instances of `SnmpDispatcher` in the
    application. In a multithreaded environment, each thread that
    works with SNMP must have its own `SnmpDispatcher` instance.
    """

    PROTO_DISPATCHER = None
    transport_dispatcher: AbstractTransportDispatcher
    cache: dict[str, Any]

    def __init__(self, transportDispatcher: AbstractTransportDispatcher = None):  # type: ignore
        if transportDispatcher:
            self.transport_dispatcher = transportDispatcher

        else:
            self.transport_dispatcher = self.PROTO_DISPATCHER()

        self._automaticDispatcher = transportDispatcher is not self.transport_dispatcher
        self._configuredTransports = set()

        self._pendingReqs = {}

        self.transport_dispatcher.register_recv_callback(self._recv_callback)
        self.transport_dispatcher.register_timer_callback(self._timer_callback)

        self.cache = {}

    def __repr__(self):
        return f"{self.__class__.__name__}(transportDispatcher={self.transport_dispatcher})"

    def close(self):
        self.transport_dispatcher.unregister_recv_callback()
        self.transport_dispatcher.unregister_timer_callback()
        if self._automaticDispatcher:
            self.transport_dispatcher.close_dispatcher()

        for requestId, stateInfo in self._pendingReqs.items():
            cbFun = stateInfo["cbFun"]
            cbCtx = stateInfo["cbCtx"]

            if cbFun:
                cbFun(self, "Request #%d terminated" % requestId, None, cbCtx)

        self._pendingReqs.clear()

    def send_pdu(
        self,
        authData,
        transportTarget: AbstractTransportTarget,
        reqPdu,
        cbFun=None,
        cbCtx=None,
    ):
        if (
            self._automaticDispatcher
            and transportTarget.TRANSPORT_DOMAIN not in self._configuredTransports
        ):
            self.transport_dispatcher.register_transport(
                transportTarget.TRANSPORT_DOMAIN,
                transportTarget.PROTO_TRANSPORT().open_client_mode(
                    transportTarget.iface
                ),
            )
            self._configuredTransports.add(transportTarget.TRANSPORT_DOMAIN)

        pMod = api.PROTOCOL_MODULES[authData.mpModel]

        reqMsg = pMod.Message()
        pMod.apiMessage.set_defaults(reqMsg)
        pMod.apiMessage.set_community(reqMsg, authData.communityName)
        pMod.apiMessage.set_pdu(reqMsg, reqPdu)

        outgoingMsg = encoder.encode(reqMsg)

        requestId = pMod.apiPDU.get_request_id(reqPdu)

        self._pendingReqs[requestId] = dict(
            outgoingMsg=outgoingMsg,
            transportTarget=transportTarget,
            cbFun=cbFun,
            cbCtx=cbCtx,
            timestamp=time() + transportTarget.timeout,
            retries=0,
        )

        self.transport_dispatcher.send_message(
            outgoingMsg,
            transportTarget.TRANSPORT_DOMAIN,
            transportTarget.transport_address,
        )

        if reqPdu.__class__ is getattr(
            pMod, "SNMPv2TrapPDU", None
        ) or reqPdu.__class__ is getattr(pMod, "TrapPDU", None):
            return requestId

        self.transport_dispatcher.job_started(id(self))

        return requestId

    def _recv_callback(
        self, snmpEngine: SnmpEngine, transportDomain, transportAddress, wholeMsg
    ):
        try:
            mpModel = verdec.decode_message_version(wholeMsg)

        except error.ProtocolError:
            return  # n.b the whole buffer gets dropped

        debug.logger & debug.FLAG_DSP and debug.logger(
            "receiveMessage: msgVersion %s, msg decoded" % mpModel
        )

        pMod = api.PROTOCOL_MODULES[mpModel]

        while wholeMsg:
            rspMsg, wholeMsg = decoder.decode(wholeMsg, asn1Spec=pMod.Message())
            rspPdu = pMod.apiMessage.get_pdu(rspMsg)

            requestId = pMod.apiPDU.get_request_id(rspPdu)

            try:
                stateInfo = self._pendingReqs.pop(requestId)

            except KeyError:
                continue

            self.transport_dispatcher.job_finished(id(self))

            cbFun = stateInfo["cbFun"]
            cbCtx = stateInfo["cbCtx"]

            if cbFun:
                cbFun(self, requestId, None, rspPdu, cbCtx)

        return wholeMsg

    def _timer_callback(self, timeNow):
        for requestId, stateInfo in tuple(self._pendingReqs.items()):
            if stateInfo["timestamp"] > timeNow:
                continue

            retries = stateInfo["retries"]
            transportTarget: AbstractTransportTarget = stateInfo["transportTarget"]

            if retries == transportTarget.retries:
                cbFun = stateInfo["cbFun"]
                cbCtx = stateInfo["cbCtx"]

                if cbFun:
                    del self._pendingReqs[requestId]
                    cbFun(
                        self,
                        requestId,
                        errind.requestTimedOut,
                        None,
                        cbCtx,
                    )
                    self.transport_dispatcher.job_finished(id(self))
                    continue

            stateInfo["retries"] += 1
            stateInfo["timestamp"] = timeNow + transportTarget.timeout

            outgoingMsg = stateInfo["outgoingMsg"]

            self.transport_dispatcher.send_message(
                outgoingMsg,
                transportTarget.TRANSPORT_DOMAIN,
                transportTarget.transport_address,
            )

    # compatibility with legacy code
    # Old to new attribute mapping
    deprecated_attributes = {
        "transportDispatcher": "transport_dispatcher",
        "sendPdu": "send_pdu",
    }

    def __getattr__(self, attr: str):
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

    def __enter__(self):
        """Open the SNMP dispatcher."""
        return self

    def __exit__(self, *args, **kwargs):
        """Close the SNMP dispatcher."""
        self.close()
