#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.type import constraint, univ
from pysnmp.proto import errind, rfc1901, rfc1902, rfc1905
from pysnmp.proto.api import v1

# Shortcuts to SNMP types
Null = univ.Null
null = Null("")
ObjectIdentifier = univ.ObjectIdentifier

Integer = rfc1902.Integer
Integer32 = rfc1902.Integer32
OctetString = rfc1902.OctetString
IpAddress = rfc1902.IpAddress
Counter32 = rfc1902.Counter32
Gauge32 = rfc1902.Gauge32
Unsigned32 = rfc1902.Unsigned32
TimeTicks = rfc1902.TimeTicks
Opaque = rfc1902.Opaque
Counter64 = rfc1902.Counter64
Bits = rfc1902.Bits

NoSuchObject = rfc1905.NoSuchObject
NoSuchInstance = rfc1905.NoSuchInstance
EndOfMibView = rfc1905.EndOfMibView

VarBind = rfc1905.VarBind
VarBindList = rfc1905.VarBindList
GetRequestPDU = rfc1905.GetRequestPDU
GetNextRequestPDU = rfc1905.GetNextRequestPDU
ResponsePDU = GetResponsePDU = rfc1905.ResponsePDU
SetRequestPDU = rfc1905.SetRequestPDU
GetBulkRequestPDU = rfc1905.GetBulkRequestPDU
InformRequestPDU = rfc1905.InformRequestPDU
SNMPv2TrapPDU = TrapPDU = rfc1905.SNMPv2TrapPDU
ReportPDU = rfc1905.ReportPDU

Message = rfc1901.Message

getNextRequestID = v1.getNextRequestID  # noqa: N816

apiVarBind = v1.apiVarBind  # noqa: N816


class PDUAPI(v1.PDUAPI):
    """SNMPv2c request PDU API."""

    _error_status = rfc1905.errorStatus.clone(0)
    _error_index = univ.Integer(0).subtype(
        subtypeSpec=constraint.ValueRangeConstraint(0, rfc1905.max_bindings)
    )

    def get_response(self, reqPDU) -> rfc1905.ResponsePDU:
        """Build response PDU."""
        rspPDU = ResponsePDU()
        self.set_defaults(rspPDU)
        self.set_request_id(rspPDU, self.get_request_id(reqPDU))
        return rspPDU

    def get_varbind_table(self, reqPDU, rspPDU):
        """Get var-binds table from response PDU."""
        return [apiPDU.get_varbinds(rspPDU)]

    def get_next_varbinds(self, varBinds, origVarBinds=None):
        """Get next var-binds."""
        errorIndication = None
        idx = nonNulls = len(varBinds)
        rspVarBinds = []
        while idx:
            idx -= 1
            if varBinds[idx][1].tagSet in (
                rfc1905.NoSuchObject.tagSet,
                rfc1905.NoSuchInstance.tagSet,
                rfc1905.EndOfMibView.tagSet,
            ):
                nonNulls -= 1
            elif origVarBinds is not None:
                seed = ObjectIdentifier(origVarBinds[idx][0]).asTuple()
                found = varBinds[idx][0].asTuple()
                if seed >= found:
                    errorIndication = errind.oidNotIncreasing

            rspVarBinds.insert(0, (varBinds[idx][0], null))

        if not nonNulls:
            rspVarBinds = []

        return errorIndication, rspVarBinds

    def set_end_of_mib_error(self, pdu, errorIndex):
        """Set endOfMibView error."""
        varBindList = self.get_varbind_list(pdu)
        varBindList[errorIndex - 1].setComponentByPosition(
            1,
            rfc1905.endOfMibView,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )

    def set_no_such_instance_error(self, pdu, errorIndex):
        """Set noSuchInstance error."""
        varBindList = self.get_varbind_list(pdu)
        varBindList[errorIndex - 1].setComponentByPosition(
            1,
            rfc1905.noSuchInstance,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )


apiPDU = PDUAPI()  # noqa: N816


class BulkPDUAPI(PDUAPI):
    """SNMPv2c bulk request PDU API."""

    _non_repeaters = rfc1905.nonRepeaters.clone(0)
    _max_repetitions = rfc1905.maxRepetitions.clone(10)

    def set_defaults(self, pdu):
        """Set SNMPv2c bulk request defaults."""
        PDUAPI.set_defaults(self, pdu)
        pdu.setComponentByPosition(
            0,
            getNextRequestID(),
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            1,
            self._non_repeaters,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            2,
            self._max_repetitions,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        varBindList = pdu.setComponentByPosition(3).getComponentByPosition(3)
        varBindList.clear()

    @staticmethod
    def get_non_repeaters(pdu):
        """Get non-repeaters component of the PDU."""
        return pdu.getComponentByPosition(1)

    @staticmethod
    def set_non_repeaters(pdu, value):
        """Set non-repeaters component of the PDU."""
        pdu.setComponentByPosition(1, value)

    @staticmethod
    def get_max_repetitions(pdu):
        """Get max repetitions component of the PDU."""
        return pdu.getComponentByPosition(2)

    @staticmethod
    def set_max_repetitions(pdu, value):
        """Set max repetitions component of the PDU."""
        pdu.setComponentByPosition(2, value)

    def get_varbind_table(self, reqPDU, rspPDU):
        """Get var-binds table from response PDU."""
        nonRepeaters = self.get_non_repeaters(reqPDU)

        reqVarBinds = self.get_varbinds(reqPDU)

        N = min(int(nonRepeaters), len(reqVarBinds))

        rspVarBinds = self.get_varbinds(rspPDU)

        # shortcut for the most trivial case
        if N == 0 and len(reqVarBinds) == 1:
            return [[vb] for vb in rspVarBinds]

        R = max(len(reqVarBinds) - N, 0)

        varBindTable = []

        if R:
            for i in range(0, len(rspVarBinds) - N, R):
                varBindRow = rspVarBinds[:N] + rspVarBinds[N + i : N + R + i]
                # ignore stray OIDs / non-rectangular table
                if len(varBindRow) == N + R:
                    varBindTable.append(varBindRow)
        elif N:
            varBindTable.append(rspVarBinds[:N])

        return varBindTable


apiBulkPDU = BulkPDUAPI()  # noqa: N816


class TrapPDUAPI(v1.PDUAPI):
    """SNMPv2c trap PDU API."""

    sysUpTime = (1, 3, 6, 1, 2, 1, 1, 3, 0)  # noqa: N815
    snmpTrapAddress = (1, 3, 6, 1, 6, 3, 18, 1, 3, 0)  # noqa: N815
    snmpTrapCommunity = (1, 3, 6, 1, 6, 3, 18, 1, 4, 0)  # noqa: N815
    snmpTrapOID = (1, 3, 6, 1, 6, 3, 1, 1, 4, 1, 0)  # noqa: N815
    snmpTrapEnterprise = (1, 3, 6, 1, 6, 3, 1, 1, 4, 3, 0)  # noqa: N815
    _zeroTime = TimeTicks(0)  # noqa: N815
    _genTrap = ObjectIdentifier((1, 3, 6, 1, 6, 3, 1, 1, 5, 1))  # noqa: N815

    def set_defaults(self, pdu):
        """Set SNMPv2c trap defaults."""
        v1.PDUAPI.set_defaults(self, pdu)
        varBinds = [
            (self.sysUpTime, self._zeroTime),
            # generic trap
            (self.snmpTrapOID, self._genTrap),
        ]
        self.set_varbinds(pdu, varBinds)


apiTrapPDU = TrapPDUAPI()  # noqa: N816


class MessageAPI(v1.MessageAPI):
    """SNMPv2c message API."""

    _version = rfc1901.version.clone(1)

    def set_defaults(self, msg):
        """Set SNMP message defaults."""
        msg.setComponentByPosition(
            0,
            self._version,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        msg.setComponentByPosition(
            1,
            self._community,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        return msg

    def get_response(self, reqMsg) -> rfc1901.Message:
        """Build response message."""
        rspMsg = Message()
        self.set_defaults(rspMsg)
        self.set_version(rspMsg, self.get_version(reqMsg))
        self.set_community(rspMsg, self.get_community(reqMsg))
        self.set_pdu(rspMsg, apiPDU.get_response(self.get_pdu(reqMsg)))
        return rspMsg


apiMessage = MessageAPI()  # noqa: N816
