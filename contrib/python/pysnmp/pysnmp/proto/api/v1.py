#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.type import univ
from pysnmp import nextid
from pysnmp.proto import error, rfc1155, rfc1157

# Shortcuts to SNMP types
Integer = univ.Integer
OctetString = univ.OctetString
Null = univ.Null
null = Null("")
ObjectIdentifier = univ.ObjectIdentifier

IpAddress = rfc1155.IpAddress
NetworkAddress = rfc1155.NetworkAddress
Counter = rfc1155.Counter
Gauge = rfc1155.Gauge
TimeTicks = rfc1155.TimeTicks
Opaque = rfc1155.Opaque

VarBind = rfc1157.VarBind
VarBindList = rfc1157.VarBindList
GetRequestPDU = rfc1157.GetRequestPDU
GetNextRequestPDU = rfc1157.GetNextRequestPDU
GetResponsePDU = rfc1157.GetResponsePDU
SetRequestPDU = rfc1157.SetRequestPDU
TrapPDU = rfc1157.TrapPDU
Message = rfc1157.Message


class VarBindAPI:
    """Var-bind API."""

    @staticmethod
    def set_oid_value(varBind, oidVal):
        """Set OID and value components of var-bind."""
        oid, val = oidVal[0], oidVal[1]
        varBind.setComponentByPosition(0, oid)
        if val is None:
            val = null
        varBind.setComponentByPosition(1).getComponentByPosition(1).setComponentByType(
            val.getTagSet(),
            val,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
            innerFlag=True,
        )
        return varBind

    @staticmethod
    def get_oid_value(varBind):
        """Return OID and value components of var-bind."""
        return varBind[0], varBind[1].getComponent(1)


apiVarBind = VarBindAPI()  # noqa: N816

getNextRequestID = nextid.Integer(0xFFFFFF)  # noqa: N816


class PDUAPI:
    """SNMP PDU API."""

    _error_status = rfc1157.errorStatus.clone(0)
    _error_index = Integer(0)

    def set_defaults(self, pdu):
        """Set default values for SNMP PDU."""
        pdu.setComponentByPosition(
            0,
            getNextRequestID(),
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            1,
            self._error_status,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            2,
            self._error_index,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        varBindList = pdu.setComponentByPosition(3).getComponentByPosition(3)
        varBindList.clear()

    @staticmethod
    def get_request_id(pdu):
        """Return request ID component of SNMP PDU."""
        return pdu.getComponentByPosition(0)

    @staticmethod
    def set_request_id(pdu, value):
        """Set request ID component of SNMP PDU."""
        pdu.setComponentByPosition(0, value)

    @staticmethod
    def get_error_status(pdu):
        """Return error status component of SNMP PDU."""
        return pdu.getComponentByPosition(1)

    @staticmethod
    def set_error_status(pdu, value):
        """Set error status component of SNMP PDU."""
        pdu.setComponentByPosition(1, value)

    @staticmethod
    def get_error_index(pdu, muteErrors=False):
        """Return error index component of SNMP PDU."""
        errorIndex = pdu.getComponentByPosition(2)
        if errorIndex > len(pdu[3]):
            if muteErrors:
                return errorIndex.clone(len(pdu[3]))
            raise error.ProtocolError(
                f"Error index out of range: {errorIndex} > {len(pdu[3])}"
            )
        return errorIndex

    @staticmethod
    def set_error_index(pdu, value):
        """Set error index component of SNMP PDU."""
        pdu.setComponentByPosition(2, value)

    def set_end_of_mib_error(self, pdu, errorIndex):
        """Set end-of-MIB error status."""
        self.set_error_index(pdu, errorIndex)
        self.set_error_status(pdu, 2)

    def set_no_such_instance_error(self, pdu, errorIndex):
        """Set no-such-instance error status."""
        self.set_end_of_mib_error(pdu, errorIndex)

    @staticmethod
    def get_varbind_list(pdu):
        """Return var-bind list component of SNMP PDU."""
        return pdu.getComponentByPosition(3)

    @staticmethod
    def set_varbind_list(pdu, varBindList):
        """Set var-bind list component of SNMP PDU."""
        pdu.setComponentByPosition(3, varBindList)

    @staticmethod
    def get_varbinds(pdu):
        """Return var-binds component of SNMP PDU."""
        return [
            apiVarBind.get_oid_value(varBind)
            for varBind in pdu.getComponentByPosition(3)
        ]

    @staticmethod
    def set_varbinds(pdu, varBinds):
        """Set var-binds component of SNMP PDU."""
        varBindList = pdu.setComponentByPosition(3).getComponentByPosition(3)
        varBindList.clear()
        for idx, varBind in enumerate(varBinds):
            if isinstance(varBind, VarBind):
                varBindList.setComponentByPosition(idx, varBind)
            else:
                varBindList.setComponentByPosition(idx)
                apiVarBind.set_oid_value(
                    varBindList.getComponentByPosition(idx), varBind
                )

    def get_response(self, reqPDU):
        """Build response PDU."""
        rspPDU = GetResponsePDU()
        self.set_defaults(rspPDU)
        self.set_request_id(rspPDU, self.get_request_id(reqPDU))
        return rspPDU

    def get_varbind_table(self, reqPDU, rspPDU):
        """Return var-bind table."""
        if apiPDU.get_error_status(rspPDU) == 2:
            varBindRow = [(vb[0], null) for vb in apiPDU.get_varbinds(reqPDU)]
        else:
            varBindRow = apiPDU.get_varbinds(rspPDU)
        return [varBindRow]

    def get_next_varbinds(self, varBinds, errorIndex=None):
        """Return next var-binds."""
        errorIndication = None

        if errorIndex:
            return errorIndication, []

        rspVarBinds = [(vb[0], null) for vb in varBinds]

        return errorIndication, rspVarBinds


apiPDU = PDUAPI()  # noqa: N816


class TrapPDUAPI:
    """SNMP trap PDU API."""

    _networkAddress = None  # noqa: N815
    _entOid = ObjectIdentifier((1, 3, 6, 1, 4, 1, 20408))  # noqa: N815
    _genericTrap = rfc1157.genericTrap.clone("coldStart")  # noqa: N815
    _zeroInt = univ.Integer(0)  # noqa: N815
    _zeroTime = TimeTicks(0)  # noqa: N815

    def set_defaults(self, pdu):
        """Set default values for SNMP trap PDU."""
        if self._networkAddress is None:
            try:
                import socket

                agentAddress = IpAddress(socket.gethostbyname(socket.gethostname()))
            except Exception:
                agentAddress = IpAddress("0.0.0.0")
            self._networkAddress = NetworkAddress().setComponentByPosition(
                0, agentAddress
            )
        pdu.setComponentByPosition(
            0,
            self._entOid,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            1,
            self._networkAddress,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            2,
            self._genericTrap,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            3,
            self._zeroInt,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        pdu.setComponentByPosition(
            4,
            self._zeroTime,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
        )
        varBindList = pdu.setComponentByPosition(5).getComponentByPosition(5)
        varBindList.clear()

    @staticmethod
    def get_enterprise(pdu):
        """Return enterprise component of SNMP trap PDU."""
        return pdu.getComponentByPosition(0)

    @staticmethod
    def set_enterprise(pdu, value):
        """Set enterprise component of SNMP trap PDU."""
        pdu.setComponentByPosition(0, value)

    @staticmethod
    def get_agent_address(pdu):
        """Return agent address component of SNMP trap PDU."""
        return pdu.getComponentByPosition(1).getComponentByPosition(0)

    @staticmethod
    def set_agent_address(pdu, value):
        """Set agent address component of SNMP trap PDU."""
        pdu.setComponentByPosition(1).getComponentByPosition(1).setComponentByPosition(
            0, value
        )

    @staticmethod
    def get_generic_trap(pdu):
        """Return generic trap component of SNMP trap PDU."""
        return pdu.getComponentByPosition(2)

    @staticmethod
    def set_generic_trap(pdu, value):
        """Set generic trap component of SNMP trap PDU."""
        pdu.setComponentByPosition(2, value)

    @staticmethod
    def get_specific_trap(pdu):
        """Return specific trap component of SNMP trap PDU."""
        return pdu.getComponentByPosition(3)

    @staticmethod
    def set_specific_trap(pdu, value):
        """Set specific trap component of SNMP trap PDU."""
        pdu.setComponentByPosition(3, value)

    @staticmethod
    def get_timestamp(pdu):
        """Return time stamp component of SNMP trap PDU."""
        return pdu.getComponentByPosition(4)

    @staticmethod
    def set_timestamp(pdu, value):
        """Set time stamp component of SNMP trap PDU."""
        pdu.setComponentByPosition(4, value)

    @staticmethod
    def get_varbind_list(pdu):
        """Return var-bind list component of SNMP trap PDU."""
        return pdu.getComponentByPosition(5)

    @staticmethod
    def set_varbind_list(pdu, varBindList):
        """Set var-bind list component of SNMP trap PDU."""
        pdu.setComponentByPosition(5, varBindList)

    @staticmethod
    def get_varbinds(pdu):
        """Return var-binds component of SNMP trap PDU."""
        varBinds = []
        for varBind in pdu.getComponentByPosition(5):
            varBinds.append(apiVarBind.get_oid_value(varBind))
        return varBinds

    @staticmethod
    def set_varbinds(pdu, varBinds):
        """Set var-binds component of SNMP trap PDU."""
        varBindList = pdu.setComponentByPosition(5).getComponentByPosition(5)
        varBindList.clear()
        idx = 0
        for varBind in varBinds:
            if isinstance(varBind, VarBind):
                varBindList.setComponentByPosition(idx, varBind)
            else:
                varBindList.setComponentByPosition(idx)
                apiVarBind.set_oid_value(
                    varBindList.getComponentByPosition(idx), varBind
                )
            idx += 1


apiTrapPDU = TrapPDUAPI()  # noqa: N816


class MessageAPI:
    """SNMP message API."""

    _version = rfc1157.version.clone(0)
    _community = univ.OctetString("public")

    def set_defaults(self, msg):
        """Set default values for SNMP message."""
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

    @staticmethod
    def get_version(msg):
        """Return version component of SNMP message."""
        return msg.getComponentByPosition(0)

    @staticmethod
    def set_version(msg, value):
        """Set version component of SNMP message."""
        msg.setComponentByPosition(0, value)

    @staticmethod
    def get_community(msg):
        """Return community component of SNMP message."""
        return msg.getComponentByPosition(1)

    @staticmethod
    def set_community(msg, value):
        """Set community component of SNMP message."""
        msg.setComponentByPosition(1, value)

    @staticmethod
    def get_pdu(
        msg,
    ) -> "TrapPDU | GetRequestPDU | GetNextRequestPDU | GetResponsePDU | SetRequestPDU":
        """Return PDU component of SNMP message."""
        return msg.getComponentByPosition(2).getComponent()

    @staticmethod
    def set_pdu(msg, value):
        """Set PDU component of SNMP message."""
        msg.setComponentByPosition(2).getComponentByPosition(2).setComponentByType(
            value.getTagSet(),
            value,
            verifyConstraints=False,
            matchTags=False,
            matchConstraints=False,
            innerFlag=True,
        )

    def get_response(self, reqMsg):
        """Return a response message to a request message."""
        rspMsg = Message()
        self.set_defaults(rspMsg)
        self.set_version(rspMsg, self.get_version(reqMsg))
        self.set_community(rspMsg, self.get_community(reqMsg))
        self.set_pdu(rspMsg, apiPDU.get_response(self.get_pdu(reqMsg)))
        return rspMsg


apiMessage = MessageAPI()  # noqa: N816
