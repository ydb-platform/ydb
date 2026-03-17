#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp import debug
from pysnmp.proto import error, rfc1905, rfc3411
from pysnmp.proto.api import v1, v2c

# 2.1.1

V1_TO_V2_VALUE_MAP = {
    v1.Integer.tagSet: v2c.Integer32(),
    v1.OctetString.tagSet: v2c.OctetString(),
    v1.Null.tagSet: v2c.Null(),
    v1.ObjectIdentifier.tagSet: v2c.ObjectIdentifier(),
    v1.IpAddress.tagSet: v2c.IpAddress(),
    v1.Counter.tagSet: v2c.Counter32(),
    v1.Gauge.tagSet: v2c.Gauge32(),
    v1.TimeTicks.tagSet: v2c.TimeTicks(),
    v1.Opaque.tagSet: v2c.Opaque(),
}

V2_TO_V1_VALUE_MAP = {  # XXX do not re-create same-type items?
    v2c.Integer32.tagSet: v1.Integer(),
    v2c.OctetString.tagSet: v1.OctetString(),
    v2c.Null.tagSet: v1.Null(),
    v2c.ObjectIdentifier.tagSet: v1.ObjectIdentifier(),
    v2c.IpAddress.tagSet: v1.IpAddress(),
    v2c.Counter32.tagSet: v1.Counter(),
    v2c.Gauge32.tagSet: v1.Gauge(),
    v2c.TimeTicks.tagSet: v1.TimeTicks(),
    v2c.Opaque.tagSet: v1.Opaque(),
}

# PDU map

V1_TO_V2_PDU_MAP = {
    v1.GetRequestPDU.tagSet: v2c.GetRequestPDU(),
    v1.GetNextRequestPDU.tagSet: v2c.GetNextRequestPDU(),
    v1.SetRequestPDU.tagSet: v2c.SetRequestPDU(),
    v1.GetResponsePDU.tagSet: v2c.ResponsePDU(),
    v1.TrapPDU.tagSet: v2c.SNMPv2TrapPDU(),
}

V2_TO_V1_PDU_MAP = {
    v2c.GetRequestPDU.tagSet: v1.GetRequestPDU(),
    v2c.GetNextRequestPDU.tagSet: v1.GetNextRequestPDU(),
    v2c.SetRequestPDU.tagSet: v1.SetRequestPDU(),
    v2c.ResponsePDU.tagSet: v1.GetResponsePDU(),
    v2c.SNMPv2TrapPDU.tagSet: v1.TrapPDU(),
    v2c.GetBulkRequestPDU.tagSet: v1.GetNextRequestPDU(),  # 4.1.1
}

# Trap map

V1_TO_V2_TRAP_MAP = {
    0: (1, 3, 6, 1, 6, 3, 1, 1, 5, 1),
    1: (1, 3, 6, 1, 6, 3, 1, 1, 5, 2),
    2: (1, 3, 6, 1, 6, 3, 1, 1, 5, 3),
    3: (1, 3, 6, 1, 6, 3, 1, 1, 5, 4),
    4: (1, 3, 6, 1, 6, 3, 1, 1, 5, 5),
    5: (1, 3, 6, 1, 6, 3, 1, 1, 5, 6),
}

V2_TO_V1_TRAP_MAP = {
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 1): 0,
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 2): 1,
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 3): 2,
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 4): 3,
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 5): 4,
    (1, 3, 6, 1, 6, 3, 1, 1, 5, 6): 5,
}

# 4.3

V2_TO_V1_ERROR_MAP = {
    0: 0,
    1: 1,
    5: 5,
    10: 3,
    9: 3,
    7: 3,
    8: 3,
    12: 3,
    6: 2,
    17: 2,
    11: 2,
    18: 2,
    13: 5,
    14: 5,
    15: 5,
    16: 2,
}

zeroInt = v1.Integer(0)  # noqa: N816


def v1_to_v2(v1Pdu, origV2Pdu=None, snmpTrapCommunity=""):
    """Return SNMPv2 PDU given SNMPv1 PDU."""
    pduType = v1Pdu.tagSet
    v2Pdu = V1_TO_V2_PDU_MAP[pduType].clone()

    debug.logger & debug.FLAG_PRX and debug.logger(
        "v1ToV2: v1Pdu %s" % v1Pdu.prettyPrint()
    )

    v2VarBinds = []

    # 3.1
    if pduType in rfc3411.NOTIFICATION_CLASS_PDUS:
        # 3.1.1
        sysUpTime = v1.apiTrapPDU.get_timestamp(v1Pdu)

        # 3.1.2
        genericTrap = v1.apiTrapPDU.get_generic_trap(v1Pdu)
        if genericTrap == 6:
            snmpTrapOIDParam = v1.apiTrapPDU.get_enterprise(v1Pdu) + (
                0,
                int(v1.apiTrapPDU.get_specific_trap(v1Pdu)),
            )

        # 3.1.3
        else:
            snmpTrapOIDParam = v2c.ObjectIdentifier(V1_TO_V2_TRAP_MAP[genericTrap])

        # 3.1.4
        v2VarBinds.append((v2c.apiTrapPDU.sysUpTime, sysUpTime))
        v2VarBinds.append((v2c.apiTrapPDU.snmpTrapOID, snmpTrapOIDParam))
        v2VarBinds.append(
            (v2c.apiTrapPDU.snmpTrapAddress, v1.apiTrapPDU.get_agent_address(v1Pdu))
        )
        v2VarBinds.append(
            (v2c.apiTrapPDU.snmpTrapCommunity, v2c.OctetString(snmpTrapCommunity))
        )
        v2VarBinds.append(
            (v2c.apiTrapPDU.snmpTrapEnterprise, v1.apiTrapPDU.get_enterprise(v1Pdu))
        )

        varBinds = v1.apiTrapPDU.get_varbinds(v1Pdu)

    else:
        varBinds = v1.apiPDU.get_varbinds(v1Pdu)

    # Translate Var-Binds
    for oid, v1Val in varBinds:
        # 2.1.1.11
        if v1Val.tagSet == v1.NetworkAddress.tagSet:
            v1Val = v1Val.getComponent()

        v2VarBinds.append((oid, V1_TO_V2_VALUE_MAP[v1Val.tagSet].clone(v1Val)))

    if pduType not in rfc3411.NOTIFICATION_CLASS_PDUS:
        errorStatus = int(v1.apiPDU.get_error_status(v1Pdu))
        errorIndex = int(v1.apiPDU.get_error_index(v1Pdu, muteErrors=True))

        if pduType in rfc3411.RESPONSE_CLASS_PDUS:
            # 4.1.2.2.1&2
            if errorStatus == 2:  # noSuchName
                if origV2Pdu.tagSet == v2c.GetNextRequestPDU.tagSet:
                    v2VarBinds = [(o, rfc1905.endOfMibView) for o, v in v2VarBinds]
                else:
                    v2VarBinds = [(o, rfc1905.noSuchObject) for o, v in v2VarBinds]

        # partial one-to-one mapping - 4.2.1
        v2c.apiPDU.set_error_status(v2Pdu, errorStatus)
        v2c.apiPDU.set_error_index(v2Pdu, errorIndex)

        # 4.1.2.1 --> no-op

        v2c.apiPDU.set_request_id(v2Pdu, int(v1.apiPDU.get_request_id(v1Pdu)))

    else:
        v2c.apiPDU.set_defaults(v2Pdu)

    v2c.apiPDU.set_varbinds(v2Pdu, v2VarBinds)

    debug.logger & debug.FLAG_PRX and debug.logger(
        "v1ToV2: v2Pdu %s" % v2Pdu.prettyPrint()
    )

    return v2Pdu


def v2_to_v1(v2Pdu, origV1Pdu=None):
    """Return SNMPv1 PDU given SNMPv2 PDU."""
    debug.logger & debug.FLAG_PRX and debug.logger(
        "v2ToV1: v2Pdu %s" % v2Pdu.prettyPrint()
    )

    pduType = v2Pdu.tagSet

    if pduType in V2_TO_V1_PDU_MAP:
        v1Pdu = V2_TO_V1_PDU_MAP[pduType].clone()

    else:
        raise error.ProtocolError("Unsupported PDU type")

    v2VarBinds = v2c.apiPDU.get_varbinds(v2Pdu)
    v1VarBinds = []

    # 3.2
    if pduType in rfc3411.NOTIFICATION_CLASS_PDUS:
        if len(v2VarBinds) < 2:
            raise error.ProtocolError(
                "SNMP v2c TRAP PDU requires at least two var-binds"
            )

        # 3.2.1
        snmpTrapOID, snmpTrapOIDParam = v2VarBinds[1]
        if snmpTrapOID != v2c.apiTrapPDU.snmpTrapOID:
            raise error.ProtocolError("Second OID not snmpTrapOID")

        if snmpTrapOIDParam in V2_TO_V1_TRAP_MAP:
            for oid, val in v2VarBinds:
                if oid == v2c.apiTrapPDU.snmpTrapEnterprise:
                    v1.apiTrapPDU.set_enterprise(v1Pdu, val)
                    break

            else:
                # snmpTraps
                v1.apiTrapPDU.set_enterprise(v1Pdu, (1, 3, 6, 1, 6, 3, 1, 1, 5))

        else:
            if snmpTrapOIDParam[-2] == 0:
                v1.apiTrapPDU.set_enterprise(v1Pdu, snmpTrapOIDParam[:-2])

            else:
                v1.apiTrapPDU.set_enterprise(v1Pdu, snmpTrapOIDParam[:-1])

        # 3.2.2
        for oid, val in v2VarBinds:
            # snmpTrapAddress
            if oid == v2c.apiTrapPDU.snmpTrapAddress:
                v1.apiTrapPDU.set_agent_address(
                    v1Pdu, v1.IpAddress(val)
                )  # v2c.OctetString is more constrained
                break

        else:
            v1.apiTrapPDU.set_agent_address(v1Pdu, v1.IpAddress("0.0.0.0"))

        # 3.2.3
        if snmpTrapOIDParam in V2_TO_V1_TRAP_MAP:
            v1.apiTrapPDU.set_generic_trap(v1Pdu, V2_TO_V1_TRAP_MAP[snmpTrapOIDParam])

        else:
            v1.apiTrapPDU.set_generic_trap(v1Pdu, 6)

        # 3.2.4
        if snmpTrapOIDParam in V2_TO_V1_TRAP_MAP:
            v1.apiTrapPDU.set_specific_trap(v1Pdu, zeroInt)

        else:
            v1.apiTrapPDU.set_specific_trap(v1Pdu, snmpTrapOIDParam[-1])

        # 3.2.5
        v1.apiTrapPDU.set_timestamp(v1Pdu, v2VarBinds[0][1])

        __v2VarBinds = []
        for oid, val in v2VarBinds[2:]:
            if oid in V2_TO_V1_TRAP_MAP or oid in (
                v2c.apiTrapPDU.sysUpTime,
                v2c.apiTrapPDU.snmpTrapAddress,
                v2c.apiTrapPDU.snmpTrapEnterprise,
            ):
                continue

            __v2VarBinds.append((oid, val))

        v2VarBinds = __v2VarBinds

        # 3.2.6 --> done below

    else:
        v1.apiPDU.set_error_status(v1Pdu, zeroInt)
        v1.apiPDU.set_error_index(v1Pdu, zeroInt)

    if pduType in rfc3411.RESPONSE_CLASS_PDUS:
        idx = len(v2VarBinds) - 1

        while idx >= 0:
            # 4.1.2.1
            oid, val = v2VarBinds[idx]
            if v2c.Counter64.tagSet == val.tagSet:
                if origV1Pdu.tagSet == v1.GetRequestPDU.tagSet:
                    v1.apiPDU.set_error_status(v1Pdu, 2)
                    v1.apiPDU.set_error_index(v1Pdu, idx + 1)
                    break

                elif origV1Pdu.tagSet == v1.GetNextRequestPDU.tagSet:
                    raise error.StatusInformation(idx=idx, pdu=v2Pdu)

                else:
                    raise error.ProtocolError("Counter64 on the way")

            # 4.1.2.2.1&2
            if val.tagSet in (
                v2c.NoSuchObject.tagSet,
                v2c.NoSuchInstance.tagSet,
                v2c.EndOfMibView.tagSet,
            ):
                v1.apiPDU.set_error_status(v1Pdu, 2)
                v1.apiPDU.set_error_index(v1Pdu, idx + 1)

            idx -= 1

        # 4.1.2.3.1
        v2ErrorStatus = v2c.apiPDU.get_error_status(v2Pdu)
        if v2ErrorStatus:
            v1.apiPDU.set_error_status(v1Pdu, V2_TO_V1_ERROR_MAP.get(v2ErrorStatus, 5))
            v1.apiPDU.set_error_index(
                v1Pdu, v2c.apiPDU.get_error_index(v2Pdu, muteErrors=True)
            )

    elif pduType in rfc3411.CONFIRMED_CLASS_PDUS:
        v1.apiPDU.set_error_status(v1Pdu, 0)
        v1.apiPDU.set_error_index(v1Pdu, 0)

    # Translate Var-Binds
    if pduType in rfc3411.RESPONSE_CLASS_PDUS and v1.apiPDU.get_error_status(v1Pdu):
        v1VarBinds = v1.apiPDU.get_varbinds(origV1Pdu)

    else:
        for oid, v2Val in v2VarBinds:
            v1VarBinds.append((oid, V2_TO_V1_VALUE_MAP[v2Val.tagSet].clone(v2Val)))

    if pduType in rfc3411.NOTIFICATION_CLASS_PDUS:
        v1.apiTrapPDU.set_varbinds(v1Pdu, v1VarBinds)

    else:
        v1.apiPDU.set_varbinds(v1Pdu, v1VarBinds)

        v1.apiPDU.set_request_id(v1Pdu, v2c.apiPDU.get_request_id(v2Pdu))

    debug.logger & debug.FLAG_PRX and debug.logger(
        "v2ToV1: v1Pdu %s" % v1Pdu.prettyPrint()
    )

    return v1Pdu
