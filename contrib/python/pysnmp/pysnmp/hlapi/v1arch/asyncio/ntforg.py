#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import asyncio
import warnings

from pysnmp.hlapi.transport import AbstractTransportTarget
from pysnmp.hlapi.v1arch.asyncio.auth import CommunityData
from pysnmp.hlapi.v1arch.asyncio.dispatch import SnmpDispatcher
from pysnmp.hlapi.varbinds import NotificationOriginatorVarBinds, NotificationType
from pysnmp.proto import api, errind, error
from pysnmp.proto.api import v2c
from pysnmp.proto.proxy import rfc2576
from pysnmp.proto.rfc1902 import Integer32
from pysnmp.smi.rfc1902 import ObjectType

__all__ = ["send_notification"]

VB_PROCESSOR = NotificationOriginatorVarBinds()


async def send_notification(
    snmpDispatcher: SnmpDispatcher,
    authData: CommunityData,
    transportTarget: AbstractTransportTarget,
    notifyType: str,
    *varBinds: NotificationType,
    **options,
) -> "tuple[errind.ErrorIndication, Integer32 | int, Integer32 | int, tuple[ObjectType, ...]]":
    r"""Creates a generator to send SNMP notification.

    When iterator gets advanced by :py:mod:`asyncio` main loop,
    SNMP TRAP or INFORM notification is send (:RFC:`1905#section-4.2.6`).
    The iterator yields :py:class:`asyncio.Future` which gets done whenever
    response arrives or error occurs.

    Parameters
    ----------
    snmpDispatcher: :py:class:`~pysnmp.hlapi.v1arch.asyncio.SnmpDispatcher`
        Class instance representing asynio-based asynchronous event loop and
        associated state information.

    authData: :py:class:`~pysnmp.hlapi.v1arch.CommunityData`
        Class instance representing SNMPv1/v2c credentials.

    transportTarget: :py:class:`~pysnmp.hlapi.v1arch.asyncio.UdpTransportTarget` or
        :py:class:`~pysnmp.hlapi.v1arch.asyncio.Udp6TransportTarget` Class instance representing
        transport type along with SNMP peer address.

    notifyType : str
        Indicates type of notification to be sent. Recognized literal
        values are *trap* or *inform*.

    \*varBinds: :class:`tuple` of OID-value pairs or :py:class:`~pysnmp.smi.rfc1902.ObjectType` or :py:class:`~pysnmp.smi.rfc1902.NotificationType`
        One or more objects representing MIB variables to place
        into SNMP notification. It could be tuples of OID-values
        or :py:class:`~pysnmp.smi.rfc1902.ObjectType` class instances
        of :py:class:`~pysnmp.smi.rfc1902.NotificationType` objects.

        Besides user variable-bindings, SNMP Notification PDU requires at
        least two variable-bindings to be present:

        0. SNMPv2-MIB::sysUpTime.0 = <agent uptime>
        1. SNMPv2-SMI::snmpTrapOID.0 = <notification ID>

        When sending SNMPv1 TRAP, more variable-bindings could be present:

        2. SNMP-COMMUNITY-MIB::snmpTrapAddress.0 = <agent-IP>
        3. SNMP-COMMUNITY-MIB::snmpTrapCommunity.0 = <snmp-community-name>
        4. SNMP-COMMUNITY-MIB::snmpTrapEnterprise.0 = <enterprise-OID>

        If user does not supply some or any of the above variable-bindings or
        if they are at the wrong positions, the system will add/reorder the
        missing ones automatically.

        On top of that, some notification types imply including some additional
        variable-bindings providing additional details on the event being
        reported. Therefore it is generally easier to use
        :py:class:`~pysnmp.smi.rfc1902.NotificationType` object which will
        help adding relevant variable-bindings.

    Other Parameters
    ----------------
    \*\*options :
        Request options:

        * `lookupMib` - load MIB and resolve response MIB variables at
          the cost of slightly reduced performance. Default is `False`,
          unless :py:class:`~pysnmp.smi.rfc1902.ObjectType` or
          :py:class:`~pysnmp.smi.rfc1902.NotificationType` is present
          among `varBinds` in which case `lookupMib` gets automatically
          enabled.

    Yields
    ------
    errorIndication: str
        True value indicates SNMP engine error.
    errorStatus: str
        True value indicates SNMP PDU error.
    errorIndex: int
        Non-zero value refers to `varBinds[errorIndex-1]`
    varBinds: tuple
        A sequence of OID-value pairs in form of base SNMP types (if
        `lookupMib` is `False`) or :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        class instances (if `lookupMib` is `True`) representing MIB variables
        returned in SNMP response.

    Raises
    ------
    PySnmpError
        Or its derivative indicating that an error occurred while
        performing SNMP operation.

    Examples
    --------
    >>> import asyncio
    >>> from pysnmp.hlapi.v1arch.asyncio import *
    >>>
    >>> async def run():
    ...     errorIndication, errorStatus, errorIndex, varBinds = await send_notification(
    ...         SnmpDispatcher(),
    ...         CommunityData('public'),
    ...         await UdpTransportTarget.create(('demo.pysnmp.com', 162)),
    ...         'trap',
    ...         NotificationType(ObjectIdentity('IF-MIB', 'linkDown')))
    ...     print(errorIndication, errorStatus, errorIndex, varBinds)
    ...
    >>> asyncio.run(run())
    (None, 0, 0, [])
    >>>
    """
    sysUpTime = v2c.apiTrapPDU.sysUpTime
    snmpTrapOID = v2c.apiTrapPDU.snmpTrapOID

    def _ensure_varbinds(varBinds):
        # Add sysUpTime if not present already
        if not varBinds or varBinds[0][0] != sysUpTime:
            varBinds.insert(0, (v2c.ObjectIdentifier(sysUpTime), v2c.TimeTicks(0)))

        # Search for and reposition sysUpTime if it's elsewhere
        for idx, varBind in enumerate(varBinds[1:]):
            if varBind[0] == sysUpTime:
                varBinds[0] = varBind
                del varBinds[idx + 1]
                break

        if len(varBinds) < 2:
            raise error.PySnmpError(
                "SNMP notification PDU requires "
                "SNMPv2-MIB::snmpTrapOID.0 to be present"
            )

        # Search for and reposition snmpTrapOID if it's elsewhere
        for idx, varBind in enumerate(varBinds[2:]):
            if varBind[0] == snmpTrapOID:
                del varBinds[idx + 2]
                if varBinds[1][0] == snmpTrapOID:
                    varBinds[1] = varBind
                else:
                    varBinds.insert(1, varBind)
                break

        # Fail on missing snmpTrapOID
        if varBinds[1][0] != snmpTrapOID:
            raise error.PySnmpError(
                "SNMP notification PDU requires "
                "SNMPv2-MIB::snmpTrapOID.0 to be present"
            )

        return varBinds

    def __callback(snmpDispatcher, stateHandle, errorIndication, rspPdu, _cbCtx):
        lookupMib, future = _cbCtx
        if future.cancelled():
            return

        errorStatus = v2c.apiTrapPDU.get_error_status(rspPdu)
        errorIndex = v2c.apiTrapPDU.get_error_index(rspPdu)

        varBinds = v2c.apiTrapPDU.get_varbinds(rspPdu)

        try:
            varBindsUnmade = VB_PROCESSOR.unmake_varbinds(
                snmpDispatcher.cache, varBinds, lookupMib  # type: ignore
            )
        except Exception as e:
            future.set_exception(e)

        else:
            future.set_result(
                (errorIndication, errorStatus, errorIndex, varBindsUnmade)
            )

    lookupMib = options.get("lookupMib")

    if not lookupMib and any(
        isinstance(x, (NotificationType, ObjectType)) for x in varBinds
    ):
        lookupMib = True

    if lookupMib:
        inputVarBinds = VB_PROCESSOR.make_varbinds(snmpDispatcher.cache, varBinds)

    # # make sure required PDU payload is in place
    # completeVarBinds = []
    #
    # # ensure sysUpTime
    # if len(varBinds) < 1 or varBinds[0][0] != pMod.apiTrapPDU.sysUpTime:
    #     varBinds.insert(0, (ObjectIdentifier(pMod.apiTrapPDU.sysUpTime), pMod.Integer(0)))
    #
    # # ensure sysUpTime
    # if len(varBinds) < 1 or varBinds[0][0] != pMod.apiTrapPDU.sysUpTime:
    #     varBinds.insert(0, (ObjectIdentifier(pMod.apiTrapPDU.sysUpTime), pMod.Integer(0)))
    #
    # # ensure snmpTrapOID
    # if len(varBinds) < 2 or varBinds[1][0] != pMod.apiTrapPDU.snmpTrapOID:
    #     varBinds.insert(0, (ObjectIdentifier(pMod.apiTrapPDU.sysUpTime), pMod.Integer(0)))

    # input PDU is always v2c
    pMod = api.PROTOCOL_MODULES[api.SNMP_VERSION_2C]

    if notifyType == "trap":
        reqPdu = pMod.TrapPDU()
    else:
        reqPdu = v2c.InformRequestPDU()

    v2c.apiTrapPDU.set_defaults(reqPdu)
    v2c.apiTrapPDU.set_varbinds(reqPdu, inputVarBinds)

    inputVarBinds = v2c.apiTrapPDU.get_varbinds(reqPdu)

    v2c.apiTrapPDU.set_varbinds(reqPdu, _ensure_varbinds(inputVarBinds))

    if authData.mpModel == 0:
        reqPdu = rfc2576.v2_to_v1(reqPdu)

    future = asyncio.Future()

    snmpDispatcher.send_pdu(
        authData, transportTarget, reqPdu, cbFun=__callback, cbCtx=(lookupMib, future)
    )

    if notifyType == "trap":

        def __trap_function(future):
            if future.cancelled():
                return
            future.set_result((None, 0, 0, []))

        loop = asyncio.get_event_loop()
        loop.call_soon(__trap_function, future)

    return await future


# Compatibility API
deprecated_attributes = {
    "sendNotification": "send_notification",
}


def __getattr__(attr: str):
    if new_attr := deprecated_attributes.get(attr):
        warnings.warn(
            f"{attr} is deprecated. Please use {new_attr} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[new_attr]
    raise AttributeError(f"module '{__name__}' has no attribute '{attr}'")
