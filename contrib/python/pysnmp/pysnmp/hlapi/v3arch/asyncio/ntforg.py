#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# Copyright (C) 2014, Zebra Technologies
# Authors: Matt Hooks <me@matthooks.com>
#          Zachary Lorusso <zlorusso@gmail.com>
#
import asyncio
import warnings


from pysnmp.entity.engine import SnmpEngine
from pysnmp.entity.rfc3413 import ntforg
from pysnmp.hlapi.transport import AbstractTransportTarget
from pysnmp.hlapi.v3arch.asyncio.auth import CommunityData, UsmUserData
from pysnmp.hlapi.v3arch.asyncio.context import ContextData
from pysnmp.hlapi.v3arch.asyncio.lcd import NotificationOriginatorLcdConfigurator
from pysnmp.hlapi.varbinds import NotificationOriginatorVarBinds
from pysnmp.smi.rfc1902 import NotificationType

__all__ = ["send_notification"]

VB_PROCESSOR = NotificationOriginatorVarBinds()
LCD = NotificationOriginatorLcdConfigurator()


async def send_notification(
    snmpEngine: SnmpEngine,
    authData: "CommunityData | UsmUserData",
    transportTarget: AbstractTransportTarget,
    contextData: ContextData,
    notifyType: str,
    *varBinds: NotificationType,
    **options,
):
    r"""Creates a generator to send SNMP notification.

    When iterator gets advanced by :py:mod:`asyncio` main loop,
    SNMP TRAP or INFORM notification is send (:RFC:`1905#section-4.2.6`).
    The iterator yields :py:class:`asyncio.get_running_loop().create_future()` which gets done whenever
    response arrives or error occurs.

    Parameters
    ----------
    snmpEngine : :py:class:`~pysnmp.hlapi.v3arch.asyncio.SnmpEngine`
        Class instance representing SNMP engine.

    authData : :py:class:`~pysnmp.hlapi.v3arch.asyncio.CommunityData` or :py:class:`~pysnmp.hlapi.v3arch.asyncio.UsmUserData`
        Class instance representing SNMP credentials.

    transportTarget : :py:class:`~pysnmp.hlapi.v3arch.asyncio.UdpTransportTarget` or :py:class:`~pysnmp.hlapi.v3arch.asyncio.Udp6TransportTarget`
        Class instance representing transport type along with SNMP peer address.

    contextData : :py:class:`~pysnmp.hlapi.v3arch.asyncio.ContextData`
        Class instance representing SNMP ContextEngineId and ContextName values.

    notifyType : str
        Indicates type of notification to be sent. Recognized literal
        values are *trap* or *inform*.

    \*varBinds: :class:`tuple` of OID-value pairs or :py:class:`~pysnmp.smi.rfc1902.ObjectType` or :py:class:`~pysnmp.smi.rfc1902.NotificationType`
        One or more objects representing MIB variables to place
        into SNMP notification. It could be tuples of OID-values
        or :py:class:`~pysnmp.smi.rfc1902.ObjectType` class instances
        of :py:class:`~pysnmp.smi.rfc1902.NotificationType` objects.

        SNMP Notification PDU places rigid requirement on the ordering of
        the variable-bindings.

        Mandatory variable-bindings:

        0. SNMPv2-MIB::sysUpTime.0 = <agent uptime>
        1. SNMPv2-SMI::snmpTrapOID.0 = {SNMPv2-MIB::coldStart, ...}

        Optional variable-bindings (applicable to SNMP v1 TRAP):

        2. SNMP-COMMUNITY-MIB::snmpTrapAddress.0 = <agent-IP>
        3. SNMP-COMMUNITY-MIB::snmpTrapCommunity.0 = <snmp-community-name>
        4. SNMP-COMMUNITY-MIB::snmpTrapEnterprise.0 = <enterprise-OID>

        Informational variable-bindings:

        * SNMPv2-SMI::NOTIFICATION-TYPE
        * SNMPv2-SMI::OBJECT-TYPE

    Other Parameters
    ----------------
    \*\*options :
        Request options:

            * `lookupMib` - load MIB and resolve response MIB variables at
              the cost of slightly reduced performance. Default is `True`.

    Yields
    ------
    errorIndication : :py:class:`~pysnmp.proto.errind.ErrorIndication`
        True value indicates SNMP engine error.
    errorStatus : str
        True value indicates SNMP PDU error.
    errorIndex : int
        Non-zero value refers to `varBinds[errorIndex-1]`
    varBinds : tuple
        A sequence of :py:class:`~pysnmp.smi.rfc1902.ObjectType` class
        instances representing MIB variables returned in SNMP response.

    Raises
    ------
    PySnmpError
        Or its derivative indicating that an error occurred while
        performing SNMP operation.

    Examples
    --------
    >>> import asyncio
    >>> from pysnmp.hlapi.v3arch.asyncio import *
    >>>
    >>> async def run():
    ...     errorIndication, errorStatus, errorIndex, varBinds = await send_notification(
    ...         SnmpEngine(),
    ...         CommunityData('public'),
    ...         await UdpTransportTarget.create(('demo.pysnmp.com', 162)),
    ...         ContextData(),
    ...         'trap',
    ...         NotificationType(ObjectIdentity('IF-MIB', 'linkDown')))
    ...     print(errorIndication, errorStatus, errorIndex, varBinds)
    ...
    >>> asyncio.run(run())
    (None, 0, 0, [])
    >>>

    """

    def __callback(
        snmpEngine: SnmpEngine,
        sendRequestHandle,
        errorIndication,
        errorStatus,
        errorIndex,
        varBinds,
        cbCtx,
    ):
        lookupMib, future = cbCtx
        if future.cancelled():
            return
        try:
            varBindsUnmade = VB_PROCESSOR.unmake_varbinds(
                snmpEngine.cache, varBinds, lookupMib
            )
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(
                (errorIndication, errorStatus, errorIndex, varBindsUnmade)
            )

    notifyName = LCD.configure(
        snmpEngine, authData, transportTarget, notifyType, contextData.contextName
    )

    future = asyncio.get_running_loop().create_future()

    ntforg.NotificationOriginator().send_varbinds(
        snmpEngine,
        notifyName,
        contextData.contextEngineId,
        contextData.contextName,
        VB_PROCESSOR.make_varbinds(snmpEngine.cache, varBinds),
        __callback,
        (options.get("lookupMib", True), future),
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
