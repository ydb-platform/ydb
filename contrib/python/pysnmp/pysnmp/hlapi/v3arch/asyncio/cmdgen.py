#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# Copyright (C) 2014, Zebra Technologies
# Authors: Matt Hooks <me@matthooks.com>
#          Zachary Lorusso <zlorusso@gmail.com>
# Modified by Ilya Etingof <ilya@snmplabs.com>
#
# Copyright (C) 2024, LeXtudio Inc. <support@lextudio.com>
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
#
import asyncio
import warnings
from typing import AsyncGenerator

from pysnmp.entity.engine import SnmpEngine
from pysnmp.entity.rfc3413 import cmdgen
from pysnmp.hlapi import varbinds
from pysnmp.hlapi.v3arch.asyncio.auth import CommunityData, UsmUserData
from pysnmp.hlapi.v3arch.asyncio.context import ContextData
from pysnmp.hlapi.v3arch.asyncio.lcd import CommandGeneratorLcdConfigurator
from pysnmp.hlapi.v3arch.asyncio.transport import AbstractTransportTarget
from pysnmp.proto import errind
from pysnmp.proto.rfc1902 import Integer32, Null
from pysnmp.proto.rfc1905 import EndOfMibView, endOfMibView
from pysnmp.smi.rfc1902 import ObjectType


__all__ = [
    "get_cmd",
    "next_cmd",
    "set_cmd",
    "bulk_cmd",
    "walk_cmd",
    "bulk_walk_cmd",
    "is_end_of_mib",
]

VB_PROCESSOR = varbinds.CommandGeneratorVarBinds()
LCD = CommandGeneratorLcdConfigurator()
is_end_of_mib = varbinds.is_end_of_mib


async def get_cmd(
    snmpEngine: SnmpEngine,
    authData: "CommunityData | UsmUserData",
    transportTarget: AbstractTransportTarget,
    contextData: ContextData,
    *varBinds: ObjectType,
    **options,
) -> "tuple[errind.ErrorIndication, Integer32 | int, Integer32 | int, tuple[ObjectType, ...]]":
    r"""Creates a generator to perform SNMP GET query.

    When iterator gets advanced by :py:mod:`asyncio` main loop,
    SNMP GET request is send (:RFC:`1905#section-4.2.1`).
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

    \*varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        One or more class instances representing MIB variables to place
        into SNMP request.

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
    ...     errorIndication, errorStatus, errorIndex, varBinds = await get_cmd(
    ...         SnmpEngine(),
    ...         CommunityData('public'),
    ...         await UdpTransportTarget.create(('demo.pysnmp.com', 161)),
    ...         ContextData(),
    ...         ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0))
    ...     )
    ...     print(errorIndication, errorStatus, errorIndex, varBinds)
    >>>
    >>> asyncio.run(run())
    (None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('SunOS zeus.pysnmp.com 4.1.3_U1 1 sun4m'))])
    >>>

    """

    def __callback(
        snmpEngine: SnmpEngine,
        sendRequestHandle,
        errorIndication: errind.ErrorIndication,
        errorStatus: "Integer32 | int",
        errorIndex: "Integer32 | int",
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
            future.set_result((
                errorIndication,
                errorStatus,
                errorIndex,
                varBindsUnmade,
            ))

    addrName, paramsName = LCD.configure(
        snmpEngine, authData, transportTarget, contextData.contextName
    )

    future = asyncio.get_running_loop().create_future()

    cmdgen.GetCommandGenerator().send_varbinds(
        snmpEngine,
        addrName,
        contextData.contextEngineId,
        contextData.contextName,
        VB_PROCESSOR.make_varbinds(snmpEngine.cache, varBinds),
        __callback,
        (options.get("lookupMib", True), future),
    )
    return await future


async def set_cmd(
    snmpEngine: SnmpEngine,
    authData: "CommunityData | UsmUserData",
    transportTarget: AbstractTransportTarget,
    contextData: ContextData,
    *varBinds: ObjectType,
    **options,
) -> "tuple[errind.ErrorIndication, Integer32 | int, Integer32 | int, tuple[ObjectType, ...]]":
    r"""Creates a generator to perform SNMP SET query.

    When iterator gets advanced by :py:mod:`asyncio` main loop,
    SNMP SET request is send (:RFC:`1905#section-4.2.5`).
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

    \*varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        One or more class instances representing MIB variables to place
        into SNMP request.

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
    ...     errorIndication, errorStatus, errorIndex, varBinds = await set_cmd(
    ...         SnmpEngine(),
    ...         CommunityData('public'),
    ...         await UdpTransportTarget.create(('demo.pysnmp.com', 161)),
    ...         ContextData(),
    ...         ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0), 'Linux i386')
    ...     )
    ...     print(errorIndication, errorStatus, errorIndex, varBinds)
    >>>
    >>> asyncio.run(run())
    (None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('Linux i386'))])
    >>>

    """

    def __callback(
        snmpEngine: SnmpEngine,
        sendRequestHandle,
        errorIndication: errind.ErrorIndication,
        errorStatus: "Integer32 | int",
        errorIndex: "Integer32 | int",
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
            future.set_result((
                errorIndication,
                errorStatus,
                errorIndex,
                varBindsUnmade,
            ))

    addrName, paramsName = LCD.configure(
        snmpEngine, authData, transportTarget, contextData.contextName
    )

    future = asyncio.get_running_loop().create_future()

    cmdgen.SetCommandGenerator().send_varbinds(
        snmpEngine,
        addrName,
        contextData.contextEngineId,
        contextData.contextName,
        VB_PROCESSOR.make_varbinds(snmpEngine.cache, varBinds),
        __callback,
        (options.get("lookupMib", True), future),
    )
    return await future


async def next_cmd(
    snmpEngine: SnmpEngine,
    authData: "CommunityData | UsmUserData",
    transportTarget: AbstractTransportTarget,
    contextData: ContextData,
    *varBinds: ObjectType,
    **options,
) -> "tuple[errind.ErrorIndication, Integer32 | str | int, Integer32 | int, tuple[ObjectType, ...]]":
    r"""Creates a generator to perform SNMP GETNEXT query.

    When iterator gets advanced by :py:mod:`asyncio` main loop,
    SNMP GETNEXT request is send (:RFC:`1905#section-4.2.2`).
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

    \*varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        One or more class instances representing MIB variables to place
        into SNMP request.

    Other Parameters
    ----------------
    \*\*options :
        Request options:

            * `lookupMib` - load MIB and resolve response MIB variables at
              the cost of slightly reduced performance. Default is `True`.
            * `ignoreNonIncreasingOid` - continue iteration even if response
              MIB variables (OIDs) are not greater then request MIB variables.
              Be aware that setting it to `True` may cause infinite loop between
              SNMP management and agent applications. Default is `False`.

    Yields
    ------
    errorIndication : :py:class:`~pysnmp.proto.errind.ErrorIndication`
        True value indicates SNMP engine error.
    errorStatus : str
        True value indicates SNMP PDU error.
    errorIndex : int
        Non-zero value refers to `varBinds[errorIndex-1]`
    varBinds : tuple
        A sequence of sequences (e.g. 2-D array) of
        :py:class:`~pysnmp.smi.rfc1902.ObjectType` class instances
        representing a table of MIB variables returned in SNMP response.
        Inner sequences represent table rows and ordered exactly the same
        as `varBinds` in request. Response to GETNEXT always contain
        a single row.

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
    ...     errorIndication, errorStatus, errorIndex, varBinds = await next_cmd(
    ...         SnmpEngine(),
    ...         CommunityData('public'),
    ...         await UdpTransportTarget.create(('demo.pysnmp.com', 161)),
    ...         ContextData(),
    ...         ObjectType(ObjectIdentity('SNMPv2-MIB', 'system'))
    ...     )
    ...     print(errorIndication, errorStatus, errorIndex, varBinds)
    >>>
    >>> asyncio.run(run())
    (None, 0, 0, [[ObjectType(ObjectIdentity('1.3.6.1.2.1.1.1.0'), DisplayString('Linux i386'))]])
    >>>

    """

    def __callback(
        snmpEngine: SnmpEngine,
        sendRequestHandle,
        errorIndication: errind.ErrorIndication,
        errorStatus: "Integer32 | int",
        errorIndex: "Integer32 | int",
        varBinds,
        cbCtx,
    ):
        lookupMib, future = cbCtx
        if future.cancelled():
            return

        if (
            options.get("ignoreNonIncreasingOid", False)
            and errorIndication
            and isinstance(errorIndication, errind.OidNotIncreasing)
        ):
            errorIndication = None  # TODO: fix this

        try:
            varBindsUnmade = VB_PROCESSOR.unmake_varbinds(
                snmpEngine.cache, varBinds, lookupMib
            )
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result((
                errorIndication,
                errorStatus,
                errorIndex,
                varBindsUnmade,
            ))

    addrName, paramsName = LCD.configure(
        snmpEngine, authData, transportTarget, contextData.contextName
    )

    future = asyncio.get_running_loop().create_future()

    cmdgen.NextCommandGenerator().send_varbinds(
        snmpEngine,
        addrName,
        contextData.contextEngineId,
        contextData.contextName,
        VB_PROCESSOR.make_varbinds(snmpEngine.cache, varBinds),
        __callback,
        (options.get("lookupMib", True), future),
    )
    return await future


async def bulk_cmd(
    snmpEngine: SnmpEngine,
    authData: "CommunityData | UsmUserData",
    transportTarget: AbstractTransportTarget,
    contextData: ContextData,
    nonRepeaters: int,
    maxRepetitions: int,
    *varBinds: ObjectType,
    **options,
) -> "tuple[errind.ErrorIndication, Integer32 | str | int, Integer32 | int, tuple[ObjectType, ...]]":
    r"""Creates a generator to perform SNMP GETBULK query.

    When iterator gets advanced by :py:mod:`asyncio` main loop,
    SNMP GETBULK request is send (:RFC:`1905#section-4.2.3`).
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

    nonRepeaters : int
        One MIB variable is requested in response for the first
        `nonRepeaters` MIB variables in request.

    maxRepetitions : int
        `maxRepetitions` MIB variables are requested in response for each
        of the remaining MIB variables in the request (e.g. excluding
        `nonRepeaters`). Remote SNMP engine may choose lesser value than
        requested.

    \*varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        One or more class instances representing MIB variables to place
        into SNMP request.

    Other Parameters
    ----------------
    \*\*options :
        Request options:

            * `lookupMib` - load MIB and resolve response MIB variables at
              the cost of slightly reduced performance. Default is `True`.
            * `ignoreNonIncreasingOid` - continue iteration even if response
              MIB variables (OIDs) are not greater then request MIB variables.
              Be aware that setting it to `True` may cause infinite loop between
              SNMP management and agent applications. Default is `False`.

    Yields
    ------
    errorIndication : :py:class:`~pysnmp.proto.errind.ErrorIndication`
        True value indicates SNMP engine error.
    errorStatus : str
        True value indicates SNMP PDU error.
    errorIndex : int
        Non-zero value refers to `varBinds[errorIndex-1]`
    varBindTable : tuple
        A flat sequence of :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        instances representing the variables returned in the SNMP
        GETBULK response in wire order. The order is:

        - First, the non-repeaters (at most one successor each, in the
          same order as requested);
        - Then, up to ``maxRepetitions`` groups of the repeaters, where
          each group contains the next lexicographic successor for each
          repeater in request order.

        The maximum length is
        ``nonRepeaters + maxRepetitions * (len(varBinds) - nonRepeaters)``,
        but the agent can return fewer items (e.g., due to
        :py:obj:`~pysnmp.proto.rfc1905.endOfMibView` or response truncation).

        See :rfc:`3416#section-4.2.3` for details on the underlying
        ``GetBulkRequest-PDU`` and the associated ``GetResponse-PDU``.

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
    ...     errorIndication, errorStatus, errorIndex, varBinds = await bulk_cmd(
    ...         SnmpEngine(),
    ...         CommunityData('public'),
    ...         await UdpTransportTarget.create(('demo.pysnmp.com', 161)),
    ...         ContextData(),
    ...         0, 2,
    ...         ObjectType(ObjectIdentity('SNMPv2-MIB', 'system'))
    ...     )
    ...     print(errorIndication, errorStatus, errorIndex, varBinds)
    >>>
    >>> asyncio.run(run())
    (None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('SunOS zeus.pysnmp.com 4.1.3_U1 1 sun4m')), ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.2.0')), ObjectIdentifier('1.3.6.1.4.1.424242.1.1'))])
    >>>

    """

    def __callback(
        snmpEngine: SnmpEngine,
        sendRequestHandle,
        errorIndication: errind.ErrorIndication,
        errorStatus: "Integer32 | int",
        errorIndex: "Integer32 | int",
        varBinds,
        cbCtx,
    ):
        lookupMib, future = cbCtx
        if future.cancelled():
            return

        if (
            options.get("ignoreNonIncreasingOid", False)
            and errorIndication
            and isinstance(errorIndication, errind.OidNotIncreasing)
        ):
            errorIndication = None  # TODO: fix here

        try:
            varBindsUnmade = VB_PROCESSOR.unmake_varbinds(
                snmpEngine.cache, varBinds, lookupMib
            )
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result((
                errorIndication,
                errorStatus,
                errorIndex,
                varBindsUnmade,
            ))

    addrName, paramsName = LCD.configure(
        snmpEngine, authData, transportTarget, contextData.contextName
    )

    future = asyncio.get_running_loop().create_future()

    cmdgen.BulkCommandGenerator().send_varbinds(
        snmpEngine,
        addrName,
        contextData.contextEngineId,
        contextData.contextName,
        nonRepeaters,
        maxRepetitions,
        VB_PROCESSOR.make_varbinds(snmpEngine.cache, varBinds),
        __callback,
        (options.get("lookupMib", True), future),
    )
    return await future


async def walk_cmd(
    snmpEngine: SnmpEngine,
    authData: "CommunityData | UsmUserData",
    transportTarget: AbstractTransportTarget,
    contextData: ContextData,
    varBind: ObjectType,
    **options,
) -> AsyncGenerator[
    "tuple[errind.ErrorIndication | None, Integer32 | str | int | None, Integer32 | int | None, tuple[ObjectType, ...]]",
    None,
]:
    r"""Creates a generator to perform one or more SNMP GETNEXT queries.

    On each iteration, new SNMP GETNEXT request is send
    (:RFC:`1905#section-4.2.2`). The iterator blocks waiting for response
    to arrive or error to occur.

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

    varBind : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        One class instance representing MIB variables to place
        into SNMP request.

    Other Parameters
    ----------------
    \*\*options :
        Request options:

            * `lookupMib` - load MIB and resolve response MIB variables at
              the cost of slightly reduced performance. Default is `True`.
            * `lexicographicMode` - walk SNMP agent's MIB till the end (if `True`),
              otherwise (if `False`) stop iteration when all response MIB
              variables leave the scope of initial MIB variables in
              `varBinds`. Default is `True`.
            * `ignoreNonIncreasingOid` - continue iteration even if response
              MIB variables (OIDs) are not greater then request MIB variables.
              Be aware that setting it to `True` may cause infinite loop between
              SNMP management and agent applications. Default is `False`.
            * `maxRows` - stop iteration once this generator instance processed
              `maxRows` of SNMP conceptual table. Default is `0` (no limit).
            * `maxCalls` - stop iteration once this generator instance processed
              `maxCalls` responses. Default is 0 (no limit).

    Yields
    ------
    errorIndication : str
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

    Notes
    -----
    The `walk_cmd` generator will be exhausted on any of the following
    conditions:

    * SNMP engine error occurs thus `errorIndication` is `True`
    * SNMP PDU `errorStatus` is reported as `True`
    * SNMP :py:class:`~pysnmp.proto.rfc1905.EndOfMibView` values
      (also known as *SNMP exception values*) are reported for all
      MIB variables in `varBinds`
    * *lexicographicMode* option is `True` and SNMP agent reports
      end-of-mib or *lexicographicMode* is `False` and all
      response MIB variables leave the scope of `varBinds`

    At any moment a new sequence of `varBinds` could be send back into
    running generator (supported since Python 2.6).

    Examples
    --------
    >>> from pysnmp.hlapi.v3arch.asyncio import *
    >>> objects = walk_cmd(SnmpEngine(),
    ...             CommunityData('public'),
    ...             await UdpTransportTarget.create(('demo.pysnmp.com', 161)),
    ...             ContextData(),
    ...             ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr')))
    ... g = [item async for item in objects]
    >>> next(g)
    (None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('SunOS zeus.pysnmp.com 4.1.3_U1 1 sun4m'))])
    >>> g.send( [ ObjectType(ObjectIdentity('IF-MIB', 'ifInOctets')) ] )
    (None, 0, 0, [(ObjectName('1.3.6.1.2.1.2.2.1.10.1'), Counter32(284817787))])
    """
    lexicographicMode = options.get("lexicographicMode", True)
    ignoreNonIncreasingOid = options.get("ignoreNonIncreasingOid", False)
    maxRows = options.get("maxRows", 0)
    maxCalls = options.get("maxCalls", 0)

    initialVars = [
        x[0] for x in VB_PROCESSOR.make_varbinds(snmpEngine.cache, (varBind,))
    ]

    totalRows = totalCalls = 0

    while True:
        if varBind:
            errorIndication, errorStatus, errorIndex, varBindTable = await next_cmd(
                snmpEngine,
                authData,
                transportTarget,
                contextData,
                (varBind[0], Null("")),
                **dict(lookupMib=options.get("lookupMib", True)),
            )
            if (
                ignoreNonIncreasingOid
                and errorIndication
                and isinstance(errorIndication, errind.OidNotIncreasing)
            ):
                errorIndication = None

            if errorIndication:
                yield (errorIndication, errorStatus, errorIndex, (varBind,))
                return
            elif errorStatus:
                if errorStatus == 2:
                    # Hide SNMPv1 noSuchName error which leaks in here
                    # from SNMPv1 Agent through internal pysnmp proxy.
                    errorStatus = 0
                    errorIndex = 0
                return
            else:
                stopFlag = True

                varBind = varBindTable[0]

                name, val = varBind
                foundEnding = isinstance(val, Null) or isinstance(val, EndOfMibView)
                foundBeyond = not lexicographicMode and not initialVars[0].isPrefixOf(
                    name
                )
                if foundEnding or foundBeyond:
                    return

                if stopFlag and varBind[1] is not endOfMibView:
                    stopFlag = False

                if stopFlag:
                    return

                totalRows += 1
                totalCalls += 1
        else:
            errorIndication = errorStatus = errorIndex = None
            varBind = None

        initialVarBinds: "tuple[ObjectType, ...]|None" = yield (
            errorIndication,
            errorStatus,
            errorIndex,
            (varBind,),
        )

        if initialVarBinds:
            varBind = initialVarBinds[0]
            initialVars = [
                x[0]
                for x in VB_PROCESSOR.make_varbinds(snmpEngine.cache, initialVarBinds)
            ]

        if maxRows and totalRows >= maxRows:
            return

        if maxCalls and totalCalls >= maxCalls:
            return


async def bulk_walk_cmd(
    snmpEngine: SnmpEngine,
    authData: "CommunityData | UsmUserData",
    transportTarget: AbstractTransportTarget,
    contextData: ContextData,
    nonRepeaters: int,
    maxRepetitions: int,
    varBind: ObjectType,
    **options,
) -> AsyncGenerator[
    "tuple[errind.ErrorIndication | None, Integer32 | str | int | None, Integer32 | int | None, tuple[ObjectType, ...]]",
    None,
]:
    r"""Creates a generator to perform one or more SNMP GETBULK queries.

    On each iteration, new SNMP GETBULK request is send
    (:RFC:`1905#section-4.2.3`). The iterator blocks waiting for response
    to arrive or error to occur.

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

    nonRepeaters : int
        One MIB variable is requested in response for the first
        `nonRepeaters` MIB variables in request.

    maxRepetitions : int
        `maxRepetitions` MIB variables are requested in response for each
        of the remaining MIB variables in the request (e.g. excluding
        `nonRepeaters`). Remote SNMP engine may choose lesser value than
        requested.

    varBind : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
        One class instance representing MIB variables to place
        into SNMP request.

    Other Parameters
    ----------------
    \*\*options :
        Request options:

            * `lookupMib` - load MIB and resolve response MIB variables at
              the cost of slightly reduced performance. Default is `True`.
            * `lexicographicMode` - walk SNMP agent's MIB till the end (if `True`),
              otherwise (if `False`) stop iteration when all response MIB
              variables leave the scope of initial MIB variables in
              `varBinds`. Default is `True`.
            * `ignoreNonIncreasingOid` - continue iteration even if response
              MIB variables (OIDs) are not greater then request MIB variables.
              Be aware that setting it to `True` may cause infinite loop between
              SNMP management and agent applications. Default is `False`.
            * `maxRows` - stop iteration once this generator instance processed
              `maxRows` of SNMP conceptual table. Default is `0` (no limit).
            * `maxCalls` - stop iteration once this generator instance processed
              `maxCalls` responses. Default is 0 (no limit).

    Yields
    ------
    errorIndication : str
        True value indicates SNMP engine error.
    errorStatus : str
        True value indicates SNMP PDU error.
    errorIndex : int
        Non-zero value refers to \*varBinds[errorIndex-1]
    varBinds : tuple
        A sequence of :py:class:`~pysnmp.smi.rfc1902.ObjectType` class
        instances representing MIB variables returned in SNMP response.

    Raises
    ------
    PySnmpError
        Or its derivative indicating that an error occurred while
        performing SNMP operation.

    Notes
    -----
    The `bulk_walk_cmd` generator will be exhausted on any of the following
    conditions:

    * SNMP engine error occurs thus `errorIndication` is `True`
    * SNMP PDU `errorStatus` is reported as `True`
    * SNMP :py:class:`~pysnmp.proto.rfc1905.EndOfMibView` values
      (also known as *SNMP exception values*) are reported for all
      MIB variables in `varBinds`
    * *lexicographicMode* option is `True` and SNMP agent reports
      end-of-mib or *lexicographicMode* is `False` and all
      response MIB variables leave the scope of `varBinds`

    At any moment a new sequence of `varBinds` could be send back into
    running generator (supported since Python 2.6).

    Setting `maxRepetitions` value to 15..50 might significantly improve
    system performance, as many MIB variables get packed into a single
    response message at once.

    Examples
    --------
    >>> from pysnmp.hlapi.v3arch.asyncio import *
    >>> objects = bulk_walk_cmd(SnmpEngine(),
    ...             CommunityData('public'),
    ...             await UdpTransportTarget.create(('demo.pysnmp.com', 161)),
    ...             ContextData(),
    ...             0, 25,
    ...             ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr')))
    ... g = [item async for item in objects]
    >>> next(g)
    (None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('SunOS zeus.pysnmp.com 4.1.3_U1 1 sun4m'))])
    >>> g.send( [ ObjectType(ObjectIdentity('IF-MIB', 'ifInOctets')) ] )
    (None, 0, 0, [(ObjectName('1.3.6.1.2.1.2.2.1.10.1'), Counter32(284817787))])
    """
    lexicographicMode = options.get("lexicographicMode", True)
    ignoreNonIncreasingOid = options.get("ignoreNonIncreasingOid", False)
    maxRows = options.get("maxRows", 0)
    maxCalls = options.get("maxCalls", 0)

    initialVars = [
        x[0] for x in VB_PROCESSOR.make_varbinds(snmpEngine.cache, (varBind,))
    ]

    totalRows = totalCalls = 0

    varBinds: "tuple[ObjectType, ...]" = (varBind,)

    while True:
        if maxRows and totalRows < maxRows:
            maxRepetitions = min(maxRepetitions, maxRows - totalRows)

        if varBinds:
            # Create a simple tuple with the OID from the previous response and Null value
            # This approach matches walk_cmd() and works with both lookupMib=True and False
            nextVarBinds = [(varBinds[-1][0], Null(""))]

            errorIndication, errorStatus, errorIndex, varBindTable = await bulk_cmd(
                snmpEngine,
                authData,
                transportTarget,
                contextData,
                nonRepeaters,
                maxRepetitions,
                *nextVarBinds,
                **dict(lookupMib=options.get("lookupMib", True)),
            )

            if (
                ignoreNonIncreasingOid
                and errorIndication
                and isinstance(errorIndication, errind.OidNotIncreasing)
            ):
                errorIndication = None

            if errorIndication:
                yield (
                    errorIndication,
                    errorStatus,
                    errorIndex,
                    varBindTable and varBinds,
                )
                if errorIndication != errind.requestTimedOut:
                    return
            elif errorStatus:
                if errorStatus == 2:
                    # Hide SNMPv1 noSuchName error which leaks in here
                    # from SNMPv1 Agent through internal pysnmp proxy.
                    errorStatus = 0
                    errorIndex = 0
                yield (
                    errorIndication,
                    errorStatus,
                    errorIndex,
                    varBinds,
                )
                return
            else:
                stopFlag = True
                varBinds = varBindTable

                for col, varBind in enumerate(varBinds):
                    name, val = varBind
                    foundEnding = isinstance(val, Null) or isinstance(val, EndOfMibView)
                    foundBeyond = not lexicographicMode and not initialVars[
                        0
                    ].isPrefixOf(name)
                    if foundEnding or foundBeyond:
                        stopFlag = True
                        result = varBinds[:col]
                        if len(result) > 0:
                            yield (
                                errorIndication,
                                errorStatus,
                                errorIndex,
                                result,
                            )
                        return

                    if stopFlag and varBinds[col][1] is not endOfMibView:
                        stopFlag = False

                if stopFlag:
                    return

                totalRows += 1
                totalCalls += 1
        else:
            errorIndication = errorStatus = errorIndex = None
            varBinds = ()

        initialVarBinds: "tuple[ObjectType, ...]|None" = yield (
            errorIndication,
            errorStatus,
            errorIndex,
            varBinds,
        )
        if initialVarBinds:
            varBinds = initialVarBinds
            initialVars = [
                x[0] for x in VB_PROCESSOR.make_varbinds(snmpEngine.cache, varBinds)
            ]

        if maxRows and totalRows >= maxRows:
            return

        if maxCalls and totalCalls >= maxCalls:
            return


# Compatibility API
deprecated_attributes = {
    "getCmd": "get_cmd",
    "setCmd": "set_cmd",
    "nextCmd": "next_cmd",
    "bulkCmd": "bulk_cmd",
    "walkCmd": "walk_cmd",
    "bulkWalkCmd": "bulk_walk_cmd",
    "isEndOfMib": "is_end_of_mib",
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
