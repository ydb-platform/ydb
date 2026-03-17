#
# This file is part of pysnmp software.
#
# Copyright (c) 2023-2024, LeXtudio Inc. <support@lextudio.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp.error import PySnmpError
from pysnmp.hlapi.v1arch.asyncio.auth import CommunityData
from pysnmp.hlapi.v1arch.asyncio.cmdgen import bulk_cmd, get_cmd, next_cmd, set_cmd
from pysnmp.hlapi.v1arch.asyncio.dispatch import SnmpDispatcher
from pysnmp.hlapi.v1arch.asyncio.transport import (
    Udp6TransportTarget,
    UdpTransportTarget,
)
from pysnmp.proto.errind import ErrorIndication
from pysnmp.proto.rfc1902 import Integer32
from pysnmp.smi.rfc1902 import ObjectType

__all__ = ["Slim"]


class Slim:
    r"""Creates slim SNMP wrapper object.

    With PySNMP new design, `Slim` is the new high level API to wrap up v1/v2c.

    Parameters
    ----------
    version : :py:object:`int`
        Value of 1 maps to SNMP v1, while value of 2 maps to v2c.
        Default value is 2.

    Raises
    ------
    PySNMPError
        If the value of `version` is neither 1 nor 2.

    Examples
    --------
    >>> Slim()
    Slim(2)
    >>>

    """

    __snmp_dispatcher: SnmpDispatcher
    version: int

    def __init__(self, version: int = 2):
        """Creates a slim SNMP wrapper object."""
        self.__snmp_dispatcher = SnmpDispatcher()
        if version not in (1, 2):
            raise PySnmpError(f"Not supported version {version}")
        self.version = version

    def close(self):
        """Closes the wrapper to release its resources."""
        self.__snmp_dispatcher.transport_dispatcher.close_dispatcher()

    def __enter__(self):
        """Returns the wrapper object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the wrapper to release its resources."""
        self.close()

    async def get(
        self,
        communityName: str,
        address: str,
        port: int,
        *varBinds,
        timeout: int = 1,
        retries: int = 5,
    ) -> "tuple[ErrorIndication | None, Integer32 | str | int | None, Integer32 | int | None, tuple[ObjectType, ...]]":
        r"""Creates a generator to perform SNMP GET query.

        When iterator gets advanced by :py:mod:`asyncio` main loop,
        SNMP GET request is send (:RFC:`1905#section-4.2.1`).
        The iterator yields :py:class:`asyncio.get_running_loop().create_future()` which gets done whenever
        response arrives or error occurs.

        Parameters
        ----------
        communityName : :py:obj:`str`
            Community name.

        address : :py:obj:`str`
            IP address or domain name.

        port : :py:obj:`int`
            Remote SNMP engine port number.

        varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            One or more class instances representing MIB variables to place
            into SNMP request.

        timeout : :py:obj:`int`, optional
            Timeout value in seconds (default is 1).

        retries : :py:obj:`int`, optional
            Number of retries (default is 5).

        Yields
        ------
        errorIndication : :py:class:`~pysnmp.proto.errind.ErrorIndication`
            True value indicates SNMP engine error.

        errorStatus : :py:obj:`str`
            True value indicates SNMP PDU error.

        errorIndex : :py:obj:`int`
            Non-zero value refers to `varBinds[errorIndex-1]`.

        varBinds : :py:obj:`tuple`
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
        >>> from pysnmp.hlapi.v1arch.asyncio.slim import Slim
        >>>
        >>> async def run():
        ...     with Slim() as slim:
        ...         errorIndication, errorStatus, errorIndex, varBinds = await slim.get(
        ...             'public',
        ...             'demo.pysnmp.com',
        ...             161,
        ...             ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0))
        ...         )
        ...         print(errorIndication, errorStatus, errorIndex, varBinds)
        >>>
        >>> asyncio.run(run())
        (None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('SunOS zeus.pysnmp.com 4.1.3_U1 1 sun4m'))])
        >>>
        """
        return await get_cmd(
            self.__snmp_dispatcher,
            CommunityData(communityName, mpModel=self.version - 1),
            await Udp6TransportTarget.create((address, port), timeout, retries)
            if ":" in address
            else await UdpTransportTarget.create((address, port), timeout, retries),
            *varBinds,
        )

    async def next(
        self,
        communityName: str,
        address: str,
        port: int,
        *varBinds,
        timeout: int = 1,
        retries: int = 5,
    ) -> "tuple[ErrorIndication | None, Integer32 | str | int | None, Integer32 | int | None, tuple[ObjectType, ...]]":
        r"""Creates a generator to perform SNMP GETNEXT query.

        When iterator gets advanced by :py:mod:`asyncio` main loop,
        SNMP GETNEXT request is send (:RFC:`1905#section-4.2.2`).
        The iterator yields :py:class:`~asyncio.get_running_loop().create_future()` which gets done whenever
        response arrives or error occurs.

        Parameters
        ----------
        communityName : :py:obj:`str`
            Community name.

        address : :py:obj:`str`
            IP address or domain name.

        port : :py:obj:`int`
            Remote SNMP engine port number.

        varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            One or more class instances representing MIB variables to place
            into SNMP request.

        timeout : :py:obj:`int`, optional
            Timeout value in seconds. Default is 1.

        retries : :py:obj:`int`, optional
            Number of retries. Default is 5.

        Yields
        ------
        errorIndication : :py:class:`~pysnmp.proto.errind.ErrorIndication`
            True value indicates SNMP engine error.

        errorStatus : :py:obj:`str`
            True value indicates SNMP PDU error.

        errorIndex : :py:obj:`int`
            Non-zero value refers to `varBinds[errorIndex-1]`

        varBinds : :py:obj:`tuple`
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
        >>> from pysnmp.hlapi.v1arch.asyncio.slim import Slim
        >>>
        >>> async def run():
        ...     with Slim() as slim:
        ...         errorIndication, errorStatus, errorIndex, varBinds = await slim.next(
        ...             'public',
        ...             'demo.pysnmp.com',
        ...             161,
        ...             ObjectType(ObjectIdentity('SNMPv2-MIB', 'system'))
        ...         )
        ...         print(errorIndication, errorStatus, errorIndex, varBinds)
        >>>
        >>> asyncio.run(run())
        (None, 0, 0, [[ObjectType(ObjectIdentity('1.3.6.1.2.1.1.1.0'), DisplayString('Linux i386'))]])
        >>>
        """
        return await next_cmd(
            self.__snmp_dispatcher,
            CommunityData(communityName, mpModel=self.version - 1),
            await Udp6TransportTarget.create((address, port), timeout, retries)
            if ":" in address
            else await UdpTransportTarget.create((address, port), timeout, retries),
            *varBinds,
        )

    async def bulk(
        self,
        communityName: str,
        address: str,
        port: int,
        nonRepeaters: int,
        maxRepetitions: int,
        *varBinds,
        timeout: int = 1,
        retries: int = 5,
    ) -> "tuple[ErrorIndication | None, Integer32 | str | int | None, Integer32 | int | None, tuple[ObjectType, ...]]":
        r"""Creates a generator to perform SNMP GETBULK query.

        When iterator gets advanced by :py:mod:`asyncio` main loop,
        SNMP GETBULK request is send (:RFC:`1905#section-4.2.3`).
        The iterator yields :py:class:`asyncio.get_running_loop().create_future()` which gets done whenever
        response arrives or error occurs.

        Parameters
        ----------
        communityName : :py:obj:`str`
            Community name.

        address : :py:obj:`str`
            IP address or domain name.

        port : :py:obj:`int`
            Remote SNMP engine port number.

        nonRepeaters : :py:obj:`int`
            One MIB variable is requested in response for the first
            `nonRepeaters` MIB variables in request.

        maxRepetitions : :py:obj:`int`
            `maxRepetitions` MIB variables are requested in response for each
            of the remaining MIB variables in the request (e.g. excluding
            `nonRepeaters`). Remote SNMP engine may choose lesser value than
            requested.

        \*varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            One or more class instances representing MIB variables to place
            into SNMP request.

        timeout : :py:obj:`int`, optional
            Timeout value in seconds. Default is 1.

        retries : :py:obj:`int`, optional
            Number of retries. Default is 5.

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
            instances returned by the SNMP GetBulk response in wire order.
            The order is: non-repeaters first (at most one each), followed
            by up to ``maxRepetitions`` groups of the repeaters in request
            order. The maximum length is
            ``nonRepeaters + maxRepetitions * (len(varBinds) - nonRepeaters)``,
            but the agent can return fewer items (e.g., due to
            :py:obj:`~pysnmp.proto.rfc1905.endOfMibView` or truncation).

            See :rfc:`3416#section-4.2.3` for protocol details.

        Raises
        ------
        PySnmpError
            Or its derivative indicating that an error occurred while
            performing SNMP operation.

        Examples
        --------
        >>> import asyncio
        >>> from pysnmp.hlapi.v1arch.asyncio.slim import Slim
        >>>
        >>> async def run():
        ...     with Slim() as slim:
        ...         errorIndication, errorStatus, errorIndex, varBinds = await slim.bulk(
        ...             'public',
        ...             'demo.pysnmp.com',
        ...             161,
        ...             0,
        ...             2,
        ...             ObjectType(ObjectIdentity('SNMPv2-MIB', 'system'))
        ...         )
        ...         print(errorIndication, errorStatus, errorIndex, varBinds)
        >>>
        >>> asyncio.run(run())
        (None, 0, 0, [[ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('SunOS zeus.pysnmp.com 4.1.3_U1 1 sun4m'))], [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.2.0')), ObjectIdentifier('1.3.6.1.4.1.424242.1.1'))]])
        >>>
        """
        version = self.version - 1
        if version == 0:
            raise PySnmpError("Cannot send V2 PDU on V1 session")
        return await bulk_cmd(
            self.__snmp_dispatcher,
            CommunityData(communityName, mpModel=version),
            await Udp6TransportTarget.create((address, port), timeout, retries)
            if ":" in address
            else await UdpTransportTarget.create((address, port), timeout, retries),
            nonRepeaters,
            maxRepetitions,
            *varBinds,
        )

    async def set(
        self,
        communityName: str,
        address: str,
        port: int,
        *varBinds,
        timeout: int = 1,
        retries: int = 5,
    ) -> "tuple[ErrorIndication | None, Integer32 | str | int | None, Integer32 | int | None, tuple[ObjectType, ...]]":
        r"""Creates a generator to perform SNMP SET query.

        When iterator gets advanced by :py:mod:`asyncio` main loop,
        SNMP SET request is send (:RFC:`1905#section-4.2.5`).
        The iterator yields :py:class:`asyncio.get_running_loop().create_future()` which gets done whenever
        response arrives or error occurs.

        Parameters
        ----------
        communityName : :py:obj:`str`
            Community name.

        address : :py:obj:`str`
            IP address or domain name.

        port : :py:obj:`int`
            Remote SNMP engine port number.

        varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            One or more class instances representing MIB variables to place
            into SNMP request.

        timeout : :py:obj:`int`, optional
            Timeout value in seconds. Default is 1.

        retries : :py:obj:`int`, optional
            Number of retries. Default is 5.

        Yields
        ------
        errorIndication : :py:class:`~pysnmp.proto.errind.ErrorIndication`
            True value indicates SNMP engine error.
        errorStatus : :py:obj:`str`
            True value indicates SNMP PDU error.
        errorIndex : :py:obj:`int`
            Non-zero value refers to `varBinds[errorIndex-1]`
        varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
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
        >>> from pysnmp.hlapi.v1arch.asyncio.slim import Slim
        >>>
        >>> async def run():
        ...     with Slim() as slim:
        ...         errorIndication, errorStatus, errorIndex, varBinds = await slim.set(
        ...             'public',
        ...             'demo.pysnmp.com',
        ...             161,
        ...             ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0), 'Linux i386')
        ...         )
        ...         print(errorIndication, errorStatus, errorIndex, varBinds)
        >>>
        >>> asyncio.run(run())
        (None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.1.0')), DisplayString('Linux i386'))])
        >>>
        """
        return await set_cmd(
            self.__snmp_dispatcher,
            CommunityData(communityName, mpModel=self.version - 1),
            await Udp6TransportTarget.create((address, port), timeout, retries)
            if ":" in address
            else await UdpTransportTarget.create((address, port), timeout, retries),
            *varBinds,
        )
