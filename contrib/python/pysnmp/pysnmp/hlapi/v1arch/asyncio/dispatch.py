#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp.carrier.asyncio.dispatch import AsyncioDispatcher
from pysnmp.hlapi.v1arch.dispatch import AbstractSnmpDispatcher

__all__ = ["SnmpDispatcher"]


class SnmpDispatcher(AbstractSnmpDispatcher):
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

    The `SnmpDispatcher` class supports context manager protocol,
    so it can be used with the `with` statement for automatic
    resource management:

    .. code-block:: python

       with SnmpDispatcher() as dispatcher:
           # use dispatcher
           pass  # dispatcher.close() is called automatically
    """

    PROTO_DISPATCHER = AsyncioDispatcher
