#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import TYPE_CHECKING


from pysnmp import error

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine


class MetaObserver:
    r"""This is a simple facility for exposing internal SNMP Engine working details to pysnmp applications.

    These details are basically local scope variables at a fixed point of
    execution. Two modes of operations are offered:

    1. Consumer: app can request an execution point context by execution point ID.
    2. Provider: app can register its callback function (and context) to be invoked
       once execution reaches specified point. All local scope variables
       will be passed to the callback as in #1.

    It's important to realize that execution context is only guaranteed
    to exist to functions that are at the same or deeper level of invocation
    relative to execution point specified.
    """

    def __init__(self):
        """Create a meta observer instance."""
        self.__observers = {}
        self.__contexts = {}
        self.__execpoints = {}

    def register_observer(self, cbFun, *execpoints, **kwargs):
        """Register a callback function to be invoked at specified execution points.

        Args:
            cbFun (callable): Callback function to be invoked.
            execpoints (str): Execution point ID(s) to trigger the callback.
            cbCtx (object): Arbitrary object to be passed to the callback.

        Raises:
            PySnmpError: If observer is already registered.
        """
        if cbFun in self.__contexts:
            raise error.PySnmpError("duplicate observer %s" % cbFun)
        else:
            self.__contexts[cbFun] = kwargs.get("cbCtx")
        for execpoint in execpoints:
            if execpoint not in self.__observers:
                self.__observers[execpoint] = []
            self.__observers[execpoint].append(cbFun)

    def unregister_observer(self, cbFun=None):
        """Unregister a callback function.

        Args:
            cbFun (callable): Callback function to be unregistered. If not specified,
                all observers will be unregistered.
        """
        if cbFun is None:
            self.__observers.clear()
            self.__contexts.clear()
        else:
            for execpoint in dict(self.__observers):
                if cbFun in self.__observers[execpoint]:
                    self.__observers[execpoint].remove(cbFun)
                if not self.__observers[execpoint]:
                    del self.__observers[execpoint]

    def store_execution_context(self, snmpEngine: "SnmpEngine", execpoint, variables):
        """Store execution context at specified execution point.

        Args:
            execpoint (str): Execution point ID.
            variables (dict): Local scope variables to store.
        """
        self.__execpoints[execpoint] = variables
        if execpoint in self.__observers:
            for cbFun in self.__observers[execpoint]:
                cbFun(snmpEngine, execpoint, variables, self.__contexts[cbFun])

    def clear_execution_context(self, snmpEngine: "SnmpEngine", *execpoints):
        """Clear execution context at specified execution points.

        Args:
            execpoints (str): Execution point ID(s) to clear.
        """
        if execpoints:
            for execpoint in execpoints:
                del self.__execpoints[execpoint]
        else:
            self.__execpoints.clear()

    def get_execution_context(self, execpoint):
        """Retrieve execution context at specified execution point.

        Args:
            execpoint (str): Execution point ID.

        Returns:
            dict: Local scope variables at the specified execution point.
        """
        return self.__execpoints[execpoint]
