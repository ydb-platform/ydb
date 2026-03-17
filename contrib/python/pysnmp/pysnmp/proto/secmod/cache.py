#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import Any, Dict


from pysnmp import nextid
from pysnmp.proto import error


class Cache:
    """SNMP securityData cache."""

    __state_reference = nextid.Integer(0xFFFFFF)
    __cache_entries: Dict[int, Any]

    def __init__(self):
        """Create a cache object."""
        self.__cache_entries = {}

    def push(self, **securityData):
        """Push securityData into cache."""
        stateReference = self.__state_reference()
        self.__cache_entries[stateReference] = securityData
        return stateReference

    def pop(self, stateReference):
        """Pop securityData from cache."""
        if stateReference in self.__cache_entries:
            securityData = self.__cache_entries[stateReference]
        else:
            raise error.ProtocolError(
                f"Cache miss for stateReference={stateReference} at {self}"
            )
        del self.__cache_entries[stateReference]
        return securityData

    def is_empty(self):
        """Return True if cache is empty."""
        return not bool(self.__cache_entries)
