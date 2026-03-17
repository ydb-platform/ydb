#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import warnings

from pyasn1.type import univ
from pysnmp import debug, error
from pysnmp.entity.engine import SnmpEngine
from pysnmp.smi.instrum import MibInstrumController


class SnmpContext:
    """Create a context object."""

    context_names: dict[bytes, MibInstrumController]

    def __init__(self, snmpEngine: SnmpEngine, contextEngineId=None):
        """Create a context object."""
        (snmpEngineId,) = snmpEngine.get_mib_builder().import_symbols(  # type: ignore
            "__SNMP-FRAMEWORK-MIB", "snmpEngineID"
        )
        if contextEngineId is None:
            # Default to local snmpEngineId
            self.contextEngineId = snmpEngineId.syntax
        else:
            self.contextEngineId = snmpEngineId.syntax.clone(contextEngineId)
        debug.logger & debug.FLAG_INS and debug.logger(
            f'SnmpContext: contextEngineId "{self.contextEngineId!r}"'
        )
        self.context_names = {
            b"": snmpEngine.message_dispatcher.mib_instrum_controller  # Default name
        }

    def register_context_name(
        self, contextName, mibInstrum: "MibInstrumController | None" = None
    ):
        """Register a context name."""
        contextName = univ.OctetString(contextName).asOctets()
        if contextName in self.context_names:
            raise error.PySnmpError("Duplicate contextName %s" % contextName)
        debug.logger & debug.FLAG_INS and debug.logger(
            f"registerContextName: registered contextName {contextName!r}, mibInstrum {mibInstrum!r}"
        )
        if mibInstrum is None:
            self.context_names[contextName] = self.context_names[b""]
        else:
            self.context_names[contextName] = mibInstrum

    def unregister_context_name(self, contextName):
        """Unregister a context name."""
        contextName = univ.OctetString(contextName).asOctets()
        if contextName in self.context_names:
            debug.logger & debug.FLAG_INS and debug.logger(
                "unregisterContextName: unregistered contextName %r" % contextName
            )
            del self.context_names[contextName]

    def get_mib_instrum(self, contextName: bytes = b"") -> MibInstrumController:
        """Get MIB instrumentation for a context name."""
        contextName = univ.OctetString(contextName).asOctets()
        if contextName not in self.context_names:
            debug.logger & debug.FLAG_INS and debug.logger(
                "getMibInstrum: contextName %r not registered" % contextName
            )
            raise error.PySnmpError("Missing contextName %s" % contextName)
        else:
            debug.logger & debug.FLAG_INS and debug.logger(
                f"getMibInstrum: contextName {contextName!r}, mibInstum {self.context_names[contextName]!r}"
            )
            return self.context_names[contextName]

    # compatibility with legacy code
    # Old to new attribute mapping
    deprecated_attributes = {
        "getMibInstrum": "get_mib_instrum",
    }

    def __getattr__(self, attr: str):
        """Handle deprecated attributes."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return getattr(self, new_attr)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{attr}'"
        )
