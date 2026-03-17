#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys
import traceback
import warnings

from pysnmp import debug
from pysnmp.smi import error
from pysnmp.smi.builder import MibBuilder

__all__ = ["AbstractMibInstrumController", "MibInstrumController"]


class AbstractMibInstrumController:
    """Abstract MIB instrumentation controller."""

    def read_variables(self, *varBinds, **context):
        """Read MIB variables."""
        raise error.NoSuchInstanceError(idx=0)

    def read_next_variables(self, *varBinds, **context):
        """Read next MIB variables."""
        raise error.EndOfMibViewError(idx=0)

    def write_variables(self, *varBinds, **context):
        """Write MIB variables."""
        raise error.NoSuchObjectError(idx=0)


class MibInstrumController(AbstractMibInstrumController):
    """MIB instrumentation controller."""

    __mib_builder: MibBuilder

    fsm_read_variable = {
        # ( state, status ) -> newState
        ("start", "ok"): "readTest",
        ("readTest", "ok"): "readGet",
        ("readGet", "ok"): "stop",
        ("*", "err"): "stop",
    }
    fsm_read_next_variable = {
        # ( state, status ) -> newState
        ("start", "ok"): "readTestNext",
        ("readTestNext", "ok"): "readGetNext",
        ("readGetNext", "ok"): "stop",
        ("*", "err"): "stop",
    }
    fsm_write_variable = {
        # ( state, status ) -> newState
        ("start", "ok"): "writeTest",
        ("writeTest", "ok"): "writeCommit",
        ("writeCommit", "ok"): "writeCleanup",
        ("writeCleanup", "ok"): "readTest",
        # Do read after successful write
        ("readTest", "ok"): "readGet",
        ("readGet", "ok"): "stop",
        # Error handling
        ("writeTest", "err"): "writeCleanup",
        ("writeCommit", "err"): "writeUndo",
        ("writeUndo", "ok"): "readTest",
        # Ignore read errors (removed columns)
        ("readTest", "err"): "stop",
        ("readGet", "err"): "stop",
        ("*", "err"): "stop",
    }

    def __init__(self, mibBuilder: MibBuilder):
        """Create an MIB instrumentation controller instance."""
        self.__mib_builder = mibBuilder
        self.lastBuildId = -1
        self.lastBuildSyms = {}

    def get_mib_builder(self) -> MibBuilder:
        """Return MIB builder associated with this controller."""
        return self.__mib_builder

    # MIB indexing

    def __index_mib(self):
        # Build a tree from MIB objects found at currently loaded modules
        if self.lastBuildId == self.__mib_builder.lastBuildId:
            return

        (
            MibScalarInstance,
            MibScalar,
            MibTableColumn,
            MibTableRow,
            MibTable,
        ) = self.__mib_builder.import_symbols(  # type: ignore
            "SNMPv2-SMI",
            "MibScalarInstance",
            "MibScalar",
            "MibTableColumn",
            "MibTableRow",
            "MibTable",
        )

        (mibTree,) = self.__mib_builder.import_symbols("SNMPv2-SMI", "iso")  # type: ignore

        #
        # Management Instrumentation gets organized as follows:
        #
        # MibTree
        #   |
        #   +----MibScalar
        #   |        |
        #   |        +-----MibScalarInstance
        #   |
        #   +----MibTable
        #   |
        #   +----MibTableRow
        #          |
        #          +-------MibTableColumn
        #                        |
        #                        +------MibScalarInstance(s)
        #
        # Mind you, only Managed Objects get indexed here, various MIB defs and
        # constants can't be SNMP managed so we drop them.
        #
        scalars = {}
        instances = {}
        tables = {}
        rows = {}
        cols = {}

        # Sort by module name to give user a chance to slip-in
        # custom MIB modules (that would be sorted out first)
        mibSymbols = list(self.__mib_builder.mibSymbols.items())
        mibSymbols.sort(key=lambda x: x[0], reverse=True)

        for modName, mibMod in mibSymbols:
            for symObj in mibMod.values():
                if isinstance(symObj, MibTable):
                    tables[symObj.name] = symObj
                elif isinstance(symObj, MibTableRow):
                    rows[symObj.name] = symObj
                elif isinstance(symObj, MibTableColumn):
                    cols[symObj.name] = symObj
                elif isinstance(symObj, MibScalarInstance):
                    instances[symObj.name] = symObj
                elif isinstance(symObj, MibScalar):
                    scalars[symObj.name] = symObj

        # Detach items from each other
        for symName, parentName in self.lastBuildSyms.items():
            if parentName in scalars:
                scalars[parentName].unregisterSubtrees(symName)
            elif parentName in cols:
                cols[parentName].unregisterSubtrees(symName)
            elif parentName in rows:
                rows[parentName].unregisterSubtrees(symName)
            else:
                mibTree.unregisterSubtrees(symName)

        lastBuildSyms = {}

        # Attach Managed Objects Instances to Managed Objects
        for inst in instances.values():
            if inst.typeName in scalars:
                scalars[inst.typeName].registerSubtrees(inst)
            elif inst.typeName in cols:
                cols[inst.typeName].registerSubtrees(inst)
            else:
                raise error.SmiError(f"Orphan MIB scalar instance {inst!r} at {self!r}")
            lastBuildSyms[inst.name] = inst.typeName

        # Attach Table Columns to Table Rows
        for col in cols.values():
            rowName = col.name[:-1]  # XXX
            if rowName in rows:
                rows[rowName].registerSubtrees(col)
            else:
                raise error.SmiError(f"Orphan MIB table column {col!r} at {self!r}")
            lastBuildSyms[col.name] = rowName

        # Attach Table Rows to MIB tree
        for row in rows.values():
            mibTree.registerSubtrees(row)
            lastBuildSyms[row.name] = mibTree.name

        # Attach Tables to MIB tree
        for table in tables.values():
            mibTree.registerSubtrees(table)
            lastBuildSyms[table.name] = mibTree.name

        # Attach Scalars to MIB tree
        for scalar in scalars.values():
            mibTree.registerSubtrees(scalar)
            lastBuildSyms[scalar.name] = mibTree.name

        self.lastBuildSyms = lastBuildSyms

        self.lastBuildId = self.__mib_builder.lastBuildId

        debug.logger & debug.FLAG_INS and debug.logger("__indexMib: rebuilt")

    # MIB instrumentation

    def flip_flop_fsm(self, fsmTable, *varBinds, **context):
        """Flip-flop Finite State Machine."""
        self.__index_mib()
        debug.logger and debug.FLAG_INS and debug.logger(
            f"flipFlopFsm: input var-binds {varBinds!r}"
        )
        """Query MIB variables."""
        (mibTree,) = self.__mib_builder.import_symbols("SNMPv2-SMI", "iso")  # type: ignore
        outputVarBinds = []
        state, status = "start", "ok"
        origExc = origTraceback = None
        while True:
            k = state, status
            if k in fsmTable:
                fsmState = fsmTable[k]
            else:
                k = "*", status
                if k in fsmTable:
                    fsmState = fsmTable[k]
                else:
                    raise error.SmiError(f"Unresolved FSM state {state}, {status}")
            debug.logger & debug.FLAG_INS and debug.logger(
                f"flipFlopFsm: state {state} status {status} -> fsmState {fsmState}"
            )
            state = fsmState
            status = "ok"
            if state == "stop":
                break

            for idx, (name, val) in enumerate(varBinds):
                mgmtFun = getattr(mibTree, state, None)
                if not mgmtFun:
                    raise error.SmiError(f"Unsupported state handler {state} at {self}")

                context["idx"] = idx

                try:
                    # Convert to tuple to avoid ObjectName instantiation
                    # on subscription
                    rval = mgmtFun((tuple(name), val), **context)

                except error.SmiError:
                    exc_t, exc_v, exc_tb = sys.exc_info()
                    debug.logger & debug.FLAG_INS and debug.logger(
                        "flipFlopFsm: fun {} exception {} for {}={!r} with traceback: {}".format(
                            mgmtFun,
                            exc_t,
                            name,
                            val,
                            traceback.format_exception(exc_t, exc_v, exc_tb),
                        )
                    )
                    if origExc is None:  # Take the first exception
                        origExc, origTraceback = exc_v, exc_tb
                    status = "err"
                    break
                else:
                    debug.logger & debug.FLAG_INS and debug.logger(
                        f"flipFlopFsm: fun {mgmtFun} suceeded for {name}={val!r}"
                    )
                    if rval is not None:
                        outputVarBinds.append((rval[0], rval[1]))

        if origExc:
            try:
                raise origExc.with_traceback(origTraceback)
            finally:
                # Break cycle between locals and traceback object
                # (seems to be irrelevant on Py3 but just in case)
                del origTraceback

        return outputVarBinds

    def read_variables(self, *varBinds, **context):
        """Read MIB variables."""
        return self.flip_flop_fsm(self.fsm_read_variable, *varBinds, **context)

    def read_next_variables(self, *varBinds, **context):
        """Read next MIB variables."""
        return self.flip_flop_fsm(self.fsm_read_next_variable, *varBinds, **context)

    def write_variables(self, *varBinds, **context):
        """Write MIB variables."""
        return self.flip_flop_fsm(self.fsm_write_variable, *varBinds, **context)

    # compatibility API
    # Old to new attribute mapping
    deprecated_attributes = {
        "getMibBuilder": "get_mib_builder",
        "readVars": "read_variables",
        "readNextVars": "read_next_variables",
        "writeVars": "write_variables",
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
