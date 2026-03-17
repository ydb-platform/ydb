#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import warnings

from pysnmp import debug
from pysnmp.smi import error
from pysnmp.smi.builder import MibBuilder
from pysnmp.smi.indices import OidOrderedDict, OrderedDict

__all__ = ["MibViewController"]

classTypes = (type,)  # noqa: N816
instanceTypes = (object,)  # noqa: N816


class MibViewController:
    """Create a MIB view controller."""

    def __init__(self, mibBuilder: MibBuilder):
        """Create a MIB view controller."""
        self.mibBuilder = mibBuilder
        self.lastBuildId = -1
        self.__mibSymbolsIdx = OrderedDict()

    # Indexing part

    def index_mib(self):
        """Re-index MIB view."""
        if self.lastBuildId == self.mibBuilder.lastBuildId:
            return

        debug.logger & debug.FLAG_MIB and debug.logger("indexMib: re-indexing MIB view")

        (MibScalarInstance,) = self.mibBuilder.import_symbols(  # type: ignore
            "SNMPv2-SMI", "MibScalarInstance"
        )

        #
        # Create indices
        #

        # Module name -> module-scope indices
        self.__mibSymbolsIdx.clear()

        # Oid <-> label indices

        # This is potentially ambiguous mapping. Sort modules in
        # ascending age for resolution
        def __sorting_function(x, b: MibBuilder = self.mibBuilder):
            if b.module_id in b.mibSymbols[x]:
                m = b.mibSymbols[x][b.module_id]
                r = m.getRevisions()
                if r:
                    return r[0][0]

            return "1970-01-01 00:00"

        modNames = list(self.mibBuilder.mibSymbols.keys())
        modNames.sort(key=__sorting_function)

        # Index modules names
        for modName in [""] + modNames:
            # Modules index
            self.__mibSymbolsIdx[modName] = mibMod = {
                "oidToLabelIdx": OidOrderedDict(),
                "labelToOidIdx": {},
                "varToNameIdx": {},
                "typeToModIdx": OrderedDict(),
                "oidToModIdx": {},
            }

            if not modName:
                globMibMod = mibMod
                continue

            # Types & MIB vars indices
            for n, v in self.mibBuilder.mibSymbols[modName].items():
                if n == self.mibBuilder.module_id:  # do not index this
                    continue  # special symbol
                if isinstance(v, classTypes):
                    if n in mibMod["typeToModIdx"]:
                        raise error.SmiError(
                            "Duplicate SMI type {}::{}, has {}".format(
                                modName, n, mibMod["typeToModIdx"][n]
                            )
                        )
                    globMibMod["typeToModIdx"][n] = modName
                    mibMod["typeToModIdx"][n] = modName
                elif isinstance(v, instanceTypes):
                    if isinstance(v, MibScalarInstance):
                        continue
                    if n in mibMod["varToNameIdx"]:
                        raise error.SmiError(
                            "Duplicate MIB variable {}::{} has {}".format(
                                modName, n, mibMod["varToNameIdx"][n]
                            )
                        )
                    globMibMod["varToNameIdx"][n] = v.name
                    mibMod["varToNameIdx"][n] = v.name
                    # Potentionally ambiguous mapping ahead
                    globMibMod["oidToModIdx"][v.name] = modName
                    mibMod["oidToModIdx"][v.name] = modName
                    globMibMod["oidToLabelIdx"][v.name] = (n,)
                    mibMod["oidToLabelIdx"][v.name] = (n,)
                else:
                    raise error.SmiError(f"Unexpected object {modName}::{n}")

        # Build oid->long-label index
        oidToLabelIdx = self.__mibSymbolsIdx[""]["oidToLabelIdx"]
        labelToOidIdx = self.__mibSymbolsIdx[""]["labelToOidIdx"]
        prevOid = ()
        baseLabel = ()
        for key in oidToLabelIdx.keys():
            keydiff = len(key) - len(prevOid)
            if keydiff > 0:
                if prevOid:
                    if keydiff == 1:
                        baseLabel = oidToLabelIdx[prevOid]
                    else:
                        baseLabel += key[-keydiff:-1]
                else:
                    baseLabel = ()
            elif keydiff < 0:
                baseLabel = ()
                keyLen = len(key)
                i = keyLen - 1
                while i:
                    k = key[:i]
                    if k in oidToLabelIdx:
                        baseLabel = oidToLabelIdx[k]
                        if i != keyLen - 1:
                            baseLabel += key[i:-1]
                        break
                    i -= 1
            # Build oid->long-label index
            oidToLabelIdx[key] = baseLabel + oidToLabelIdx[key]
            # Build label->oid index
            labelToOidIdx[oidToLabelIdx[key]] = key
            prevOid = key

        # Build module-scope oid->long-label index
        for mibMod in self.__mibSymbolsIdx.values():
            for oid in mibMod["oidToLabelIdx"].keys():
                mibMod["oidToLabelIdx"][oid] = oidToLabelIdx[oid]
                mibMod["labelToOidIdx"][oidToLabelIdx[oid]] = oid

        self.lastBuildId = self.mibBuilder.lastBuildId

    # Module management

    def get_ordered_module_name(self, index):
        """Return module name by index."""
        self.index_mib()
        modNames = self.__mibSymbolsIdx.keys()
        if modNames:
            return modNames[index]
        raise error.SmiError("No modules loaded at %s" % self)

    def get_first_module_name(self):
        """Return first module name."""
        return self.get_ordered_module_name(0)

    def get_last_module_name(self):
        """Return last module name."""
        return self.get_ordered_module_name(-1)

    def get_next_module_name(self, modName):
        """Return next module name."""
        self.index_mib()
        try:
            return self.__mibSymbolsIdx.next_key(modName)
        except KeyError:
            raise error.SmiError(f"No module next to {modName} at {self}")

    # MIB tree node management

    def __get_oid_label(self, nodeName, oidToLabelIdx, labelToOidIdx):
        """getOidLabel(nodeName) -> (oid, label, suffix)."""
        if not nodeName:
            return nodeName, nodeName, ()
        if nodeName in labelToOidIdx:
            return labelToOidIdx[nodeName], nodeName, ()
        if nodeName in oidToLabelIdx:
            return nodeName, oidToLabelIdx[nodeName], ()
        if len(nodeName) < 2:
            return nodeName, nodeName, ()
        oid, label, suffix = self.__get_oid_label(
            nodeName[:-1], oidToLabelIdx, labelToOidIdx
        )
        suffix = suffix + nodeName[-1:]
        resLabel = label + tuple(str(x) for x in suffix)
        if resLabel in labelToOidIdx:
            return labelToOidIdx[resLabel], resLabel, ()
        resOid = oid + suffix
        if resOid in oidToLabelIdx:
            return resOid, oidToLabelIdx[resOid], ()
        return oid, label, suffix

    def get_node_name_by_oid(self, nodeName, modName=""):
        """Return node name by OID."""
        self.index_mib()
        if modName in self.__mibSymbolsIdx:
            mibMod = self.__mibSymbolsIdx[modName]
        else:
            raise error.SmiError(f"No module {modName} at {self}")
        oid, label, suffix = self.__get_oid_label(
            nodeName, mibMod["oidToLabelIdx"], mibMod["labelToOidIdx"]
        )
        if oid == label:
            raise error.NoSuchObjectError(
                str=f"Can't resolve node name {modName}::{nodeName} at {self}"
            )
        debug.logger & debug.FLAG_MIB and debug.logger(
            f"getNodeNameByOid: resolved {modName}:{nodeName} -> {label}.{suffix}"
        )
        return oid, label, suffix

    def get_node_name_by_desc(self, nodeName, modName=""):
        """Return node name by MIB symbol."""
        self.index_mib()
        if modName in self.__mibSymbolsIdx:
            mibMod = self.__mibSymbolsIdx[modName]
        else:
            raise error.SmiError(f"No module {modName} at {self}")
        if nodeName in mibMod["varToNameIdx"]:
            oid = mibMod["varToNameIdx"][nodeName]
        else:
            raise error.NoSuchObjectError(
                str=f"No such symbol {modName}::{nodeName} at {self}"
            )
        debug.logger & debug.FLAG_MIB and debug.logger(
            f"getNodeNameByDesc: resolved {modName}:{nodeName} -> {oid}"
        )
        return self.get_node_name_by_oid(oid, modName)

    def get_node_name(self, nodeName, modName=""):
        """Return node name by OID or MIB symbol."""
        # nodeName may be either an absolute OID/label or a
        # ( MIB-symbol, su, ff, ix)
        try:
            # First try nodeName as an OID/label
            return self.get_node_name_by_oid(nodeName, modName)
        except error.NoSuchObjectError:
            # ...on failure, try as MIB symbol
            oid, label, suffix = self.get_node_name_by_desc(nodeName[0], modName)
            # ...with trailing suffix
            return self.get_node_name_by_oid(oid + suffix + nodeName[1:], modName)

    def get_ordered_node_name(self, index, modName=""):
        """Return node name by index."""
        self.index_mib()
        if modName in self.__mibSymbolsIdx:
            mibMod = self.__mibSymbolsIdx[modName]
        else:
            raise error.SmiError(f"No module {modName} at {self}")
        if not mibMod["oidToLabelIdx"]:
            raise error.NoSuchObjectError(
                str=f"No variables at MIB module {modName} at {self}"
            )
        try:
            oid, label = mibMod["oidToLabelIdx"].items()[index]
        except KeyError:
            raise error.NoSuchObjectError(
                str=f"No symbol at position {index} in MIB module {modName} at {self}"
            )
        return oid, label, ()

    def get_first_node_name(self, modName=""):
        """Return first node name."""
        return self.get_ordered_node_name(0, modName)

    def get_last_node_name(self, modName=""):
        """Return last node name."""
        return self.get_ordered_node_name(-1, modName)

    def get_next_node_name(self, nodeName, modName=""):
        """Return next node name."""
        oid, label, suffix = self.get_node_name(nodeName, modName)
        try:
            return self.get_node_name(
                self.__mibSymbolsIdx[modName]["oidToLabelIdx"].next_key(oid) + suffix,
                modName,
            )
        except KeyError:
            raise error.NoSuchObjectError(
                str=f"No name next to {modName}::{nodeName} at {self}"
            )

    def get_parent_node_name(self, nodeName, modName=""):
        """Return parent node name."""
        oid, label, suffix = self.get_node_name(nodeName, modName)
        if len(oid) < 2:
            raise error.NoSuchObjectError(
                str=f"No parent name for {modName}::{nodeName} at {self}"
            )
        return oid[:-1], label[:-1], oid[-1:] + suffix

    def get_node_location(self, nodeName, modName=""):
        """Return module name, node name, and suffix."""
        oid, label, suffix = self.get_node_name(nodeName, modName)
        return self.__mibSymbolsIdx[""]["oidToModIdx"][oid], label[-1], suffix

    # MIB type management

    def get_type_name(self, typeName, modName=""):
        """Return type name by MIB symbol."""
        self.index_mib()
        if modName in self.__mibSymbolsIdx:
            mibMod = self.__mibSymbolsIdx[modName]
        else:
            raise error.SmiError(f"No module {modName} at {self}")
        if typeName in mibMod["typeToModIdx"]:
            m = mibMod["typeToModIdx"][typeName]
        else:
            raise error.NoSuchObjectError(
                str=f"No such type {modName}::{typeName} at {self}"
            )
        return m, typeName

    def get_ordered_type_name(self, index, modName=""):
        """Return type name by index."""
        self.index_mib()
        if modName in self.__mibSymbolsIdx:
            mibMod = self.__mibSymbolsIdx[modName]
        else:
            raise error.SmiError(f"No module {modName} at {self}")
        if not mibMod["typeToModIdx"]:
            raise error.NoSuchObjectError(
                str=f"No types at MIB module {modName} at {self}"
            )
        t = mibMod["typeToModIdx"].keys()[index]
        return mibMod["typeToModIdx"][t], t

    def get_first_type_name(self, modName=""):
        """Get first type name."""
        return self.get_ordered_type_name(0, modName)

    def get_last_type_name(self, modName=""):
        """Get last type name."""
        return self.get_ordered_type_name(-1, modName)

    def get_next_type(self, typeName, modName=""):
        """Get next type name."""
        m, t = self.get_type_name(typeName, modName)
        try:
            return self.__mibSymbolsIdx[m]["typeToModIdx"].next_key(t)
        except KeyError:
            raise error.NoSuchObjectError(
                str=f"No type next to {modName}::{typeName} at {self}"
            )

    # Compatibility API
    deprecated_attributes = {
        "getOrderedModuleName": "get_ordered_module_name",
        "getFirstModuleName": "get_first_module_name",
        "getLastModuleName": "get_last_module_name",
        "getNextModuleName": "get_next_module_name",
        "getNodeNameByOid": "get_node_name_by_oid",
        "getNodeNameByDesc": "get_node_name_by_desc",
        "getNodeName": "get_node_name",
        "getOrderedNodeName": "get_ordered_node_name",
        "getFirstNodeName": "get_first_node_name",
        "getLastNodeName": "get_last_node_name",
        "getNextNodeName": "get_next_node_name",
        "getParentNodeName": "get_parent_node_name",
        "getNodeLocation": "get_node_location",
        "getTypeName": "get_type_name",
        "getOrderedTypeName": "get_ordered_type_name",
        "getFirstTypeName": "get_first_type_name",
        "getLastTypeName": "get_last_type_name",
        "getNextType": "get_next_type",
    }

    def __getattr__(self, attr):
        """Redirect some attrs access to the OID object to behave alike."""
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
