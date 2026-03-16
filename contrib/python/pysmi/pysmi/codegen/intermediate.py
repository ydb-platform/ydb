#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2019, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import re
import sys
from collections import OrderedDict
from time import strftime, strptime

from pysmi import config, debug, error
from pysmi.codegen.base import AbstractCodeGen
from pysmi.mibinfo import MibInfo


class IntermediateCodeGen(AbstractCodeGen):
    """Turns MIB AST into an intermediate representation.

    This intermediate representation is based on built-in Python types
    and structures that could easily be used from within the template
    engines.
    """

    constImports = {
        "SNMPv2-SMI": (
            "iso",
            "NOTIFICATION-TYPE",  # bug in some MIBs (e.g. A3COM-HUAWEI-DHCPSNOOP-MIB)
            "MODULE-IDENTITY",
            "OBJECT-TYPE",
            "OBJECT-IDENTITY",
        ),
        "SNMPv2-TC": (
            "DisplayString",
            "PhysAddress",
            "TEXTUAL-CONVENTION",
        ),  # XXX
        "SNMPv2-CONF": (
            "MODULE-COMPLIANCE",
            "NOTIFICATION-GROUP",
        ),  # XXX
    }

    # never compile these, they either:
    # - define MACROs (implementation supplies them)
    # - or carry conflicting OIDs (so that all IMPORT's of them will be rewritten)
    # - or have manual fixes
    # - or import base ASN.1 types from implementation-specific MIBs
    fakeMibs = (
        "ASN1",
        "ASN1-ENUMERATION",
        "ASN1-REFINEMENT",
    ) + AbstractCodeGen.baseMibs

    baseTypes = ["Integer", "Integer32", "Bits", "ObjectIdentifier", "OctetString"]

    SMI_TYPES = {
        "NetworkAddress": "IpAddress",  # RFC1065-SMI, RFC1155-SMI -> SNMPv2-SMI
        "nullSpecific": "zeroDotZero",  # RFC1158-MIB -> SNMPv2-SMI
        "ipRoutingTable": "ipRouteTable",  # RFC1158-MIB -> RFC1213-MIB
        "snmpEnableAuthTraps": "snmpEnableAuthenTraps",  # RFC1158-MIB -> SNMPv2-MIB
    }

    def __init__(self):
        self._rows = set()
        self._seenSyms = set()
        self._importMap = {}
        self._out = {}  # k, v = name, generated code
        self._moduleIdentityOid = None
        self._moduleRevision = None
        self._enterpriseOid = None
        self._oids = set()
        self._complianceOids = []
        self.moduleName = ["DUMMY"]
        self.genRules = {"text": True}
        self.symbolTable = {}

    def prep_data(self, pdata):
        data = []
        for el in pdata:
            if not isinstance(el, tuple):
                data.append(el)
            elif len(el) == 1:
                data.append(el[0])
            else:
                data.append(self.handlersTable[el[0]](self, self.prep_data(el[1:])))
        return data

    def gen_imports(self, imports):
        # convertion to SNMPv2
        toDel = []
        for module in list(imports):
            if module in self.convertImportv2:
                for symbol in imports[module]:
                    if symbol in self.convertImportv2[module]:
                        toDel.append((module, symbol))

                        for newImport in self.convertImportv2[module][symbol]:
                            newModule, newSymbol = newImport

                            if newModule in imports:
                                imports[newModule].append(newSymbol)
                            else:
                                imports[newModule] = [newSymbol]

        # removing converted symbols
        for d in toDel:
            imports[d[0]].remove(d[1])

        # merging mib and constant imports
        for module in self.constImports:
            if module in imports:
                imports[module] += self.constImports[module]
            else:
                imports[module] = self.constImports[module]

        outDict = OrderedDict()
        outDict["class"] = "imports"
        for module in sorted(imports):
            symbols = []
            for symbol in sorted(set(imports[module])):
                symbols.append(symbol)

            if symbols:
                self._seenSyms.update([self.trans_opers(s) for s in symbols])
                self._importMap.update([(self.trans_opers(s), module) for s in symbols])
                if module not in outDict:
                    outDict[module] = []

                outDict[module].extend(symbols)

        return OrderedDict(imports=outDict), tuple(sorted(imports))

    def add_to_exports(self, symbol, moduleIdentity=0):
        self._seenSyms.add(symbol)

    # noinspection PyUnusedLocal
    def reg_sym(
        self,
        symbol,
        outDict,
        parentOid=None,
        moduleIdentity=False,
        moduleCompliance=False,
    ):
        if symbol in self._seenSyms and symbol not in self._importMap:
            raise error.PySmiSemanticError(f"Duplicate symbol found: {symbol}")

        self.add_to_exports(symbol, moduleIdentity)
        self._out[symbol] = outDict

        if "oid" in outDict:
            self._oids.add(outDict["oid"])

            if not self._enterpriseOid and outDict["oid"].startswith("1.3.6.1.4.1."):
                self._enterpriseOid = ".".join(outDict["oid"].split(".")[:7])

            if moduleIdentity:
                if self._moduleIdentityOid:
                    if config.STRICT_MODE:
                        raise error.PySmiSemanticError("Duplicate module identity")
                    else:
                        pass
                else:
                    self._moduleIdentityOid = outDict["oid"]

            if moduleCompliance:
                self._complianceOids.append(outDict["oid"])

    def gen_numeric_oid(self, oid):
        numericOid = ()

        for part in oid:
            if isinstance(part, tuple):
                parent, module = part
                if parent == "iso":
                    numericOid += (1,)
                    continue

                if module not in self.symbolTable:
                    # TODO: do getname for possible future borrowed mibs
                    raise error.PySmiSemanticError(
                        f'no module "{module}" in symbolTable'
                    )

                if parent not in self.symbolTable[module]:
                    raise error.PySmiSemanticError(
                        f'no symbol "{parent}" in module "{module}"'
                    )
                numericOid += self.gen_numeric_oid(
                    self.symbolTable[module][parent]["oid"]
                )

            else:
                numericOid += (part,)

        return numericOid

    def get_base_type(self, symName, module):
        if module not in self.symbolTable:
            raise error.PySmiSemanticError(f'no module "{module}" in symbolTable')

        if symName not in self.symbolTable[module]:
            raise error.PySmiSemanticError(
                f'no symbol "{symName}" in module "{module}"'
            )

        symType, symSubtype = self.symbolTable[module][symName].get(
            "syntax", (("", ""), "")
        )
        if not symType[0]:
            raise error.PySmiSemanticError(f'unknown type for symbol "{symName}"')

        if symType[0] in self.baseTypes:
            return symType, symSubtype

        else:
            baseSymType, baseSymSubtype = self.get_base_type(*symType)

            if isinstance(baseSymSubtype, dict):
                # An enumeration (INTEGER or BITS). Combine the enumeration
                # lists when applicable. That is a bit more permissive than
                # strictly needed, as syntax refinement may only remove entries
                # from the base enumeration (RFC 2578 Sec. 9 point (2)).
                if isinstance(symSubtype, dict):
                    baseSymSubtype.update(symSubtype)
                symSubtype = baseSymSubtype

            elif isinstance(baseSymSubtype, list):
                # A value range or size constraint. Note that each list is an
                # intersection of unions of ranges. Taking the intersection
                # instead of the most-top level union of ranges is a bit more
                # restrictive than strictly needed, as range syntax refinement
                # may only remove allowed values from the base type (RFC 2578
                # Sec. 9. points (1) and (3)), but it matches what pyasn1 does.
                if isinstance(symSubtype, list):
                    symSubtype += baseSymSubtype
                else:
                    symSubtype = baseSymSubtype

            return baseSymType, symSubtype

    def is_type_derived_from_tc(self, symName, module):
        """Is the given type derived from a Textual-Convention declaration?

        Given that deriving simple types from textual conventions is currently
        not supported altogether, this method tests only the immediate parent.

        Any problems are ignored: if the given type is not definitely derived
        from a textual convention, this method returns False.
        """
        if module not in self.symbolTable or symName not in self.symbolTable[module]:
            return False

        return self.symbolTable[module][symName]["isTC"]

    # Clause generation functions

    # noinspection PyUnusedLocal
    def gen_agent_capabilities(self, data):
        name, productRelease, status, description, reference, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid

        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr
        outDict["class"] = "agentcapabilities"

        if productRelease:
            outDict["productrelease"] = productRelease

        if status:
            outDict["status"] = status

        if self.genRules["text"] and description:
            outDict["description"] = description

        if self.genRules["text"] and reference:
            outDict["reference"] = reference

        self.reg_sym(pysmiName, outDict, parentOid)

        return outDict

    # noinspection PyUnusedLocal
    def gen_module_identity(self, data):
        name, lastUpdated, organization, contactInfo, description, revisions, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid

        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr
        outDict["class"] = "moduleidentity"

        if revisions:
            outDict["revisions"] = revisions

            self._moduleRevision = revisions[0]["revision"]

        if self.genRules["text"]:
            if lastUpdated:
                outDict["lastupdated"] = lastUpdated
            if organization:
                outDict["organization"] = organization
            if contactInfo:
                outDict["contactinfo"] = contactInfo
            if description:
                outDict["description"] = description

        self.reg_sym(pysmiName, outDict, parentOid, moduleIdentity=True)

        return outDict

    # noinspection PyUnusedLocal
    def gen_module_compliance(self, data):
        name, status, description, reference, compliances, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid

        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr
        outDict["class"] = "modulecompliance"

        if compliances:
            outDict["modulecompliance"] = compliances

        if status:
            outDict["status"] = status

        if self.genRules["text"] and description:
            outDict["description"] = description

        if self.genRules["text"] and reference:
            outDict["reference"] = reference

        self.reg_sym(pysmiName, outDict, parentOid, moduleCompliance=True)

        return outDict

    # noinspection PyUnusedLocal
    def gen_notification_group(self, data):
        name, objects, status, description, reference, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid
        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr
        outDict["class"] = "notificationgroup"

        if objects:
            outDict["objects"] = [
                {
                    "module": self._importMap.get(
                        self.trans_opers(obj), self.moduleName[0]
                    ),
                    "object": obj,
                }
                for obj in objects
            ]

        if status:
            outDict["status"] = status

        if self.genRules["text"] and description:
            outDict["description"] = description

        if self.genRules["text"] and reference:
            outDict["reference"] = reference

        self.reg_sym(pysmiName, outDict, parentOid)

        return outDict

    # noinspection PyUnusedLocal
    def gen_notification_type(self, data):
        name, objects, status, description, reference, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid
        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr
        outDict["class"] = "notificationtype"

        if objects:
            outDict["objects"] = [
                {
                    "module": self._importMap.get(
                        self.trans_opers(obj), self.moduleName[0]
                    ),
                    "object": obj,
                }
                for obj in objects
            ]

        if status:
            outDict["status"] = status

        if self.genRules["text"] and description:
            outDict["description"] = description

        if self.genRules["text"] and reference:
            outDict["reference"] = reference

        self.reg_sym(pysmiName, outDict, parentOid)

        return outDict

    # noinspection PyUnusedLocal
    def gen_object_group(self, data):
        name, objects, status, description, reference, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid
        outDict = OrderedDict({"name": name, "oid": oidStr, "class": "objectgroup"})

        if objects:
            outDict["objects"] = [
                {
                    "module": self._importMap.get(
                        self.trans_opers(obj), self.moduleName[0]
                    ),
                    "object": obj,
                }
                for obj in objects
            ]

        if status:
            outDict["status"] = status

        if self.genRules["text"] and description:
            outDict["description"] = description

        if self.genRules["text"] and reference:
            outDict["reference"] = reference

        self.reg_sym(pysmiName, outDict, parentOid)

        return outDict

    # noinspection PyUnusedLocal
    def gen_object_identity(self, data):
        name, status, description, reference, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid

        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr
        outDict["class"] = "objectidentity"

        if status:
            outDict["status"] = status

        if self.genRules["text"] and description:
            outDict["description"] = description

        if self.genRules["text"] and reference:
            outDict["reference"] = reference

        self.reg_sym(pysmiName, outDict, parentOid)

        return outDict

    # noinspection PyUnusedLocal
    def gen_object_type(self, data):
        (
            name,
            syntax,
            units,
            maxaccess,
            status,
            description,
            reference,
            augmention,
            index,
            defval,
            oid,
        ) = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid
        indexStr, fakeSyms, fakeSymDicts = index or ("", [], [])

        defval = self.process_defval(defval, objname=pysmiName)

        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr

        if syntax[0]:
            nodetype = syntax[0] == "Bits" and "scalar" or syntax[0]  # Bits hack
            # If this object type is used as a column, but it also has a
            # "SEQUENCE OF" syntax, then it is really a table and not a column.
            #
            # Note that _symtable_cols contains original (non-Pythonized)
            # symbol names! Thus, check with 'name' rather than 'pysmiName'.
            isColumn = (
                name in self.symbolTable[self.moduleName[0]]["_symtable_cols"]
                and syntax[1]
            )
            nodetype = isColumn and "column" or nodetype
            outDict["nodetype"] = nodetype

        outDict["class"] = "objecttype"

        if syntax[1]:
            outDict["syntax"] = syntax[1]
        if defval:
            outDict["default"] = defval
        if units:
            outDict["units"] = units
        if maxaccess:
            outDict["maxaccess"] = maxaccess
        if indexStr:
            outDict["indices"] = indexStr
        if self.genRules["text"] and reference:
            outDict["reference"] = reference
        if augmention:
            augmention = self.trans_opers(augmention)
            outDict["augmention"] = OrderedDict()
            outDict["augmention"]["name"] = name
            outDict["augmention"]["module"] = self.moduleName[0]
            outDict["augmention"]["object"] = augmention
        if status:
            outDict["status"] = status

        if self.genRules["text"] and description:
            outDict["description"] = description

        self.reg_sym(pysmiName, outDict, parentOid)

        for fakeSym, fakeSymDict in zip(fakeSyms, fakeSymDicts):
            fakeSymDict["oid"] = f"{oidStr}.{fakeSymDict['oid']}"
            self.reg_sym(fakeSym, fakeSymDict, pysmiName)

        return outDict

    # noinspection PyUnusedLocal
    def gen_trap_type(self, data):
        name, enterprise, variables, description, reference, value = data

        pysmiName = self.trans_opers(name)

        enterpriseStr, parentOid = enterprise

        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = enterpriseStr + ".0." + str(value)
        outDict["class"] = "notificationtype"

        if variables:
            outDict["objects"] = [
                {
                    "module": self._importMap.get(
                        self.trans_opers(obj), self.moduleName[0]
                    ),
                    "object": obj,
                }
                for obj in variables
            ]

        if self.genRules["text"] and description:
            outDict["description"] = description

        if self.genRules["text"] and reference:
            outDict["reference"] = reference

        self.reg_sym(pysmiName, outDict, parentOid)

        return outDict

    # noinspection PyUnusedLocal
    def gen_type_declaration(self, data):
        name, declaration = data

        outDict = OrderedDict()
        outDict["name"] = name
        outDict["class"] = "type"

        if declaration:
            parentType, attrs = declaration
            if parentType:  # skipping SEQUENCE case
                pysmiName = self.trans_opers(name)
                outDict.update(attrs)
                self.reg_sym(pysmiName, outDict)

                # Establish if this type is derived from a Textual-Convention
                # declaration, as needed for pysnmp code generation.
                typeType = outDict["type"]["type"]
                if (
                    typeType in self.symbolTable[self.moduleName[0]]
                    or typeType in self._importMap
                ):
                    module = self._importMap.get(typeType, self.moduleName[0])
                    isDerivedFromTC = self.is_type_derived_from_tc(typeType, module)
                else:
                    isDerivedFromTC = False
                outDict["type"]["tcbase"] = isDerivedFromTC

        return outDict

    # noinspection PyUnusedLocal
    def gen_value_declaration(self, data):
        name, oid = data

        pysmiName = self.trans_opers(name)

        oidStr, parentOid = oid
        outDict = OrderedDict()
        outDict["name"] = name
        outDict["oid"] = oidStr
        outDict["class"] = "objectidentity"

        self.reg_sym(pysmiName, outDict, parentOid)

        return outDict

    # Subparts generation functions

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_bit_names(self, data):
        names = data[0]
        return names

    def gen_bits(self, data):
        bits = data[0]

        outDict = OrderedDict()
        outDict["type"] = "Bits"
        outDict["class"] = "type"
        outDict["bits"] = OrderedDict()

        for name, bit in sorted(bits, key=lambda x: x[1]):
            outDict["bits"][name] = bit

        return "scalar", outDict

    # noinspection PyUnusedLocal
    def gen_compliances(self, data):
        compliances = []

        for complianceModule in data[0]:
            name = complianceModule[0] or self.moduleName[0]
            compliances += [
                {"object": compl, "module": name} for compl in complianceModule[1]
            ]

        return compliances

    # noinspection PyUnusedLocal
    def gen_conceptual_table(self, data):
        row = data[0]

        if row[1] and row[1][-2:] == "()":
            row = row[1][:-2]
            self._rows.add(row)

        return "table", ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_contact_info(self, data):
        text = data[0]
        return self.textFilter("contact-info", text)

    def is_in_range(self, intersection, value):
        """Check whether the given value falls within the given constraints.

        The given list represents an intersection and contains elements that
        represent unions of individual ranges. Each individual range consists
        of one or two elements (a single value, or a low..high range). The
        given value is considered in range if it falls within range for each of
        the unions within the intersection. If the intersection is empty, the
        value is accepted as well.
        """
        for union in intersection:
            in_range = False
            for rng in union:
                if self.str2int(rng[0]) <= value <= self.str2int(rng[-1]):
                    in_range = True
                    break
            if not in_range:
                return False
        return True

    # noinspection PyUnusedLocal
    def gen_display_hint(self, data):
        return data[0]

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_def_val(self, data):
        return data[0]

    def _process_integer_defval(self, defval, defvalType):
        """Process a DEFVAL value for an integer/enumeration type."""
        if isinstance(defval, int):  # decimal
            value = defval

        elif self.is_hex(defval):  # hex
            value = int(defval[1:-2] or "0", 16)

        elif self.is_binary(defval):  # binary
            value = int(defval[1:-2] or "0", 2)

        elif isinstance(defvalType[1], dict):  # enumeration label
            # For enumerations, the ASN.1 DEFVAL statements contain names,
            # whereas the code generation template expects integer values
            # (represented as strings).
            nameToValueMap = defvalType[1]

            # buggy MIB: DEFVAL { { ... } }
            if isinstance(defval, list):
                defval = [dv for dv in defval if dv in nameToValueMap]
                if defval:
                    defval = defval[0]
                # (fall through)

            # good MIB: DEFVAL { ... }
            if defval not in nameToValueMap:
                raise ValueError("unknown enumeration label")

            # Return early so as to skip the enumeration value check below.
            # After all, we already know the resulting number is valid.
            return str(nameToValueMap[defval]), "decimal"

        else:
            raise ValueError("wrong input type for integer")

        if isinstance(defvalType[1], dict):  # enumeration number
            # For numerical values given for enumerated integers, make sure
            # that they are valid, because pyasn1 will not check this case and
            # thus let us set a default value that is not valid for the type.
            if value not in defvalType[1].values():
                raise ValueError("wrong enumeration value")

        elif isinstance(defvalType[1], list):  # range constraints
            if not self.is_in_range(defvalType[1], value):
                raise ValueError("value does not conform to range constraints")

        return str(value), "decimal"

    def _process_string_defval(self, defval, defvalType):
        """Process a DEFVAL value for a string type (including BITS)."""
        defvalBaseType = defvalType[0][0]  # either "OctetString" or "Bits"

        if isinstance(defval, int):  # decimal
            if defvalBaseType != "Bits":
                raise ValueError("decimal values have no meaning for OCTET STRING")

            # Convert the value to a hex string. Add padding if needed.
            value = defval and hex(defval)[2:] or ""
            value = ("0" + value) if len(value) % 2 == 1 else value

            fmt = "hex"

        elif self.is_hex(defval):  # hex
            # Extract the value as hex string. Add padding if needed.
            value = defval[1:-2]
            value = ("0" + value) if len(value) % 2 == 1 else value

            fmt = "hex"

        elif self.is_binary(defval):  # binary
            binval = defval[1:-2]

            # Make sure not to lose any leading zeroes. Add padding if needed.
            width = ((len(binval) + 7) // 8) * 2
            value = width and "{:0{width}x}".format(int(binval, 2), width=width) or ""
            fmt = "hex"

        elif defval and defval[0] == '"' and defval[-1] == '"':  # quoted string
            if defvalBaseType != "OctetString":
                raise ValueError("quoted strings have no meaning for BITS")

            value = defval[1:-1]
            fmt = "string"

        elif defvalBaseType == "Bits" and isinstance(defval, list):  # bit labels
            defvalBits = []

            bits = defvalType[1]

            for bit in defval:
                bitValue = bits.get(bit, None)
                if bitValue is not None:
                    defvalBits.append((bit, bitValue))
                else:
                    raise ValueError("unknown bit")

            return self.gen_bits([defvalBits])[1], "bits"

        else:
            raise ValueError("wrong input type for string")

        if defvalBaseType == "OctetString" and isinstance(
            defvalType[1], list
        ):  # size constraints
            size = len(value) // 2 if fmt == "hex" else len(value)

            if not self.is_in_range(defvalType[1], size):
                raise ValueError("value does not conform to size constraints")

        return value, fmt

    def _process_oid_defval(self, defval, defvalType):
        """Process a DEFVAL value for an object identifier."""
        if isinstance(defval, str) and (
            self.trans_opers(defval) in self.symbolTable[self.moduleName[0]]
            or self.trans_opers(defval) in self._importMap
        ):
            pysmiDefval = self.trans_opers(defval)
            module = self._importMap.get(pysmiDefval, self.moduleName[0])

            value = self.gen_numeric_oid(self.symbolTable[module][pysmiDefval]["oid"])

            return str(value), "oid"

        else:
            raise ValueError("wrong input type for object identifier")

    def process_defval(self, defval, objname):
        if defval is None:
            return {}

        defvalType = self.get_base_type(objname, self.moduleName[0])
        defvalBaseType = defvalType[0][0]

        # Our general policy is that DEFVAL values are considered discardable:
        # any values that were accepted by the parser but turn out to be
        # invalid, are dropped here, so that they will not keep the MIB from
        # being compiled or loaded. The underlying idea is that DEFVAL values
        # are not all that important, and therefore better discarded than the
        # cause of a MIB loading failure.
        #
        # For each combination of input value and object type, the following
        # table shows whether the combination is:
        # - required to be supported by RFC 2578;
        # - accepted out of lenience by our implementation; or,
        # - discarded for being no meaningful combination.
        # Note that enumerations are also Integer32/Integer types, but handled
        # slightly differently, so they are a separate column in this table.
        #
        # Input         Integer     Enumeration OctetString ObjectId.   Bits
        # ------------- ----------- ----------- ----------- ----------- ---------
        # decimal       required    accepted    discarded   discarded   accepted
        # hex           accepted    accepted    required    discarded   accepted
        # binary        accepted    accepted    required    discarded   accepted
        # quoted string discarded   discarded   required    discarded   discarded
        # symbol/label  discarded   required    discarded   required    discarded
        # brackets      discarded   accepted    discarded   discarded   required

        try:
            if defvalBaseType in ("Integer32", "Integer"):
                value, fmt = self._process_integer_defval(defval, defvalType)
            elif defvalBaseType in ("OctetString", "Bits"):
                value, fmt = self._process_string_defval(defval, defvalType)
            elif defvalBaseType == "ObjectIdentifier":
                value, fmt = self._process_oid_defval(defval, defvalType)
            else:  # pragma: no cover
                raise ValueError("unknown base type")

        except ValueError:
            # Discard the given default value.
            return {}

        outDict = OrderedDict(basetype=defvalBaseType, value=value, format=fmt)
        return {"default": outDict}

    # noinspection PyMethodMayBeStatic
    def gen_description(self, data):
        return self.textFilter("description", data[0])

    # noinspection PyMethodMayBeStatic
    def gen_reference(self, data):
        return self.textFilter("reference", data[0])

    # noinspection PyMethodMayBeStatic
    def gen_status(self, data):
        return data[0]

    def gen_product_release(self, data):
        return data[0]

    def gen_enum_spec(self, data):
        items = data[0]
        return {"enumeration": dict(items)}

    # noinspection PyUnusedLocal
    def gen_table_index(self, data):
        def gen_fake_sym_dict(fakeSym, fakeOidSuffix, idxType):
            syntaxDict = OrderedDict()
            syntaxDict["type"] = self.SMI_TYPES.get(idxType, idxType)
            syntaxDict["class"] = "type"

            outDict = OrderedDict()
            outDict["name"] = fakeSym
            outDict["oid"] = str(fakeOidSuffix)  # suffix only; fixed up later
            outDict["class"] = "objecttype"
            outDict["nodetype"] = "column"
            outDict["syntax"] = syntaxDict
            outDict["maxaccess"] = "not-accessible"
            outDict["status"] = "mandatory"  # SMIv1
            return outDict

        indexes = data[0]
        idxStrlist, fakeSyms, fakeSymDicts = [], [], []

        # For fake indices, we generate fake column object types. Each of those
        # is given its own OID. Minimize the chance of collisions by starting
        # with the highest possible OID child number and going backward.
        fakeOidSuffix = 2**32 - 1

        for idx in indexes:
            isImplied = idx[0]
            idxName = idx[1]
            if idxName in self.smiv1IdxTypes:  # SMIv1 support
                idxType = idxName
                idxName = self.fakeIdxPrefix + str(self.fakeIdxNumber)
                fakeSymDict = gen_fake_sym_dict(idxName, fakeOidSuffix, idxType)
                fakeSyms.append(idxName)
                fakeSymDicts.append(fakeSymDict)
                self.fakeIdxNumber += 1
                fakeOidSuffix -= 1

            index = OrderedDict()
            index["module"] = self._importMap.get(
                self.trans_opers(idxName), self.moduleName[0]
            )
            index["object"] = idxName
            index["implied"] = isImplied
            idxStrlist.append(index)

        return idxStrlist, fakeSyms, fakeSymDicts

    def gen_integer_subtype(self, data):
        ranges = []
        for rng in data[0]:
            vmin, vmax = len(rng) == 1 and (rng[0], rng[0]) or rng
            vmin, vmax = self.str2int(vmin), self.str2int(vmax)
            ran = OrderedDict()
            ran["min"] = vmin
            ran["max"] = vmax
            ranges.append(ran)

        return {"range": ranges}

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_max_access(self, data):
        return data[0]

    def gen_octetstring_subtype(self, data):
        sizes = []
        for rng in data[0]:
            vmin, vmax = len(rng) == 1 and (rng[0], rng[0]) or rng
            vmin, vmax = self.str2int(vmin), self.str2int(vmax)

            size = OrderedDict()
            size["min"] = vmin
            size["max"] = vmax
            sizes.append(size)

        outDict = {"size": sizes}

        # If the union of ranges consists of a single size, then store that
        # information as well, so that pysnmp (which needs to know) does not
        # have to compute it itself. We take a slightly elaborate approach so
        # that strange ranges such as SIZE(4 | 4) also produce a fixed size.
        #
        # Note that as per RFC 2578 Sec. 9 point (3), derived types may only
        # ever reduce the ranges of their base types, so we never need to
        # override a base type's fixed length to be "non-fixed" again.
        minSize = min(size["min"] for size in sizes)
        maxSize = max(size["max"] for size in sizes)
        if minSize == maxSize:
            outDict["fixed"] = minSize

        return outDict

    # noinspection PyUnusedLocal
    def gen_oid(self, data):
        out = ()
        parent = ""
        for el in data[0]:
            if isinstance(el, str):
                parent = self.trans_opers(el)
                out += ((parent, self._importMap.get(parent, self.moduleName[0])),)

            elif isinstance(el, int):
                out += (el,)

            elif isinstance(el, tuple):
                out += (el[1],)  # XXX Do we need to create a new object el[0]?

            else:
                raise error.PySmiSemanticError(f"unknown datatype for OID: {el}")

        return ".".join([str(x) for x in self.gen_numeric_oid(out)]), parent

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_objects(self, data):
        return data[0]

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_time(self, data):
        times = []
        for timeStr in data:
            if len(timeStr) == 11:
                timeStr = "19" + timeStr

            elif config.STRICT_MODE and len(timeStr) != 13:
                raise error.PySmiSemanticError(f"Invalid date {timeStr}")
            try:
                times.append(
                    strftime("%Y-%m-%d %H:%M", strptime(timeStr, "%Y%m%d%H%MZ"))
                )

            except ValueError:
                if config.STRICT_MODE:
                    raise error.PySmiSemanticError(f"Invalid date {timeStr}")

                timeStr = "197001010000Z"  # dummy date for dates with typos
                times.append(
                    strftime("%Y-%m-%d %H:%M", strptime(timeStr, "%Y%m%d%H%MZ"))
                )

        return times

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_last_updated(self, data):
        return data[0]

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_organization(self, data):
        return self.textFilter("organization", data[0])

    # noinspection PyUnusedLocal
    def gen_revisions(self, data):
        revisions = []
        for x in data[0]:
            revision = OrderedDict()
            revision["revision"] = self.gen_time([x[0]])[0]
            revision["description"] = self.textFilter("description", x[1][1])
            revisions.append(revision)
        return revisions

    def gen_row(self, data):
        row = data[0]
        row = self.trans_opers(row)

        return (
            row in self.symbolTable[self.moduleName[0]]["_symtable_rows"]
            and ("row", "")
            or self.gen_simple_syntax(data)
        )

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_sequence(self, data):
        return "", ""

    def gen_simple_syntax(self, data):
        objType = data[0]
        objType = self.SMI_TYPES.get(objType, objType)
        objType = self.trans_opers(objType)

        subtype = len(data) == 2 and data[1] or {}

        outDict = OrderedDict()
        outDict["type"] = objType
        outDict["class"] = "type"

        if subtype:
            outDict["constraints"] = subtype

        return "scalar", outDict

    # noinspection PyUnusedLocal
    def gen_type_declaration_rhs(self, data):
        if len(data) == 1:
            parentType, attrs = data[0]

            outDict = OrderedDict()
            if not attrs:
                return outDict
            # just syntax
            outDict["type"] = attrs

        else:
            # Textual convention
            display, status, description, reference, syntax = data
            parentType, attrs = syntax

            outDict = OrderedDict()
            outDict["type"] = attrs
            outDict["class"] = "textualconvention"
            if display:
                outDict["displayhint"] = display
            if status:
                outDict["status"] = status
            if self.genRules["text"] and description:
                outDict["description"] = description
            if self.genRules["text"] and reference:
                outDict["reference"] = reference

        return parentType, outDict

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def gen_units(self, data):
        text = data[0]
        return self.textFilter("units", text)

    handlersTable = {
        "agentCapabilitiesClause": gen_agent_capabilities,
        "moduleIdentityClause": gen_module_identity,
        "moduleComplianceClause": gen_module_compliance,
        "notificationGroupClause": gen_notification_group,
        "notificationTypeClause": gen_notification_type,
        "objectGroupClause": gen_object_group,
        "objectIdentityClause": gen_object_identity,
        "objectTypeClause": gen_object_type,
        "trapTypeClause": gen_trap_type,
        "typeDeclaration": gen_type_declaration,
        "valueDeclaration": gen_value_declaration,
        "PRODUCT-RELEASE": gen_product_release,
        "ApplicationSyntax": gen_simple_syntax,
        "BitNames": gen_bit_names,
        "BITS": gen_bits,
        "ComplianceModules": gen_compliances,
        "conceptualTable": gen_conceptual_table,
        "CONTACT-INFO": gen_contact_info,
        "DISPLAY-HINT": gen_display_hint,
        "DEFVAL": gen_def_val,
        "DESCRIPTION": gen_description,
        "REFERENCE": gen_reference,
        "Status": gen_status,
        "enumSpec": gen_enum_spec,
        "INDEX": gen_table_index,
        "integerSubType": gen_integer_subtype,
        "MaxAccessPart": gen_max_access,
        "Notifications": gen_objects,
        "octetStringSubType": gen_octetstring_subtype,
        "objectIdentifier": gen_oid,
        "Objects": gen_objects,
        "LAST-UPDATED": gen_last_updated,
        "ORGANIZATION": gen_organization,
        "Revisions": gen_revisions,
        "row": gen_row,
        "SEQUENCE": gen_sequence,
        "SimpleSyntax": gen_simple_syntax,
        "typeDeclarationRHS": gen_type_declaration_rhs,
        "UNITS": gen_units,
        "VarTypes": gen_objects,
        # 'a': lambda x: genXXX(x, 'CONSTRAINT')
    }

    # TODO: make intermediate format less tied to JSON
    # One thing is to produce OIDs in a tuple form
    # The other thing is index data - may be we should
    # have it prepared at the intermediate stage...?

    def gen_code(self, ast, symbolTable, **kwargs):
        self.genRules["text"] = kwargs.get("genTexts", False)
        self.textFilter = kwargs.get("textFilter") or (
            lambda symbol, text: re.sub(r"\s+", " ", text)
        )
        self.symbolTable = symbolTable
        self._rows.clear()
        self._seenSyms.clear()
        self._importMap.clear()
        self._out.clear()
        self._moduleIdentityOid = None
        self._enterpriseOid = None
        self._oids = set()
        self._complianceOids = []
        self.moduleName[0], moduleOid, imports, declarations = ast

        outDict, importedModules = self.gen_imports(imports)

        for declr in declarations or []:
            if declr:
                self.handlersTable[declr[0]](self, self.prep_data(declr[1:]))

        for sym in self.symbolTable[self.moduleName[0]]["_symtable_order"]:
            if sym not in self._out:
                raise error.PySmiCodegenError(f"No generated code for symbol {sym}")

            outDict[sym] = self._out[sym]

        outDict["meta"] = OrderedDict()
        outDict["meta"]["module"] = self.moduleName[0]

        if "comments" in kwargs:
            outDict["meta"]["comments"] = kwargs["comments"]

        debug.logger & debug.FLAG_CODEGEN and debug.logger(
            f"canonical MIB name {self.moduleName[0]} ({moduleOid}), imported MIB(s) {','.join(importedModules) or '<none>'}"
        )

        return (
            MibInfo(
                oid=moduleOid,
                identity=self._moduleIdentityOid,
                name=self.moduleName[0],
                revision=self._moduleRevision,
                oids=self._oids,
                enterprise=self._enterpriseOid,
                compliance=self._complianceOids,
                imported=tuple(x for x in importedModules if x not in self.fakeMibs),
            ),
            outDict,
        )
