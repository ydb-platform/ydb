#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
# Build an internally used symbol table for each passed MIB.
#
from pysmi import config, debug, error, implicit_imports
from pysmi.codegen.base import AbstractCodeGen
from pysmi.mibinfo import MibInfo


class SymtableCodeGen(AbstractCodeGen):
    symsTable = {
        "MODULE-IDENTITY": ("ModuleIdentity",),
        "OBJECT-TYPE": ("MibScalar", "MibTable", "MibTableRow", "MibTableColumn"),
        "NOTIFICATION-TYPE": ("NotificationType",),
        "TEXTUAL-CONVENTION": ("TextualConvention",),
        "MODULE-COMPLIANCE": ("ModuleCompliance",),
        "OBJECT-GROUP": ("ObjectGroup",),
        "NOTIFICATION-GROUP": ("NotificationGroup",),
        "AGENT-CAPABILITIES": ("AgentCapabilities",),
        "OBJECT-IDENTITY": ("ObjectIdentity",),
        "TRAP-TYPE": ("NotificationType",),  # smidump always uses NotificationType
        "BITS": ("Bits",),
    }

    constImports = {
        "SNMPv2-SMI": (
            "iso",
            "Bits",  # XXX
            "Integer32",  # XXX
            "TimeTicks",  # bug in some IETF MIBs
            "Counter32",  # bug in some IETF MIBs (e.g. DSA-MIB)
            "Counter64",  # bug in some MIBs (e.g.A3COM-HUAWEI-LswINF-MIB)
            "NOTIFICATION-TYPE",  # bug in some MIBs (e.g. A3COM-HUAWEI-DHCPSNOOP-MIB)
            "Gauge32",  # bug in some IETF MIBs (e.g. DSA-MIB)
            "MODULE-IDENTITY",
            "OBJECT-TYPE",
            "OBJECT-IDENTITY",
            "Unsigned32",
            "IpAddress",  # XXX
            "MibIdentifier",
        ),  # OBJECT IDENTIFIER
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

    baseTypes = ["Integer", "Integer32", "Bits", "ObjectIdentifier", "OctetString"]

    typeClasses = {
        "COUNTER32": "Counter32",
        "COUNTER64": "Counter64",
        "GAUGE32": "Gauge32",
        "INTEGER": "Integer32",  # XXX
        "INTEGER32": "Integer32",
        "IPADDRESS": "IpAddress",
        "NETWORKADDRESS": "IpAddress",
        "OBJECT IDENTIFIER": "ObjectIdentifier",
        "OCTET STRING": "OctetString",
        "OPAQUE": "Opaque",
        "TIMETICKS": "TimeTicks",
        "UNSIGNED32": "Unsigned32",
        "Counter": "Counter32",
        "Gauge": "Gauge32",
        "NetworkAddress": "IpAddress",  # RFC1065-SMI, RFC1155-SMI -> SNMPv2-SMI
        "nullSpecific": "zeroDotZero",  # RFC1158-MIB -> SNMPv2-SMI
        "ipRoutingTable": "ipRouteTable",  # RFC1158-MIB -> RFC1213-MIB
        "snmpEnableAuthTraps": "snmpEnableAuthenTraps",  # RFC1158-MIB -> SNMPv2-MIB
    }

    def __init__(self):
        self._rows = set()  # symbols
        self._cols = {}  # k, v = name, datatype [name is *not* a Pythonized symbol!]
        self._sequenceTypes = set()
        self._exports = set()
        self._postponedSyms = {}  # k, v = symbol, (parents, properties)
        self._parentOids = set()
        self._importMap = {}  # k, v = symbol, MIB
        self._symsOrder = []
        self._out = {}  # k, v = symbol, properties
        self.moduleName = ["DUMMY"]
        self._moduleRevision = None
        self.genRules = {"text": True}

    def sym_trans(self, symbol):
        if symbol in self.symsTable:
            return self.symsTable[symbol]

        return (symbol,)

    def prep_data(self, pdata, classmode=False):
        data = []
        for el in pdata:
            if not isinstance(el, tuple):
                data.append(el)

            elif len(el) == 1:
                data.append(el[0])

            else:
                data.append(
                    self.handlersTable[el[0]](
                        self,
                        self.prep_data(el[1:], classmode=classmode),
                        classmode=classmode,
                    )
                )

        return data

    def gen_imports(self, imports, apply_implicit=False):
        # normalize incoming imports into a mutable mapping of lists
        if not isinstance(imports, dict):
            try:
                imports = dict(imports)
            except Exception:
                imports = {}

        # ensure all import value containers are lists we can append to
        for m in list(imports):
            imports[m] = list(imports[m])

        # helper to safely add a symbol to imports[module]
        def _add_import(mod, sym):
            if mod in imports:
                if isinstance(imports[mod], list):
                    imports[mod].append(sym)
                else:
                    try:
                        imports[mod] = list(imports[mod]) + [sym]
                    except Exception:
                        imports[mod] = [sym]
            else:
                imports[mod] = [sym]

        # convertion to SNMPv2
        toDel = []
        for module in list(imports):
            if module in self.convertImportv2:
                for symbol in imports[module]:
                    if symbol in self.convertImportv2[module]:
                        toDel.append((module, symbol))

                        for newImport in self.convertImportv2[module][symbol]:
                            newModule, newSymbol = newImport
                            _add_import(newModule, newSymbol)

        # removing converted symbols
        for d in toDel:
            imports[d[0]].remove(d[1])

        # merging mib and constant imports (ensure lists)
        for module in self.constImports:
            for sym in self.constImports[module]:
                _add_import(module, sym)

        # apply per-MIB implicit imports only when explicitly requested
        if apply_implicit:
            try:
                mib_exceptions = implicit_imports.IMPLICIT_IMPORTS.get(
                    self.moduleName[0]
                )
            except Exception:
                mib_exceptions = None

            if mib_exceptions:
                for newModule, newSymbol in mib_exceptions:
                    _add_import(newModule, newSymbol)

        for module in sorted(imports):
            symbols = ()
            for symbol in set(imports[module]):
                symbols += self.sym_trans(symbol)

            if symbols:
                self._importMap.update([(self.trans_opers(s), module) for s in symbols])

        return {}, tuple(sorted(imports))

    def all_parents_exists(self, parents):
        parentsExists = True
        for parent in parents:
            if not (
                parent in self._out
                or parent in self._importMap
                or parent in self.baseTypes
                or parent in ("MibTable", "MibTableRow", "MibTableColumn")
                or parent in self._rows
            ):
                parentsExists = False
                break

        return parentsExists

    def reg_sym(self, symbol, symProps, parents=()):
        if (
            symbol in self._out
            or symbol in self._postponedSyms
            or (config.STRICT_MODE and symbol in self._importMap)
        ):
            raise error.PySmiSemanticError(f"Duplicate symbol found: {symbol}")

        if self.all_parents_exists(parents):
            self._out[symbol] = symProps
            self._symsOrder.append(symbol)
            self.reg_postponed_syms()

        else:
            self._postponedSyms[symbol] = (parents, symProps)

    def reg_postponed_syms(self):
        regedSyms = []
        for sym, val in self._postponedSyms.items():
            parents, symProps = val

            if self.all_parents_exists(parents):
                self._out[sym] = symProps
                self._symsOrder.append(sym)
                regedSyms.append(sym)

        for sym in regedSyms:
            self._postponedSyms.pop(sym)

        # Clause handlers

    # noinspection PyUnusedLocal
    def gen_agent_capabilities(self, data, classmode=False):
        origName, release, status, description, reference, oid = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "AgentCapabilities", "oid": oid, "origName": origName}

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_module_identity(self, data, classmode=False):
        (
            origName,
            lastUpdated,
            organization,
            contactInfo,
            description,
            revisions,
            oid,
        ) = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "ModuleIdentity", "oid": oid, "origName": origName}

        if revisions:
            self._moduleRevision = revisions[0]

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_module_compliance(self, data, classmode=False):
        origName, status, description, reference, compliances, oid = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "ModuleCompliance", "oid": oid, "origName": origName}

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_notification_group(self, data, classmode=False):
        origName, objects, status, description, reference, oid = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "NotificationGroup", "oid": oid, "origName": origName}

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_notification_type(self, data, classmode=False):
        origName, objects, status, description, reference, oid = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "NotificationType", "oid": oid, "origName": origName}

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_object_group(self, data, classmode=False):
        origName, objects, status, description, reference, oid = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "ObjectGroup", "oid": oid, "origName": origName}

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_object_identity(self, data, classmode=False):
        origName, status, description, reference, oid = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "ObjectIdentity", "oid": oid, "origName": origName}

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_object_type(self, data, classmode=False):
        (
            origName,
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

        pysmiName = self.trans_opers(origName)

        symProps = {
            "type": "ObjectType",
            "oid": oid,
            "syntax": syntax,  # (type, module), subtype
            "origName": origName,
        }

        parents = [syntax[0][0]]

        if augmention:
            parents.append(self.trans_opers(augmention))

        if index and index[1]:
            namepart, fakeIndexes, fakeSymSyntax = index
            for fakeIdx, fakeSyntax in zip(fakeIndexes, fakeSymSyntax):
                fakeName = namepart + str(fakeIdx)

                fakeSymProps = {
                    "type": "fakeColumn",
                    "oid": oid + (fakeIdx,),
                    "syntax": fakeSyntax,
                    "origName": fakeName,
                }

                self.reg_sym(fakeName, fakeSymProps)

        self.reg_sym(pysmiName, symProps, parents)

    # noinspection PyUnusedLocal
    def gen_trap_type(self, data, classmode=False):
        origName, enterprise, variables, description, reference, value = data

        pysmiName = self.trans_opers(origName)

        symProps = {
            "type": "NotificationType",
            "oid": enterprise + (0, value),
            "origName": origName,
        }

        self.reg_sym(pysmiName, symProps)

    # noinspection PyUnusedLocal
    def gen_type_declaration(self, data, classmode=False):
        origName, declaration = data

        pysmiName = self.trans_opers(origName)

        if declaration:
            parentType, attrs, isTC = declaration

            if parentType:  # skipping SEQUENCE case
                symProps = {
                    "type": "TypeDeclaration",
                    "syntax": (parentType, attrs),  # (type, module), subtype
                    "origName": origName,
                    "isTC": isTC,
                }

                self.reg_sym(pysmiName, symProps, [declaration[0][0]])
            else:
                self._sequenceTypes.add(pysmiName)

    # noinspection PyUnusedLocal
    def gen_value_declaration(self, data, classmode=False):
        origName, oid = data

        pysmiName = self.trans_opers(origName)

        symProps = {"type": "MibIdentifier", "oid": oid, "origName": origName}

        self.reg_sym(pysmiName, symProps)

    # Subparts generation functions
    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def gen_bit_names(self, data, classmode=False):
        names = data[0]
        return names

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def gen_bits(self, data, classmode=False):
        bits = dict(data[0])
        return ("Bits", ""), bits

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_compliances(self, data, classmode=False):
        return ""

    # noinspection PyUnusedLocal
    def gen_conceptual_table(self, data, classmode=False):
        row = data[0]
        if row[0] and row[0][0]:
            self._rows.add(row[0][0])  # (already a Pythonized symbol)
        return ("MibTable", ""), ""

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_contact_info(self, data, classmode=False):
        return ""

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_display_hint(self, data, classmode=False):
        return ""

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_def_val(self, data, classmode=False):
        return ""

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_description(self, data, classmode=False):
        return ""

    def gen_reference(self, data, classmode=False):
        return ""

    def gen_status(self, data, classmode=False):
        return ""

    def gen_product_release(self, data, classmode=False):
        return ""

    def gen_enum_spec(self, data, classmode=False):
        return self.gen_bits(data, classmode=classmode)[1]

    def gen_index(self, data, classmode=False):
        indexes = data[0]

        fakeIndexes, fakeSymsSyntax = [], []

        for idx in indexes:
            idxName = idx[1]
            if idxName in self.smiv1IdxTypes:  # SMIv1 support
                idxType = idxName

                objType = self.typeClasses.get(idxType, idxType)
                objType = self.trans_opers(objType)

                fakeIndexes.append(self.fakeIdxNumber)
                fakeSymsSyntax.append((("MibTableColumn", ""), objType))
                self.fakeIdxNumber += 1

        return self.fakeIdxPrefix, fakeIndexes, fakeSymsSyntax

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_integer_subtype(self, data, classmode=False):
        return [data[0]]

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_max_access(self, data, classmode=False):
        return ""

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_octetstring_subtype(self, data, classmode=False):
        return [data[0]]

    # noinspection PyUnusedLocal
    def gen_oid(self, data, classmode=False):
        out = ()
        for el in data[0]:
            if isinstance(el, str):
                parent = self.trans_opers(el)
                self._parentOids.add(parent)
                out += ((parent, self._importMap.get(parent, self.moduleName[0])),)

            elif isinstance(el, int):
                out += (el,)

            elif isinstance(el, tuple):
                out += (el[1],)  # XXX Do we need to create a new object el[0]?

            else:
                raise error.PySmiSemanticError(f"unknown datatype for OID: {el}")

        return out

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_objects(self, data, classmode=False):
        return ""

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_time(self, data, classmode=False):
        return ""

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_last_updated(self, data, classmode=False):
        return data[0]

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_organization(self, data, classmode=False):
        return data[0]

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_revisions(self, data, classmode=False):
        lastRevision, lastDescription = data[0][0][0], data[0][0][1][1]
        return lastRevision, lastDescription

    def gen_row(self, data, classmode=False):
        row = data[0]
        row = self.trans_opers(row)
        return (
            row in self._rows
            and (("MibTableRow", ""), "")
            or self.gen_simple_syntax(data, classmode=classmode)
        )

    # noinspection PyUnusedLocal
    def gen_sequence(self, data, classmode=False):
        cols = data[0]
        self._cols.update(cols)
        return "", ""

    # noinspection PyUnusedLocal
    def gen_simple_syntax(self, data, classmode=False):
        objType = data[0]

        module = ""

        objType = self.typeClasses.get(objType, objType)
        objType = self.trans_opers(objType)

        if objType not in self.baseTypes:
            module = self._importMap.get(objType, self.moduleName[0])

        subtype = len(data) == 2 and data[1] or ""

        return (objType, module), subtype

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def gen_type_declaration_rhs(self, data, classmode=False):
        if len(data) == 1:
            parentType, attrs = data[0]  # just syntax
            isTC = False

        else:
            # Textual convention
            display, status, description, reference, syntax = data
            parentType, attrs = syntax
            isTC = True

        return parentType, attrs, isTC

    # noinspection PyUnusedLocal,PyUnusedLocal,PyMethodMayBeStatic
    def gen_units(self, data, classmode=False):
        return ""

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
        "PRODUCT-RELEASE": gen_product_release,
        "enumSpec": gen_enum_spec,
        "INDEX": gen_index,
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
    }

    def correct_postponed_syms(self):
        """
        We are about to fail on unresolvable symbols.

        See if we can correct any cases to make them valid after all.
        """
        for sym, val in self._postponedSyms.items():
            parents, symProps = val

            if symProps["type"] == "ObjectType":
                syntax = symProps["syntax"]

                # Leniency for non-conforming MIBs: if the object type is not
                # known as a table row, but it has a SEQUENCE-type syntax, then
                # turn it into a table row after all. This leniency exception
                # allows the "SEQUENCE OF" type name of the conceptual table to
                # contain typos in common cases.
                if syntax[0][0] in self._sequenceTypes:
                    self._rows.add(syntax[0][0])
                    symProps["syntax"] = (("MibTableRow", ""), "")
                    parents[0] = "MibTableRow"

        self.reg_postponed_syms()

    def gen_code(self, ast, symbolTable, **kwargs):
        self.genRules["text"] = kwargs.get("genTexts", False)
        self._rows.clear()
        self._cols.clear()
        self._parentOids.clear()
        self._symsOrder = []
        self._postponedSyms.clear()
        self._importMap.clear()
        self._out = {}  # should be new object, do not use `clear` method
        self.moduleName[0], moduleOid, imports, declarations = ast
        # First attempt without implicit imports
        try:
            out, importedModules = self.gen_imports(imports, apply_implicit=False)

            for declr in declarations or []:
                if declr:
                    clausetype = declr[0]
                    classmode = clausetype == "typeDeclaration"
                    self.handlersTable[declr[0]](
                        self, self.prep_data(declr[1:], classmode), classmode
                    )

            if self._postponedSyms:
                self.correct_postponed_syms()
                if self._postponedSyms:
                    raise error.PySmiSemanticError(
                        f"Unknown parents for symbols: {', '.join(self._postponedSyms)}"
                    )

            for sym in self._parentOids:
                if sym not in self._out and sym not in self._importMap:
                    raise error.PySmiSemanticError(f"Unknown parent symbol: {sym}")

        except error.PySmiSemanticError:
            # Try again only if we have implicit imports defined for this MIB
            if implicit_imports.IMPLICIT_IMPORTS.get(self.moduleName[0]):
                # reset state and retry with implicit imports applied
                self._rows.clear()
                self._cols.clear()
                self._parentOids.clear()
                self._symsOrder = []
                self._postponedSyms.clear()
                self._importMap.clear()
                self._out = {}

                out, importedModules = self.gen_imports(imports, apply_implicit=True)

                for declr in declarations or []:
                    if declr:
                        clausetype = declr[0]
                        classmode = clausetype == "typeDeclaration"
                        self.handlersTable[declr[0]](
                            self, self.prep_data(declr[1:], classmode), classmode
                        )

                if self._postponedSyms:
                    self.correct_postponed_syms()
                    if self._postponedSyms:
                        raise error.PySmiSemanticError(
                            f"Unknown parents for symbols: {', '.join(self._postponedSyms)}"
                        )

                for sym in self._parentOids:
                    if sym not in self._out and sym not in self._importMap:
                        raise error.PySmiSemanticError(f"Unknown parent symbol: {sym}")
            else:
                raise

        self._out["_symtable_order"] = list(self._symsOrder)
        self._out["_symtable_cols"] = list(self._cols)
        self._out["_symtable_rows"] = list(self._rows)

        debug.logger & debug.FLAG_CODEGEN and debug.logger(
            f"canonical MIB name {self.moduleName[0]} ({moduleOid}), imported MIB(s) {','.join(importedModules) or '<none>'}, Symbol table size {len(self._out)} symbols"
        )

        return (
            MibInfo(
                oid=None,
                name=self.moduleName[0],
                revision=self._moduleRevision,
                imported=tuple(x for x in importedModules),
            ),
            self._out,
        )
