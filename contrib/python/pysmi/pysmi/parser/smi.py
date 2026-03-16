#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import os
import sys

import ply.yacc as yacc
from pysmi import debug
from pysmi import error
from pysmi.lexer.smi import lexerFactory
from pysmi.parser.base import AbstractParser

YACC_VERSION = [int(x) for x in yacc.__version__.split(".")]


# noinspection PyMethodMayBeStatic,PyIncorrectDocstring
class SmiV2Parser(AbstractParser):
    defaultLexer = lexerFactory()

    def __init__(self, startSym="mibFile", tempdir=""):
        if tempdir:
            tempdir = os.path.join(tempdir, startSym)
            try:
                os.makedirs(tempdir)
            except OSError:
                if sys.exc_info()[1].errno != 17:
                    raise error.PySmiError(
                        f"Failed to create cache directory {tempdir}: {sys.exc_info()[1]}"
                    )

        self.lexer = self.defaultLexer(tempdir=tempdir)

        # tokens are required for parser
        self.tokens = self.lexer.tokens

        if YACC_VERSION < [3, 0]:
            self.parser = yacc.yacc(
                module=self,
                start=startSym,
                write_tables=bool(tempdir),
                debug=False,
                outputdir=tempdir,
            )
        else:
            if debug.logger & debug.FLAG_PARSER:
                logger = debug.logger.get_current_logger()
            else:
                logger = yacc.NullLogger()

            if debug.logger & debug.FLAG_GRAMMAR:
                debuglogger = debug.logger.get_current_logger()
            else:
                debuglogger = None

            self.parser = yacc.yacc(
                module=self,
                start=startSym,
                write_tables=bool(tempdir),
                debug=False,
                outputdir=tempdir,
                debuglog=debuglogger,
                errorlog=logger,
            )

    def reset(self):
        # Ply requires lexer reinitialization for (at least) resetting lineno
        self.lexer.reset()

    def parse(self, data, **kwargs):
        debug.logger & debug.FLAG_PARSER and debug.logger(
            f'source MIB size is {len(data)} characters, first 50 characters are "{data[:50]}..."'
        )

        ast = self.parser.parse(data, lexer=self.lexer.lexer)

        self.reset()

        if ast and ast[0] == "mibFile" and ast[1]:  # mibfile is not empty
            return ast[1]
        else:
            return []

    #
    # SMIv2 grammar follows
    #

    def p_mibFile(self, p):
        """mibFile : modules
        | empty"""
        p[0] = ("mibFile", p[1])

    def p_modules(self, p):
        """modules : modules module
        | module"""
        n = len(p)
        if n == 3:
            p[0] = p[1] + [p[2]]
        elif n == 2:
            p[0] = [p[1]]

    def p_module(self, p):
        """module : moduleName moduleOid DEFINITIONS COLON_COLON_EQUAL BEGIN exportsClause linkagePart declarationPart END"""
        p[0] = (
            p[1],  # name
            p[2],  # oid
            p[7] or {},  # linkage (imports)
            p[8],
        )  # declaration

    def p_moduleOid(self, p):
        """moduleOid : '{' objectIdentifier '}'
        | empty"""
        n = len(p)
        if n == 4:
            p[0] = p[2]

    def p_linkagePart(self, p):
        """linkagePart : linkageClause
        | empty"""
        if p[1]:
            p[0] = p[1]

    def p_linkageClause(self, p):
        """linkageClause : IMPORTS importPart ';'"""
        p[0] = p[2]

    def p_exportsClause(self, p):
        """exportsClause : EXPORTS
        | empty"""

    def p_importPart(self, p):
        """importPart : imports
        | empty"""
        # libsmi: TODO: ``IMPORTS ;'' allowed? refer ASN.1!
        if p[1]:
            importDict = {}
            for imp in p[1]:  # don't do just dict() because moduleNames may be repeated
                fromModule, symbols = imp
                if fromModule in importDict:
                    importDict[fromModule] += symbols
                else:
                    importDict[fromModule] = symbols

            p[0] = importDict

    def p_imports(self, p):
        """imports : imports import
        | import"""
        n = len(p)
        if n == 3:
            p[0] = p[1] + [p[2]]
        elif n == 2:
            p[0] = [p[1]]

    def p_import(self, p):
        """import : importIdentifiers FROM moduleName"""
        # libsmi: TODO: multiple clauses with same moduleName allowed?
        # I guess so. refer ASN.1!
        p[0] = (p[3], p[1])  # moduleName  # ids

    def p_importIdentifiers(self, p):
        """importIdentifiers : importIdentifiers ',' importIdentifier
        | importIdentifier"""
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]

    # Note that some named types must not be imported, REF:RFC1902,590
    def p_importIdentifier(self, p):
        """importIdentifier : LOWERCASE_IDENTIFIER
        | UPPERCASE_IDENTIFIER
        | importedKeyword"""
        p[0] = p[1]

    def p_importedKeyword(self, p):
        """importedKeyword : importedSMIKeyword
        | BITS
        | INTEGER32
        | IPADDRESS
        | MANDATORY_GROUPS
        | MODULE_COMPLIANCE
        | MODULE_IDENTITY
        | OBJECT_GROUP
        | OBJECT_IDENTITY
        | OBJECT_TYPE
        | OPAQUE
        | TEXTUAL_CONVENTION
        | TIMETICKS
        | UNSIGNED32"""
        p[0] = p[1]

    def p_importedSMIKeyword(self, p):
        """importedSMIKeyword : AGENT_CAPABILITIES
        | COUNTER32
        | COUNTER64
        | GAUGE32
        | NOTIFICATION_GROUP
        | NOTIFICATION_TYPE
        | TRAP_TYPE"""
        p[0] = p[1]

    def p_moduleName(self, p):
        """moduleName : UPPERCASE_IDENTIFIER"""
        p[0] = p[1]

    def p_declarationPart(self, p):
        """declarationPart : declarations
        | empty"""
        if p[1]:
            p[0] = p[1]

    def p_declarations(self, p):
        """declarations : declarations declaration
        | declaration"""
        n = len(p)
        if n == 3:
            p[0] = p[1] + [p[2]]
        elif n == 2:
            p[0] = [p[1]]

    def p_declaration(self, p):
        """declaration : typeDeclaration
        | valueDeclaration
        | objectIdentityClause
        | objectTypeClause
        | trapTypeClause
        | notificationTypeClause
        | moduleIdentityClause
        | moduleComplianceClause
        | objectGroupClause
        | notificationGroupClause
        | agentCapabilitiesClause
        | macroClause"""
        if p[1]:
            p[0] = p[1]

    def p_macroClause(self, p):
        """macroClause : macroName MACRO END"""

    def p_macroName(self, p):
        """macroName : MODULE_IDENTITY
        | OBJECT_TYPE
        | TRAP_TYPE
        | NOTIFICATION_TYPE
        | OBJECT_IDENTITY
        | TEXTUAL_CONVENTION
        | OBJECT_GROUP
        | NOTIFICATION_GROUP
        | MODULE_COMPLIANCE
        | AGENT_CAPABILITIES"""

    def p_choiceClause(self, p):
        """choiceClause : CHOICE"""

    # libsmi: The only ASN.1 value declarations are for OIDs, REF:RFC1902,491.
    def p_fuzzy_lowercase_identifier(self, p):
        """fuzzy_lowercase_identifier : LOWERCASE_IDENTIFIER
        | UPPERCASE_IDENTIFIER"""
        p[0] = p[1]

    def p_valueDeclaration(self, p):
        """valueDeclaration : fuzzy_lowercase_identifier OBJECT IDENTIFIER COLON_COLON_EQUAL '{' objectIdentifier '}'"""
        p[0] = ("valueDeclaration", p[1], p[6])  # id  # objectIdentifier

    def p_typeDeclaration(self, p):
        """typeDeclaration : typeName COLON_COLON_EQUAL typeDeclarationRHS"""
        p[0] = ("typeDeclaration", p[1], p[3])  # name  # declarationRHS

    def p_typeName(self, p):
        """typeName : UPPERCASE_IDENTIFIER
        | typeSMI"""
        p[0] = p[1]

    def p_typeSMI(self, p):
        """typeSMI : typeSMIandSPPI
        | typeSMIonly"""
        p[0] = p[1]

    def p_typeSMIandSPPI(self, p):
        """typeSMIandSPPI : IPADDRESS
        | TIMETICKS
        | OPAQUE
        | INTEGER32
        | UNSIGNED32"""
        p[0] = p[1]

    def p_typeSMIonly(self, p):
        """typeSMIonly : COUNTER32
        | GAUGE32
        | COUNTER64"""
        p[0] = p[1]

    def p_typeDeclarationRHS(self, p):
        """typeDeclarationRHS : Syntax
        | TEXTUAL_CONVENTION DisplayPart STATUS Status DESCRIPTION Text ReferPart SYNTAX Syntax
        | choiceClause"""
        if p[1]:
            if p[1] == "TEXTUAL-CONVENTION":
                p[0] = (
                    "typeDeclarationRHS",
                    p[2],  # display
                    p[4],  # status
                    (p[5], p[6]),  # description
                    p[7],  # reference
                    p[9],
                )  # syntax
            else:
                p[0] = ("typeDeclarationRHS", p[1])
                # ignore the choiceClause

    def p_conceptualTable(self, p):
        """conceptualTable : SEQUENCE OF row"""
        p[0] = ("conceptualTable", p[3])

    def p_row(self, p):
        """row : UPPERCASE_IDENTIFIER"""
        # libsmi: TODO: this must be an entryType
        p[0] = ("row", p[1])

    def p_entryType(self, p):
        """entryType : SEQUENCE '{' sequenceItems '}'"""
        p[0] = (p[1], p[3])

    def p_sequenceItems(self, p):
        """sequenceItems : sequenceItems ',' sequenceItem
        | sequenceItem"""
        # libsmi: TODO: might this list be emtpy?
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]

    def p_sequenceItem(self, p):
        """sequenceItem : LOWERCASE_IDENTIFIER sequenceSyntax"""
        p[0] = (p[1], p[2])

    def p_Syntax(self, p):
        """Syntax : ObjectSyntax
        | BITS '{' NamedBits '}'"""
        # libsmi: TODO: standalone `BITS' ok? seen in RMON2-MIB
        # libsmi: -> no, it's only allowed in a SEQUENCE {...}
        n = len(p)
        if n == 2:
            p[0] = p[1]
        elif n == 5:
            p[0] = (p[1], p[3])

    def p_sequenceSyntax(self, p):
        """sequenceSyntax : BITS
        | UPPERCASE_IDENTIFIER anySubType
        | sequenceObjectSyntax"""
        p[0] = p[1]  # no subtype or complex syntax supported

    def p_NamedBits(self, p):
        """NamedBits : NamedBits ',' NamedBit
        | NamedBit"""
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]

    def p_NamedBit(self, p):
        """NamedBit : LOWERCASE_IDENTIFIER '(' NUMBER ')'"""
        p[0] = (p[1], p[3])

    def p_objectIdentityClause(self, p):
        """objectIdentityClause : LOWERCASE_IDENTIFIER OBJECT_IDENTITY STATUS Status DESCRIPTION Text ReferPart COLON_COLON_EQUAL '{' objectIdentifier '}'"""
        p[0] = (
            "objectIdentityClause",
            p[1],  # id
            #  p[2], # OBJECT_IDENTITY
            p[4],  # status
            (p[5], p[6]),  # description
            p[7],  # reference
            p[10],
        )  # objectIdentifier

    def p_objectTypeClause(self, p):
        """objectTypeClause : LOWERCASE_IDENTIFIER OBJECT_TYPE SYNTAX Syntax UnitsPart MaxOrPIBAccessPart STATUS Status descriptionClause ReferPart IndexPart MibIndex DefValPart COLON_COLON_EQUAL '{' ObjectName '}'"""
        p[0] = (
            "objectTypeClause",
            p[1],  # id
            #  p[2], # OBJECT_TYPE
            p[4],  # syntax
            p[5],  # UnitsPart
            p[6],  # MaxOrPIBAccessPart
            p[8],  # status
            p[9],  # descriptionClause
            p[10],  # reference
            p[11],  # augmentions
            p[12],  # index
            p[13],  # DefValPart
            p[16],
        )  # ObjectName

    def p_descriptionClause(self, p):
        """descriptionClause : DESCRIPTION Text
        | empty"""
        if p[1]:
            p[0] = (p[1], p[2])

    def p_trapTypeClause(self, p):
        """trapTypeClause : fuzzy_lowercase_identifier TRAP_TYPE ENTERPRISE objectIdentifier VarPart DescrPart ReferPart COLON_COLON_EQUAL NUMBER"""
        # libsmi: TODO: range of number?
        p[0] = (
            "trapTypeClause",
            p[1],  # fuzzy_lowercase_identifier
            #  p[2], # TRAP_TYPE
            p[4],  # objectIdentifier
            p[5],  # VarPart
            p[6],  # description
            p[7],  # reference
            p[9],
        )  # NUMBER

    def p_VarPart(self, p):
        """VarPart : VARIABLES '{' VarTypes '}'
        | empty"""
        p[0] = p[1] and p[3] or []

    def p_VarTypes(self, p):
        """VarTypes : VarTypes ',' VarType
        | VarType"""
        n = len(p)
        if n == 4:
            p[0] = ("VarTypes", p[1][1] + [p[3]])
        elif n == 2:
            p[0] = ("VarTypes", [p[1]])

    def p_VarType(self, p):
        """VarType : ObjectName"""
        p[0] = p[1][1][0]

    def p_DescrPart(self, p):
        """DescrPart : DESCRIPTION Text
        | empty"""
        if p[1]:
            p[0] = (p[1], p[2])

    def p_MaxOrPIBAccessPart(self, p):
        """MaxOrPIBAccessPart : MaxAccessPart
        | empty"""
        if p[1]:
            p[0] = p[1]

    def p_MaxAccessPart(self, p):
        """MaxAccessPart : MAX_ACCESS Access
        | ACCESS Access"""
        p[0] = ("MaxAccessPart", p[2])

    def p_notificationTypeClause(self, p):
        """notificationTypeClause : LOWERCASE_IDENTIFIER NOTIFICATION_TYPE NotificationObjectsPart STATUS Status DESCRIPTION Text ReferPart COLON_COLON_EQUAL '{' NotificationName '}'"""
        p[0] = (
            "notificationTypeClause",
            p[1],  # id
            #  p[2], # NOTIFICATION_TYPE
            p[3],  # NotificationObjectsPart
            p[5],  # status
            (p[6], p[7]),  # description
            p[8],  # reference
            p[11],
        )  # NotificationName aka objectIdentifier

    def p_moduleIdentityClause(self, p):
        """moduleIdentityClause : LOWERCASE_IDENTIFIER MODULE_IDENTITY SubjectCategoriesPart LAST_UPDATED ExtUTCTime ORGANIZATION Text CONTACT_INFO Text DESCRIPTION Text RevisionPart COLON_COLON_EQUAL '{' objectIdentifier '}'"""
        p[0] = (
            "moduleIdentityClause",
            p[1],  # id
            #  p[2], # MODULE_IDENTITY
            # XXX  p[3], # SubjectCategoriesPart
            (p[4], p[5]),  # last updated
            (p[6], p[7]),  # organization
            (p[8], p[9]),  # contact info
            (p[10], p[11]),  # description
            p[12],  # RevisionPart
            p[15],
        )  # objectIdentifier

    # Subject categories: RFC3159

    def p_SubjectCategoriesPart(self, p):
        """SubjectCategoriesPart : SUBJECT_CATEGORIES '{' SubjectCategories '}'
        | empty"""
        # if p[1]:
        #  p[0] = (p[1], p[3])

    def p_SubjectCategories(self, p):
        """SubjectCategories : CategoryIDs"""
        # p[0] = p[1]

    def p_CategoryIDs(self, p):
        """CategoryIDs : CategoryIDs ',' CategoryID
        | CategoryID"""
        # n = len(p)
        # if n == 4:
        #  p[0] = ('CategoryIDs', p[1][1] + [p[3]])
        # elif n == 2:
        #  p[0] = ('CategoryIDs', [p[1]])

    def p_CategoryID(self, p):
        """CategoryID : LOWERCASE_IDENTIFIER '(' NUMBER ')'
        | LOWERCASE_IDENTIFIER"""
        # n = len(p)
        # if n == 2:
        #  p[0] = ('CategoryID', p[1])
        # elif n == 5:
        #  p[0] = ('CategoryID', p[3])

    # ...subject categories

    def p_ObjectSyntax(self, p):
        """ObjectSyntax : SimpleSyntax
        | conceptualTable
        | row
        | entryType
        | ApplicationSyntax
        | typeTag SimpleSyntax"""
        n = len(p)
        if n == 2:
            p[0] = p[1]
        elif n == 3:
            p[0] = p[2]

    def p_typeTag(self, p):
        """typeTag : '[' APPLICATION NUMBER ']' IMPLICIT
        | '[' UNIVERSAL NUMBER ']' IMPLICIT"""

    def p_sequenceObjectSyntax(self, p):
        """sequenceObjectSyntax : sequenceSimpleSyntax
        | sequenceApplicationSyntax"""
        # libsmi: TO DO: add to this rule conceptualTable, row, entryType
        p[0] = p[1]

    def p_valueofObjectSyntax(self, p):
        """valueofObjectSyntax : valueofSimpleSyntax"""
        p[0] = p[1]

    def p_SimpleSyntax(self, p):
        """SimpleSyntax : INTEGER
        | INTEGER integerSubType
        | INTEGER enumSpec
        | INTEGER32
        | INTEGER32 integerSubType
        | UPPERCASE_IDENTIFIER enumSpec
        | UPPERCASE_IDENTIFIER integerSubType
        | OCTET STRING
        | OCTET STRING octetStringSubType
        | UPPERCASE_IDENTIFIER octetStringSubType
        | OBJECT IDENTIFIER anySubType"""
        n = len(p)
        if n == 2:
            p[0] = ("SimpleSyntax", p[1])

        elif n == 3:
            if p[1] == "OCTET":
                p[0] = ("SimpleSyntax", p[1] + " " + p[2])
            else:
                p[0] = ("SimpleSyntax", p[1], p[2])

        elif n == 4:
            p[0] = ("SimpleSyntax", p[1] + " " + p[2], p[3])

    def p_valueofSimpleSyntax(self, p):
        """valueofSimpleSyntax : NUMBER
        | NEGATIVENUMBER
        | NUMBER64
        | NEGATIVENUMBER64
        | HEX_STRING
        | BIN_STRING
        | LOWERCASE_IDENTIFIER
        | QUOTED_STRING
        | '{' objectIdentifier_defval '}'"""
        # libsmi for objectIdentifier_defval:
        # This is only for some MIBs with invalid numerical
        # OID notation for DEFVALs. We DO NOT parse them
        # correctly. We just don't want to produce a
        # parser error.
        n = len(p)
        if n == 2:
            p[0] = p[1]
        elif n == 4:  # XXX
            pass

    def p_sequenceSimpleSyntax(self, p):
        """sequenceSimpleSyntax : INTEGER anySubType
        | INTEGER32 anySubType
        | OCTET STRING anySubType
        | OBJECT IDENTIFIER anySubType"""
        n = len(p)
        if n == 3:
            p[0] = p[1]  # XXX not supporting subtypes here
        elif n == 4:
            p[0] = p[1] + " " + p[2]  # XXX not supporting subtypes here

    def p_ApplicationSyntax(self, p):
        """ApplicationSyntax : IPADDRESS anySubType
        | COUNTER32
        | COUNTER32 integerSubType
        | GAUGE32
        | GAUGE32 integerSubType
        | UNSIGNED32
        | UNSIGNED32 integerSubType
        | TIMETICKS anySubType
        | OPAQUE
        | OPAQUE octetStringSubType
        | COUNTER64
        | COUNTER64 integerSubType"""
        # COUNTER32 and COUNTER64 was with anySubType in libsmi
        n = len(p)
        if n == 2:
            p[0] = ("ApplicationSyntax", p[1])
        elif n == 3:
            p[0] = ("ApplicationSyntax", p[1], p[2])

    def p_sequenceApplicationSyntax(self, p):
        """sequenceApplicationSyntax : IPADDRESS anySubType
        | COUNTER32 anySubType
        | GAUGE32 anySubType
        | UNSIGNED32 anySubType
        | TIMETICKS anySubType
        | OPAQUE
        | COUNTER64 anySubType"""
        n = len(p)
        if n == 2:
            p[0] = p[1]
        elif n == 3:
            p[0] = p[1]  # XXX not supporting subtypes here

    def p_anySubType(self, p):
        """anySubType : integerSubType
        | octetStringSubType
        | enumSpec
        | empty"""
        if p[1]:
            p[0] = p[1]

    def p_integerSubType(self, p):
        """integerSubType : '(' ranges ')'"""
        p[0] = ("integerSubType", p[2])

    def p_octetStringSubType(self, p):
        """octetStringSubType : '(' SIZE '(' ranges ')' ')'"""
        p[0] = ("octetStringSubType", p[4])

    def p_ranges(self, p):
        """ranges : ranges '|' range
        | range"""
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]

    def p_range(self, p):
        """range : value DOT_DOT value
        | value"""
        n = len(p)
        if n == 2:
            p[0] = (p[1],)
        elif n == 4:
            p[0] = (p[1], p[3])

    def p_value(self, p):
        """value : NEGATIVENUMBER
        | NUMBER
        | NEGATIVENUMBER64
        | NUMBER64
        | HEX_STRING
        | BIN_STRING"""
        p[0] = p[1]

    def p_enumSpec(self, p):
        """enumSpec : '{' enumItems '}'"""
        p[0] = ("enumSpec", p[2])

    def p_enumItems(self, p):
        """enumItems : enumItems ',' enumItem
        | enumItem"""
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]

    def p_enumItem(self, p):
        """enumItem : LOWERCASE_IDENTIFIER '(' enumNumber ')'"""
        p[0] = (p[1], p[3])

    def p_enumNumber(self, p):
        """enumNumber : NUMBER
        | NEGATIVENUMBER"""
        # XXX              | LOWERCASE_IDENTIFIER"""
        p[0] = p[1]

    def p_Status(self, p):
        """Status : LOWERCASE_IDENTIFIER"""
        p[0] = ("Status", p[1])

    def p_DisplayPart(self, p):
        """DisplayPart : DISPLAY_HINT Text
        | empty"""
        if p[1]:
            p[0] = (p[1], p[2])

    def p_UnitsPart(self, p):
        """UnitsPart : UNITS Text
        | empty"""
        if p[1]:
            p[0] = (p[1], p[2])

    def p_Access(self, p):
        """Access : LOWERCASE_IDENTIFIER"""
        p[0] = p[1]

    def p_IndexPart(self, p):
        """IndexPart : AUGMENTS '{' Entry '}'
        | empty"""
        if p[1]:
            p[0] = p[3]

    def p_MibIndex(self, p):
        """MibIndex : INDEX '{' IndexTypes '}'
        | empty"""
        if p[1]:
            p[0] = (p[1], p[3])

    def p_IndexTypes(self, p):
        """IndexTypes : IndexTypes ',' IndexType
        | IndexType"""
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]

    def p_IndexType(self, p):
        """IndexType : IMPLIED Index
        | Index"""
        n = len(p)
        if n == 2:
            p[0] = (0, p[1])
        elif n == 3:
            p[0] = (1, p[2])  # IMPLIED

    def p_Index(self, p):
        """Index : ObjectName"""
        # libsmi: TODO: use the SYNTAX value of the correspondent
        #               OBJECT-TYPE invocation
        p[0] = p[1][1][0]  # XXX just name???

    def p_Entry(self, p):
        """Entry : ObjectName"""
        p[0] = p[1][1][0]

    def p_DefValPart(self, p):
        """DefValPart : DEFVAL '{' Value '}'
        | empty"""
        if p[1] and p[3] is not None:
            p[0] = (p[1], p[3])

    def p_Value(self, p):
        """Value : valueofObjectSyntax
        | '{' BitsValue '}'"""
        n = len(p)
        if n == 2:
            p[0] = p[1]
        elif n == 4:
            p[0] = p[2]

    def p_BitsValue(self, p):
        """BitsValue : BitNames
        | empty"""
        p[0] = p[1] or []

    def p_BitNames(self, p):
        """BitNames : BitNames ',' LOWERCASE_IDENTIFIER
        | LOWERCASE_IDENTIFIER"""
        n = len(p)
        if n == 4:
            p[0] = ("BitNames", p[1][1] + [p[3]])
        elif n == 2:
            p[0] = ("BitNames", [p[1]])

    def p_ObjectName(self, p):
        """ObjectName : objectIdentifier"""
        p[0] = p[1]

    def p_NotificationName(self, p):
        """NotificationName : objectIdentifier"""
        p[0] = p[1]

    def p_ReferPart(self, p):
        """ReferPart : REFERENCE Text
        | empty"""
        if p[1]:
            p[0] = (p[1], p[2])

    def p_RevisionPart(self, p):
        """RevisionPart : Revisions
        | empty"""
        if p[1]:
            p[0] = p[1]

    def p_Revisions(self, p):
        """Revisions : Revisions Revision
        | Revision"""
        n = len(p)
        if n == 3:
            p[0] = ("Revisions", p[1][1] + [p[2]])
        elif n == 2:
            p[0] = ("Revisions", [p[1]])

    def p_Revision(self, p):
        """Revision : REVISION ExtUTCTime DESCRIPTION Text"""
        p[0] = (p[2], (p[3], p[4]))  # revision time  # description

    def p_NotificationObjectsPart(self, p):
        """NotificationObjectsPart : OBJECTS '{' Objects '}'
        | empty"""
        p[0] = p[1] and p[3] or []

    def p_ObjectGroupObjectsPart(self, p):
        """ObjectGroupObjectsPart : OBJECTS '{' Objects '}'"""
        p[0] = p[3]

    def p_Objects(self, p):
        """Objects : Objects ',' Object
        | Object"""
        n = len(p)
        if n == 4:
            p[0] = ("Objects", p[1][1] + [p[3]])
        elif n == 2:
            p[0] = ("Objects", [p[1]])

    def p_Object(self, p):
        """Object : ObjectName"""
        p[0] = p[1][1][0]

    def p_NotificationsPart(self, p):
        """NotificationsPart : NOTIFICATIONS '{' Notifications '}'"""
        p[0] = p[3]

    def p_Notifications(self, p):
        """Notifications : Notifications ',' Notification
        | Notification"""
        n = len(p)
        if n == 4:
            p[0] = ("Notifications", p[1][1] + [p[3]])
        elif n == 2:
            p[0] = ("Notifications", [p[1]])

    def p_Notification(self, p):
        """Notification : NotificationName"""
        p[0] = p[1][1][0]

    def p_Text(self, p):
        """Text : QUOTED_STRING"""
        p[0] = p[1][1:-1]  # getting rid of quotes

    def p_ExtUTCTime(self, p):
        """ExtUTCTime : QUOTED_STRING"""
        p[0] = p[1][1:-1]  # getting rid of quotes

    def p_objectIdentifier(self, p):
        """objectIdentifier : subidentifiers"""
        p[0] = ("objectIdentifier", p[1])

    def p_subidentifiers(self, p):
        """subidentifiers : subidentifiers subidentifier
        | subidentifier"""
        n = len(p)
        if n == 3:
            p[0] = p[1] + [p[2]]
        elif n == 2:
            p[0] = [p[1]]

    def p_subidentifier(self, p):
        """subidentifier : fuzzy_lowercase_identifier
        | NUMBER
        | LOWERCASE_IDENTIFIER '(' NUMBER ')'"""
        n = len(p)
        if n == 2:
            p[0] = p[1]
        elif n == 5:
            # NOTE: we are not creating new symbol p[1] because formally
            # it is not defined in *this* MIB
            p[0] = (p[1], p[3])

    def p_objectIdentifier_defval(self, p):
        """objectIdentifier_defval : subidentifiers_defval"""
        p[0] = ("objectIdentifier_defval", p[1])

    def p_subidentifiers_defval(self, p):
        """subidentifiers_defval : subidentifiers_defval subidentifier_defval
        | subidentifier_defval"""
        n = len(p)
        if n == 3:
            p[0] = ("subidentifiers_defval", p[1][1] + [p[2]])
        elif n == 2:
            p[0] = ("subidentifiers_defval", [p[1]])

    def p_subidentifier_defval(self, p):
        """subidentifier_defval : LOWERCASE_IDENTIFIER '(' NUMBER ')'
        | NUMBER"""
        n = len(p)
        if n == 2:
            p[0] = ("subidentifier_defval", p[1])
        elif n == 5:
            p[0] = ("subidentifier_defval", p[1], p[3])

    def p_objectGroupClause(self, p):
        """objectGroupClause : LOWERCASE_IDENTIFIER OBJECT_GROUP ObjectGroupObjectsPart STATUS Status DESCRIPTION Text ReferPart COLON_COLON_EQUAL '{' objectIdentifier '}'"""
        p[0] = (
            "objectGroupClause",
            p[1],  # id
            p[3],  # objects
            p[5],  # status
            (p[6], p[7]),  # description
            p[8],  # reference
            p[11],
        )  # objectIdentifier

    def p_notificationGroupClause(self, p):
        """notificationGroupClause : LOWERCASE_IDENTIFIER NOTIFICATION_GROUP NotificationsPart STATUS Status DESCRIPTION Text ReferPart COLON_COLON_EQUAL '{' objectIdentifier '}'"""
        p[0] = (
            "notificationGroupClause",
            p[1],  # id
            p[3],  # notifications
            p[5],  # status
            (p[6], p[7]),  # description
            p[8],  # reference
            p[11],
        )  # objectIdentifier

    def p_moduleComplianceClause(self, p):
        """moduleComplianceClause : LOWERCASE_IDENTIFIER MODULE_COMPLIANCE STATUS Status DESCRIPTION Text ReferPart ComplianceModulePart COLON_COLON_EQUAL '{' objectIdentifier '}'"""
        p[0] = (
            "moduleComplianceClause",
            p[1],  # id
            #  p[2], # MODULE_COMPLIANCE
            p[4],  # status
            (p[5], p[6]),  # description
            p[7],  # reference
            p[8],  # ComplianceModules
            p[11],
        )  # objectIdentifier

    def p_ComplianceModulePart(self, p):
        """ComplianceModulePart : ComplianceModules"""
        p[0] = p[1]

    def p_ComplianceModules(self, p):
        """ComplianceModules : ComplianceModules ComplianceModule
        | ComplianceModule"""
        n = len(p)
        if n == 3:
            p[0] = ("ComplianceModules", p[1][1] + [p[2]])
        elif n == 2:
            p[0] = ("ComplianceModules", [p[1]])

    def p_ComplianceModule(self, p):
        """ComplianceModule : MODULE ComplianceModuleName MandatoryPart CompliancePart"""
        objects = p[3] and p[3][1] or []
        objects += p[4] and p[4][1] or []
        p[0] = (p[2], objects)  # ModuleName  # MandatoryPart + CompliancePart

    def p_ComplianceModuleName(self, p):
        """ComplianceModuleName : UPPERCASE_IDENTIFIER
        | empty"""
        # XXX                   | UPPERCASE_IDENTIFIER objectIdentifier
        p[0] = p[1]

    def p_MandatoryPart(self, p):
        """MandatoryPart : MANDATORY_GROUPS '{' MandatoryGroups '}'
        | empty"""
        if p[1]:
            p[0] = p[3]

    def p_MandatoryGroups(self, p):
        """MandatoryGroups : MandatoryGroups ',' MandatoryGroup
        | MandatoryGroup"""
        n = len(p)
        if n == 4:
            p[0] = ("MandatoryGroups", p[1][1] + [p[3]])
        elif n == 2:
            p[0] = ("MandatoryGroups", [p[1]])

    def p_MandatoryGroup(self, p):
        """MandatoryGroup : objectIdentifier"""
        p[0] = p[1][1][0]  # objectIdentifier? Maybe name?

    def p_CompliancePart(self, p):
        """CompliancePart : Compliances
        | empty"""
        if p[1]:
            p[0] = p[1]

    def p_Compliances(self, p):
        """Compliances : Compliances Compliance
        | Compliance"""
        n = len(p)
        if n == 3:
            p[0] = p[1] and p[2] and ("Compliances", p[1][1] + [p[2]]) or p[1]
        elif n == 2:
            p[0] = p[1] and ("Compliances", [p[1]]) or None

    def p_Compliance(self, p):
        """Compliance : ComplianceGroup
        | ComplianceObject"""
        if p[1]:
            p[0] = p[1]

    def p_ComplianceGroup(self, p):
        """ComplianceGroup : GROUP objectIdentifier DESCRIPTION Text"""
        p[0] = p[2][1][0]  # objectIdentifier
        #        p[1], # GROUP
        #        (p[3], p[4])) # description

    def p_ComplianceObject(self, p):
        """ComplianceObject : OBJECT ObjectName SyntaxPart WriteSyntaxPart AccessPart DESCRIPTION Text"""
        # p[0] = (p[1], # object
        #        p[2], # name
        #        p[3], # syntax
        #        p[4], # write syntax
        #        p[5], # access
        #        (p[6], p[7])) # description

    def p_SyntaxPart(self, p):
        """SyntaxPart : SYNTAX Syntax
        | empty"""
        if p[1]:
            p[0] = p[2]

    def p_WriteSyntaxPart(self, p):
        """WriteSyntaxPart : WRITE_SYNTAX WriteSyntax
        | empty"""
        if p[1]:
            p[0] = p[2]

    def p_WriteSyntax(self, p):
        """WriteSyntax : Syntax"""
        p[0] = ("WriteSyntax", p[1])

    def p_AccessPart(self, p):
        """AccessPart : MIN_ACCESS Access
        | empty"""
        if p[1]:
            p[0] = (p[1], p[2])

    def p_agentCapabilitiesClause(self, p):
        """agentCapabilitiesClause : LOWERCASE_IDENTIFIER AGENT_CAPABILITIES PRODUCT_RELEASE Text STATUS Status DESCRIPTION Text ReferPart ModulePart_Capabilities COLON_COLON_EQUAL '{' objectIdentifier '}'"""
        p[0] = (
            "agentCapabilitiesClause",
            p[1],  # id
            #   p[2], # AGENT_CAPABILITIES
            (p[3], p[4]),  # product release
            p[6],  # status
            (p[7], p[8]),  # description
            p[9],  # reference
            #   p[10], # module capabilities
            p[13],
        )  # objectIdentifier

    def p_ModulePart_Capabilities(self, p):
        """ModulePart_Capabilities : Modules_Capabilities
        | empty"""
        # if p[1]:
        #  p[0] = p[1]

    def p_Modules_Capabilities(self, p):
        """Modules_Capabilities : Modules_Capabilities Module_Capabilities
        | Module_Capabilities"""
        # n = len(p)
        # if n == 3:
        #  p[0] = ('Modules_Capabilities', p[1][1] + [p[2]])
        # elif n == 2:
        #  p[0] = ('Modules_Capabilities', [p[1]])

    def p_Module_Capabilities(self, p):
        """Module_Capabilities : SUPPORTS ModuleName_Capabilities INCLUDES '{' CapabilitiesGroups '}' VariationPart"""
        # p[0] = ('Module_Capabilities', (p[1], p[2]), # supports
        #                               (p[3], p[5]), # includes
        #                               p[7]) # variations

    def p_CapabilitiesGroups(self, p):
        """CapabilitiesGroups : CapabilitiesGroups ',' CapabilitiesGroup
        | CapabilitiesGroup"""
        # n = len(p)
        # if n == 4:
        #  p[0] = ('CapabilitiesGroups', p[1][1] + [p[3]])
        # elif n == 2:
        #  p[0] = ('CapabilitiesGroups', [p[1]])

    def p_CapabilitiesGroup(self, p):
        """CapabilitiesGroup : objectIdentifier"""
        # p[0] = ('CapabilitiesGroup', p[1])

    def p_ModuleName_Capabilities(self, p):
        """ModuleName_Capabilities : UPPERCASE_IDENTIFIER objectIdentifier
        | UPPERCASE_IDENTIFIER"""
        # n = len(p)
        # if n == 2:
        #  p[0] = ('ModuleName_Capabilities', p[1])
        # elif n == 3:
        #  p[0] = ('ModuleName_Capabilities', p[1], p[2])

    def p_VariationPart(self, p):
        """VariationPart : Variations
        | empty"""
        # if p[1]:
        #  p[0] = p[1]

    def p_Variations(self, p):
        """Variations : Variations Variation
        | Variation"""
        # n = len(p)
        # if n == 3:
        #  p[0] = ('Variations', p[1][1] + [p[2]])
        # elif n == 2:
        #  p[0] = ('Variations', [p[1]])        pass

    def p_Variation(self, p):
        """Variation : VARIATION ObjectName SyntaxPart WriteSyntaxPart VariationAccessPart CreationPart DefValPart DESCRIPTION Text"""
        # p[0] = (p[1], # variation
        #        p[2], # name
        #        p[3], # syntax
        #        p[4], # write syntax
        #        p[5], # access
        #        p[6], # creation
        #        p[7], # defval
        #        (p[8], p[9])) # description

    def p_VariationAccessPart(self, p):
        """VariationAccessPart : ACCESS VariationAccess
        | empty"""
        # if p[1]:
        #  p[0] = (p[1], p[2])

    def p_VariationAccess(self, p):
        """VariationAccess : LOWERCASE_IDENTIFIER"""
        # p[0] = p[1]

    def p_CreationPart(self, p):
        """CreationPart : CREATION_REQUIRES '{' Cells '}'
        | empty"""
        if p[1]:
            p[0] = (p[1], p[3])

    def p_Cells(self, p):
        """Cells : Cells ',' Cell
        | Cell"""
        n = len(p)
        if n == 4:
            p[0] = ("Cells", p[1][1] + [p[3]])
        elif n == 2:
            p[0] = ("Cells", [p[1]])

    def p_Cell(self, p):
        """Cell : ObjectName"""
        p[0] = ("Cell", p[1])

    def p_empty(self, p):
        """empty :"""

    # Error rule for syntax errors
    def p_error(self, p):
        if p:
            raise error.PySmiParserError(
                f"Bad grammar near token type {p.type}, value {p.value}",
                lineno=p.lineno,
            )


#
# Parser grammar relaxation follows.
#
# The classes that follow serve a purpose of encapsulating assorted functions
# into a namespace. The namespace type is not universally supported across all
# Python versions we want to run on, thus the hack with `staticmethod` decorator
# and `self` first parameter.
#

#
# SMIv1 grammar
#


# noinspection PyIncorrectDocstring
class SupportSmiV1Keywords:
    # NETWORKADDRESS added
    @staticmethod
    def p_importedKeyword(self, p):
        """importedKeyword : importedSMIKeyword
        | BITS
        | INTEGER32
        | IPADDRESS
        | NETWORKADDRESS
        | MANDATORY_GROUPS
        | MODULE_COMPLIANCE
        | MODULE_IDENTITY
        | OBJECT_GROUP
        | OBJECT_IDENTITY
        | OBJECT_TYPE
        | OPAQUE
        | TEXTUAL_CONVENTION
        | TIMETICKS
        | UNSIGNED32"""
        p[0] = p[1]

    # NETWORKADDRESS added
    @staticmethod
    def p_typeSMIandSPPI(self, p):
        """typeSMIandSPPI : IPADDRESS
        | NETWORKADDRESS
        | TIMETICKS
        | OPAQUE
        | INTEGER32
        | UNSIGNED32"""
        p[0] = p[1]

    # NETWORKADDRESS added
    @staticmethod
    def p_ApplicationSyntax(self, p):
        """ApplicationSyntax : IPADDRESS anySubType
        | NETWORKADDRESS anySubType
        | COUNTER32
        | COUNTER32 integerSubType
        | GAUGE32
        | GAUGE32 integerSubType
        | UNSIGNED32
        | UNSIGNED32 integerSubType
        | TIMETICKS anySubType
        | OPAQUE
        | OPAQUE octetStringSubType
        | COUNTER64
        | COUNTER64 integerSubType"""
        n = len(p)
        if n == 2:
            p[0] = ("ApplicationSyntax", p[1])
        elif n == 3:
            p[0] = ("ApplicationSyntax", p[1], p[2])

    # NETWORKADDRESS added for SEQUENCE syntax
    @staticmethod
    def p_sequenceApplicationSyntax(self, p):
        """sequenceApplicationSyntax : IPADDRESS anySubType
        | NETWORKADDRESS anySubType
        | COUNTER32 anySubType
        | GAUGE32 anySubType
        | UNSIGNED32 anySubType
        | TIMETICKS anySubType
        | OPAQUE
        | COUNTER64 anySubType"""
        n = len(p)
        if n == 2:
            p[0] = p[1]
        elif n == 3:
            p[0] = p[1]  # XXX not supporting subtypes here


# noinspection PyIncorrectDocstring
class SupportIndex:
    # SMIv1 IndexTypes added
    @staticmethod
    def p_Index(self, p):
        """Index : ObjectName
        | typeSMIv1"""

        # libsmi: TODO: use the SYNTAX value of the correspondent
        #               OBJECT-TYPE invocation
        p[0] = isinstance(p[1], tuple) and p[1][1][0] or p[1]

    # for Index rule
    @staticmethod
    def p_typeSMIv1(self, p):
        """typeSMIv1 : INTEGER
        | OCTET STRING
        | IPADDRESS
        | NETWORKADDRESS"""
        n = len(p)
        indextype = n == 3 and p[1] + " " + p[2] or p[1]
        p[0] = indextype


#
# Some changes in grammar to handle common mistakes in MIBs
#


# noinspection PyIncorrectDocstring
class CommaInImport:
    # comma at the end of import list
    @staticmethod
    def p_importIdentifiers(self, p):
        """importIdentifiers : importIdentifiers ',' importIdentifier
        | importIdentifier
        | importIdentifiers ','"""
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]
        elif n == 3:  # excessive comma case
            p[0] = p[1]


# noinspection PyIncorrectDocstring
class CommaInSequence:
    # comma at the end of sequence list
    @staticmethod
    def p_sequenceItems(self, p):
        """sequenceItems : sequenceItems ',' sequenceItem
        | sequenceItem
        | sequenceItems ','"""
        # libsmi: TODO: might this list be emtpy?
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]
        elif n == 3:  # excessive comma case
            p[0] = p[1]


# noinspection PyIncorrectDocstring
class CommaAndSpaces:
    # common typos handled (mix of commas and spaces)
    @staticmethod
    def p_enumItems(self, p):
        """enumItems : enumItems ',' enumItem
        | enumItem
        | enumItems enumItem
        | enumItems ','"""
        n = len(p)
        if n == 4:
            p[0] = p[1] + [p[3]]
        elif n == 2:
            p[0] = [p[1]]
        elif n == 3:  # typo case
            if p[2] == ",":
                p[0] = p[1]
            else:
                p[0] = p[1] + [p[2]]


# noinspection PyIncorrectDocstring
class UppercaseIdentifier:
    # common mistake - using UPPERCASE_IDENTIFIER
    @staticmethod
    def p_enumItem(self, p):
        """enumItem : LOWERCASE_IDENTIFIER '(' enumNumber ')'
        | UPPERCASE_IDENTIFIER '(' enumNumber ')'"""
        p[0] = (p[1], p[3])


# noinspection PyIncorrectDocstring
class LowcaseIdentifier:
    # common mistake - LOWERCASE_IDENTIFIER in symbol's name
    @staticmethod
    def p_notificationTypeClause(self, p):
        """notificationTypeClause : fuzzy_lowercase_identifier NOTIFICATION_TYPE NotificationObjectsPart STATUS Status DESCRIPTION Text ReferPart COLON_COLON_EQUAL '{' NotificationName '}'"""  # some MIBs have uppercase and/or lowercase id
        p[0] = (
            "notificationTypeClause",
            p[1],  # id
            #  p[2], # NOTIFICATION_TYPE
            p[3],  # NotificationObjectsPart
            p[5],  # status
            (p[6], p[7]),  # description
            p[8],  # Reference
            p[11],
        )  # NotificationName aka objectIdentifier


# noinspection PyIncorrectDocstring,PyIncorrectDocstring
class CurlyBracesInEnterprises:
    # common mistake - curly brackets around enterprise symbol
    @staticmethod
    def p_trapTypeClause(self, p):
        """trapTypeClause : fuzzy_lowercase_identifier TRAP_TYPE EnterprisePart VarPart DescrPart ReferPart COLON_COLON_EQUAL NUMBER"""
        # libsmi: TODO: range of number?
        p[0] = (
            "trapTypeClause",
            p[1],  # fuzzy_lowercase_identifier
            #  p[2], # TRAP_TYPE
            p[3],  # EnterprisePart (objectIdentifier)
            p[4],  # VarPart
            p[5],  # description
            p[6],  # reference
            p[8],
        )  # NUMBER

    @staticmethod
    def p_EnterprisePart(self, p):
        """EnterprisePart : ENTERPRISE objectIdentifier
        | ENTERPRISE '{' objectIdentifier '}'"""
        n = len(p)
        if n == 3:
            p[0] = p[2]
        elif n == 5:  # common mistake case
            p[0] = p[3]


# noinspection PyIncorrectDocstring
class NoCells:
    # common mistake - no Cells
    @staticmethod
    def p_CreationPart(self, p):
        """CreationPart : CREATION_REQUIRES '{' Cells '}'
        | CREATION_REQUIRES '{' '}'
        | empty"""
        n = len(p)
        if n == 5:
            p[0] = (p[1], p[3])


relaxedGrammar = {
    "supportSmiV1Keywords": [
        SupportSmiV1Keywords.p_importedKeyword,
        SupportSmiV1Keywords.p_typeSMIandSPPI,
        SupportSmiV1Keywords.p_ApplicationSyntax,
        SupportSmiV1Keywords.p_sequenceApplicationSyntax,
    ],
    "supportIndex": [SupportIndex.p_Index, SupportIndex.p_typeSMIv1],
    "commaAtTheEndOfImport": [CommaInImport.p_importIdentifiers],
    "commaAtTheEndOfSequence": [CommaInSequence.p_sequenceItems],
    "mixOfCommasAndSpaces": [CommaAndSpaces.p_enumItems],
    "uppercaseIdentifier": [UppercaseIdentifier.p_enumItem],
    "lowcaseIdentifier": [LowcaseIdentifier.p_notificationTypeClause],
    "curlyBracesAroundEnterpriseInTrap": [
        CurlyBracesInEnterprises.p_trapTypeClause,
        CurlyBracesInEnterprises.p_EnterprisePart,
    ],
    "noCells": [NoCells.p_CreationPart],
}


def parserFactory(**grammarOptions):
    """Factory function producing custom specializations of base *SmiV2Parser*
    class.

    Keyword Args:
        grammarOptions: a list of (bool) typed optional keyword parameters
                        enabling particular set of SMIv2 grammar relaxations.

    Returns:
        Specialized copy of *SmiV2Parser* class.

    Notes:
        The following SMIv2 grammar relaxation parameters are defined:

        * supportSmiV1Keywords - parses SMIv1 grammar
        * supportIndex - tolerates ASN.1 types in INDEX clause
        * commaAtTheEndOfImport - tolerates stray comma at the end of IMPORT section
        * commaAtTheEndOfSequence - tolerates stray comma at the end of sequence of elements in MIB
        * mixOfCommasAndSpaces - tolerate a mix of comma and spaces in MIB enumerations
        * uppercaseIdentifier - tolerate uppercased MIB identifiers
        * lowcaseIdentifier - tolerate lowercase MIB identifiers
        * curlyBracesAroundEnterpriseInTrap - tolerate curly braces around enterprise ID in TRAP MACRO
        * noCells - tolerate missing cells (XXX)

    Examples:

    >>> from pysmi.parser import smi
    >>> SmiV1Parser = smi.parserFactory(supportSmiV1Keywords=True, supportIndex=True)

    """
    classAttr = {}

    for option in grammarOptions:
        if grammarOptions[option]:
            if option not in relaxedGrammar:
                raise error.PySmiError(f"Unknown parser relaxation option: {option}")

            for func in relaxedGrammar[option]:
                classAttr[func.__name__] = func

    classAttr["defaultLexer"] = lexerFactory(**grammarOptions)

    return type("SmiParser", (SmiV2Parser,), classAttr)
