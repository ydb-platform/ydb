#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# ASN.1 source file://asn1/SNMPv2-SMI
# Produced by pysmi-1.5.8 at Sat Nov  2 15:25:23 2024
# On host MacBook-Pro.local platform Darwin version 24.1.0 by user lextm
# Using Python version 3.12.0 (main, Nov 14 2023, 23:52:11) [Clang 15.0.0 (clang-1500.0.40.1)]
#
# IMPORTANT: this contains customizations
import sys
import traceback

from pyasn1.error import PyAsn1Error
from pyasn1.type import namedtype, univ

from pysnmp import cache, debug
from pysnmp.proto import rfc1902
from pysnmp.smi import error, exval
from pysnmp.smi.indices import OidOrderedDict

# IMPORTANT: customizations
# syntax of objects

ObjectIdentifier = rfc1902.ObjectIdentifier
OctetString = rfc1902.OctetString
Bits = rfc1902.Bits
Integer32 = rfc1902.Integer32
IpAddress = rfc1902.IpAddress
Counter32 = rfc1902.Counter32
Gauge32 = rfc1902.Gauge32
Unsigned32 = rfc1902.Unsigned32
TimeTicks = rfc1902.TimeTicks
Opaque = rfc1902.Opaque
Counter64 = rfc1902.Counter64


# MIB tree foundation class


class MibNode:
    label = ""

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r})"

    def getName(self):
        return self.name

    def getLabel(self):
        return self.label

    def setLabel(self, label):
        self.label = label
        return self

    def clone(self, name=None):
        myClone = self.__class__(self.name)
        if name is not None:
            myClone.name = name
        if self.label is not None:
            myClone.label = self.label
        return myClone


# definitions for information modules


class ModuleIdentity(MibNode):
    status = "current"
    lastUpdated = ""
    organization = ""
    contactInfo = ""
    description = ""
    revisions = ()
    revisionsDescriptions = ()

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getLastUpdated(self):
        return self.lastUpdated

    def setLastUpdated(self, v):
        self.lastUpdated = v
        return self

    def getOrganization(self):
        return self.organization

    def setOrganization(self, v):
        self.organization = v
        return self

    def getContactInfo(self):
        return self.contactInfo

    def setContactInfo(self, v):
        self.contactInfo = v
        return self

    def getDescription(self):
        return self.description

    def setDescription(self, v):
        self.description = v
        return self

    def getRevisions(self):
        return self.revisions

    def setRevisions(self, args):
        self.revisions = args
        return self

    def getRevisionsDescriptions(self):
        return self.revisionsDescriptions

    def setRevisionsDescriptions(self, args):
        self.revisionsDescriptions = args
        return self

    def asn1Print(self):
        return """\
MODULE-IDENTITY
  LAST-UPDATED {}
  ORGANIZATION "{}"
  CONTACT-INFO "{}"
  DESCRIPTION "{}"
  {}""".format(
            self.getLastUpdated(),
            self.getOrganization(),
            self.getContactInfo(),
            self.getDescription(),
            "".join(['REVISION "%s"\n' % x for x in self.getRevisions()]),
        )

    ## compatibility with legacy code
    def set_revisions(self, *args):
        return self.setRevisions(args)

    def set_last_updated(self, lastUpdated):
        self.setLastUpdated(lastUpdated)
        return self

    def set_organization(self, organization):
        self.setOrganization(organization)
        return self

    def set_contact_info(self, contactInfo):
        self.setContactInfo(contactInfo)
        return self

    def set_description(self, description):
        self.setDescription(description)
        return self


class ObjectIdentity(MibNode):
    status = "current"
    description = ""
    reference = ""

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getDescription(self):
        return self.description

    def setDescription(self, v):
        self.description = v
        return self

    def getReference(self):
        return self.reference

    def setReference(self, v):
        self.reference = v
        return self

    def asn1Print(self):
        return """\
OBJECT-IDENTITY
  STATUS {}
  DESCRIPTION "{}"
  REFERENCE "{}"
""".format(
            self.getStatus(), self.getDescription(), self.getReference()
        )

    ## compatibility with legacy code
    def set_status(self, status):
        self.setStatus(status)
        return self

    def set_description(self, description):
        self.setDescription(description)
        return self

    def set_reference(self, reference):
        self.setReference(reference)
        return self


# definition for objects


class NotificationType(MibNode):
    objects = ()
    status = "current"
    description = ""
    reference = ""

    def getObjects(self):
        return self.objects

    def setObjects(self, *args, **kwargs):
        if kwargs.get("append"):
            self.objects += args
        else:
            self.objects = args
        return self

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getDescription(self):
        return self.description

    def setDescription(self, v):
        self.description = v
        return self

    def getReference(self):
        return self.reference

    def setReference(self, v):
        self.reference = v
        return self

    def asn1Print(self):
        return """\
NOTIFICATION-TYPE
  OBJECTS {{ {} }}
  STATUS {}
  DESCRIPTION "{}"
  REFERENCE "{}"
""".format(
            ", ".join([x for x in self.getObjects()]),
            self.getStatus(),
            self.getDescription(),
            self.getReference(),
        )

    ## compatibility with legacy code
    def set_objects(self, *args, **kwargs):
        return self.setObjects(*args, **kwargs)

    def set_status(self, status):
        self.setStatus(status)
        return self

    def set_description(self, description):
        self.setDescription(description)
        return self


class MibIdentifier(MibNode):
    @staticmethod
    def asn1Print():
        return "OBJECT IDENTIFIER"


class ObjectType(MibNode):
    units = ""
    maxAccess = "not-accessible"
    status = "current"
    description = ""
    reference = ""

    def __init__(self, name, syntax=None):
        MibNode.__init__(self, name)
        self.syntax = syntax

    # XXX
    def __eq__(self, other):
        return self.syntax == other

    def __ne__(self, other):
        return self.syntax != other

    def __lt__(self, other):
        return self.syntax < other

    def __le__(self, other):
        return self.syntax <= other

    def __gt__(self, other):
        return self.syntax > other

    def __ge__(self, other):
        return self.syntax >= other

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r}, {self.syntax!r})"

    def getSyntax(self):
        return self.syntax

    def setSyntax(self, v):
        self.syntax = v
        return self

    def getUnits(self):
        return self.units

    def setUnits(self, v):
        self.units = v
        return self

    def getMaxAccess(self):
        return self.maxAccess

    def setMaxAccess(self, v):
        self.maxAccess = v
        return self

    def getStatus(self):
        return self.status

    def setStatus(self, v):
        self.status = v
        return self

    def getDescription(self):
        return self.description

    def setDescription(self, v):
        self.description = v
        return self

    def getReference(self):
        return self.reference

    def setReference(self, v):
        self.reference = v
        return self

    def asn1Print(self):
        return """
OBJECT-TYPE
  SYNTAX {}
  UNITS "{}"
  MAX-ACCESS {}
  STATUS {}
  DESCRIPTION "{}"
  REFERENCE "{}" """.format(
            self.getSyntax().__class__.__name__,
            self.getUnits(),
            self.getMaxAccess(),
            self.getStatus(),
            self.getDescription(),
            self.getReference(),
        )

    ## compatibility with legacy code
    def set_status(self, status):
        self.setStatus(status)
        return self

    def set_description(self, description):
        self.setDescription(description)
        return self

        ## compatibility with legacy code

    def set_max_access(self, access):
        self.setMaxAccess(access)
        return self

    def set_units(self, units):
        self.setUnits(units)
        return self

    def set_reference(self, reference):
        self.setReference(reference)
        return self


class MibTree(ObjectType):
    branchVersionId = 0  # cnanges on tree structure change
    maxAccess = "not-accessible"

    def __init__(self, name, syntax=None):
        ObjectType.__init__(self, name, syntax)
        self._vars = OidOrderedDict()

    # Subtrees registration

    def registerSubtrees(self, *subTrees):
        self.branchVersionId += 1
        for subTree in subTrees:
            if subTree.name in self._vars:
                raise error.SmiError(
                    f"MIB subtree {subTree.name} already registered at {self}"
                )
            self._vars[subTree.name] = subTree

    def unregisterSubtrees(self, *names):
        self.branchVersionId += 1
        for name in names:
            # This may fail if you fill a table by exporting MibScalarInstances
            # but later drop them through SNMP.
            if name not in self._vars:
                raise error.SmiError(f"MIB subtree {name} not registered at {self}")
            del self._vars[name]

    #
    # Tree traversal
    #
    # Missing branches are indicated by the NoSuchObjectError exception.
    # Although subtrees may indicate their missing branches by the
    # NoSuchInstanceError exception.
    #

    def getBranch(self, name, **context):
        """Return a branch of this tree where the 'name' OID may reside"""
        for keyLen in self._vars.get_keys_lengths():
            subName = name[:keyLen]
            if subName in self._vars:
                return self._vars[subName]

        raise error.NoSuchObjectError(name=name, idx=context.get("idx"))

    def getNextBranch(self, name, **context):
        # Start from the beginning
        if self._vars:
            first = list(self._vars.keys())[0]
        else:
            first = ()
        if self._vars and name < first:
            return self._vars[first]
        else:
            try:
                return self._vars[self._vars.next_key(name)]
            except KeyError:
                raise error.NoSuchObjectError(name=name, idx=context.get("idx"))

    def getNode(self, name, **context):
        """Return tree node found by name"""
        if name == self.name:
            return self
        else:
            return self.getBranch(name, **context).getNode(name, **context)

    def getNextNode(self, name, **context):
        """Return tree node next to name"""
        try:
            nextNode = self.getBranch(name, **context)
        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            return self.getNextBranch(name, **context)
        else:
            try:
                return nextNode.getNextNode(name, **context)
            except (error.NoSuchInstanceError, error.NoSuchObjectError):
                try:
                    return self._vars[self._vars.next_key(nextNode.name)]
                except KeyError:
                    raise error.NoSuchObjectError(name=name, idx=context.get("idx"))

    # MIB instrumentation

    # Read operation

    def readTest(self, varBind, **context):
        name, val = varBind

        if name == self.name:
            acFun = context.get("acFun")
            if acFun:
                if self.maxAccess not in (
                    "read-only",
                    "read-write",
                    "read-create",
                ) or acFun("read", (name, self.syntax), **context):
                    raise error.NoAccessError(name=name, idx=context.get("idx"))
        else:
            try:
                node = self.getBranch(name, **context)

            except (error.NoSuchInstanceError, error.NoSuchObjectError):
                return  # missing object is not an error here

            else:
                node.readTest(varBind, **context)

    def readGet(self, varBind, **context):
        name, val = varBind

        try:
            node = self.getBranch(name, **context)

        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            return name, exval.noSuchObject

        else:
            return node.readGet(varBind, **context)

    # Read next operation is subtree-specific

    depthFirst, breadthFirst = 0, 1

    def readTestNext(self, varBind, **context):
        name, val = varBind

        topOfTheMib = context.get("oName") is None
        if topOfTheMib:
            context["oName"] = name

        nextName = name
        direction = self.depthFirst

        while True:  # NOTE(etingof): linear search here
            if direction == self.depthFirst:
                direction = self.breadthFirst
                try:
                    node = self.getBranch(nextName, **context)

                except (error.NoSuchInstanceError, error.NoSuchObjectError):
                    continue

            else:
                try:
                    node = self.getNextBranch(nextName, **context)

                except (error.NoSuchInstanceError, error.NoSuchObjectError):
                    if topOfTheMib:
                        return
                    raise

                direction = self.depthFirst
                nextName = node.name

            try:
                return node.readTestNext(varBind, **context)

            except (
                error.NoAccessError,
                error.NoSuchInstanceError,
                error.NoSuchObjectError,
            ):
                pass

    def readGetNext(self, varBind, **context):
        name, val = varBind

        topOfTheMib = context.get("oName") is None
        if topOfTheMib:
            context["oName"] = name

        nextName = name
        direction = self.depthFirst

        while True:  # NOTE(etingof): linear search ahead!
            if direction == self.depthFirst:
                direction = self.breadthFirst
                try:
                    node = self.getBranch(nextName, **context)

                except (error.NoSuchInstanceError, error.NoSuchObjectError):
                    continue

            else:
                try:
                    node = self.getNextBranch(nextName, **context)

                except (error.NoSuchInstanceError, error.NoSuchObjectError):
                    if topOfTheMib:
                        return name, exval.endOfMib
                    raise

                direction = self.depthFirst
                nextName = node.name

            try:
                return node.readGetNext((nextName, val), **context)

            except (
                error.NoAccessError,
                error.NoSuchInstanceError,
                error.NoSuchObjectError,
            ):
                pass

    # Write operation

    def writeTest(self, varBind, **context):
        name, val = varBind

        if name == self.name:
            # Make sure variable is writable
            acFun = context.get("acFun")
            if acFun:
                if self.maxAccess not in ("read-write", "read-create") or acFun(
                    "write", (name, self.syntax), **context
                ):
                    raise error.NotWritableError(name=name, idx=context.get("idx"))
        else:
            node = self.getBranch(name, **context)
            node.writeTest(varBind, **context)

    def writeCommit(self, varBind, **context):
        name, val = varBind

        node = self.getBranch(name, **context)
        node.writeCommit(varBind, **context)

    def writeCleanup(self, varBind, **context):
        name, val = varBind

        self.branchVersionId += 1

        node = self.getBranch(name, **context)
        node.writeCleanup(varBind, **context)

    def writeUndo(self, varBind, **context):
        name, val = varBind

        node = self.getBranch(name, **context)
        node.writeUndo(varBind, **context)


class MibScalar(MibTree):
    """Scalar MIB variable. Implements access control checking."""

    maxAccess = "read-only"

    #
    # Subtree traversal
    #
    # Missing branches are indicated by the NoSuchInstanceError exception.
    #

    def getBranch(self, name, **context):
        try:
            return MibTree.getBranch(self, name, **context)

        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def getNextBranch(self, name, **context):
        try:
            return MibTree.getNextBranch(self, name, **context)

        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def getNode(self, name, **context):
        try:
            return MibTree.getNode(self, name, **context)

        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def getNextNode(self, name, **context):
        try:
            return MibTree.getNextNode(self, name, **context)

        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    # MIB instrumentation methods

    # Read operation

    def readTest(self, varBind, **context):
        name, val = varBind

        if name == self.name:
            raise error.NoAccessError(name=name, idx=context.get("idx"))

        acFun = context.get("acFun")
        if acFun:
            if self.maxAccess not in (
                "read-only",
                "read-write",
                "read-create",
            ) or acFun("read", (name, self.syntax), **context):
                raise error.NoAccessError(name=name, idx=context.get("idx"))

        MibTree.readTest(self, varBind, **context)

    def readGet(self, varBind, **context):
        name, val = varBind

        try:
            node = self.getBranch(name, **context)

        except error.NoSuchInstanceError:
            return name, exval.noSuchInstance

        else:
            return node.readGet(varBind, **context)

    def readTestNext(self, varBind, **context):
        name, val = varBind

        acFun = context.get("acFun")
        if acFun:
            if self.maxAccess not in (
                "read-only",
                "read-write",
                "read-create",
            ) or acFun("read", (name, self.syntax), **context):
                raise error.NoAccessError(name=name, idx=context.get("idx"))

        MibTree.readTestNext(self, varBind, **context)

    def readGetNext(self, varBind, **context):
        name, val = varBind

        # have to duplicate AC here as *Next code above treats
        # noAccess as a noSuchObject at the Test stage, goes on
        # to Reading
        acFun = context.get("acFun")
        if acFun:
            if self.maxAccess not in (
                "read-only",
                "read-write",
                "read-create",
            ) or acFun("read", (name, self.syntax), **context):
                raise error.NoAccessError(name=name, idx=context.get("idx"))

        return MibTree.readGetNext(self, varBind, **context)

    # Two-phase commit implementation

    def writeTest(self, varBind, **context):
        name, val = varBind

        if name == self.name:
            raise error.NoAccessError(name=name, idx=context.get("idx"))

        acFun = context.get("acFun")
        if acFun:
            if self.maxAccess not in ("read-write", "read-create") or acFun(
                "write", (name, self.syntax), **context
            ):
                raise error.NotWritableError(name=name, idx=context.get("idx"))

        MibTree.writeTest(self, varBind, **context)


class MibScalarInstance(MibTree):
    """Scalar MIB variable instance. Implements read/write operations."""

    def __init__(self, typeName, instId, syntax):
        MibTree.__init__(self, typeName + instId, syntax)
        self.typeName = typeName
        self.instId = instId
        self.__oldSyntax = None

    #
    # Managed object value access methods
    #

    # noinspection PyUnusedLocal
    def getValue(self, name, **context):
        debug.logger & debug.FLAG_INS and debug.logger(
            f"getValue: returning {self.syntax!r} for {self.name}"
        )
        return self.syntax.clone()

    def setValue(self, value, name, **context):
        if value is None:
            value = univ.noValue

        try:
            if hasattr(self.syntax, "setValue"):
                return self.syntax.setValue(value)
            else:
                return self.syntax.clone(value)

        except PyAsn1Error:
            exc_t, exc_v, exc_tb = sys.exc_info()
            debug.logger & debug.FLAG_INS and debug.logger(
                "setValue: {}={!r} failed with traceback {}".format(
                    self.name, value, traceback.format_exception(exc_t, exc_v, exc_tb)
                )
            )
            if isinstance(exc_v, error.TableRowManagement):
                raise exc_v
            else:
                raise error.WrongValueError(
                    name=name, idx=context.get("idx"), msg=exc_v
                )

    #
    # Subtree traversal
    #
    # Missing branches are indicated by the NoSuchInstanceError exception.
    #

    def getBranch(self, name, **context):
        try:
            return MibTree.getBranch(self, name, **context)

        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def getNextBranch(self, name, **context):
        try:
            return MibTree.getNextBranch(self, name, **context)

        except (error.NoSuchInstanceError, error.NoSuchObjectError):
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def getNode(self, name, **context):
        # Recursion terminator
        if name == self.name:
            return self
        raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def getNextNode(self, name, **context):
        raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    # MIB instrumentation methods

    # Read operation

    def readTest(self, varBind, **context):
        name, val = varBind

        if name != self.name:
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def readGet(self, varBind, **context):
        name, val = varBind

        # Return current variable (name, value)
        if name == self.name:
            debug.logger & debug.FLAG_INS and debug.logger(
                f"readGet: {self.name}={self.syntax!r}"
            )
            return self.name, self.getValue(name, **context)
        else:
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def readTestNext(self, varBind, **context):
        name, val = varBind

        oName = context.get("oName")

        if name != self.name or name <= oName:
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def readGetNext(self, varBind, **context):
        name, val = varBind

        oName = context.get("oName")

        if name == self.name and name > oName:
            debug.logger & debug.FLAG_INS and debug.logger(
                f"readGetNext: {self.name}={self.syntax!r}"
            )
            return self.readGet(varBind, **context)

        else:
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    # Write operation: two-phase commit

    # noinspection PyAttributeOutsideInit
    def writeTest(self, varBind, **context):
        name, val = varBind

        # Make sure write's allowed
        if name == self.name:
            try:
                self.__newSyntax = self.setValue(val, name, **context)

            except error.MibOperationError:
                # SMI exceptions may carry additional content
                why = sys.exc_info()[1]
                if "syntax" in why:
                    self.__newSyntax = why["syntax"]
                    raise why
                else:
                    raise error.WrongValueError(
                        name=name, idx=context.get("idx"), msg=sys.exc_info()[1]
                    )
        else:
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def writeCommit(self, varBind, **context):
        # Backup original value
        if self.__oldSyntax is None:
            self.__oldSyntax = self.syntax
        # Commit new value
        self.syntax = self.__newSyntax

    # noinspection PyAttributeOutsideInit
    def writeCleanup(self, varBind, **context):
        self.branchVersionId += 1
        name, val = varBind
        debug.logger & debug.FLAG_INS and debug.logger(f"writeCleanup: {name}={val!r}")
        # Drop previous value
        self.__newSyntax = self.__oldSyntax = None

    # noinspection PyAttributeOutsideInit
    def writeUndo(self, varBind, **context):
        # Revive previous value
        self.syntax = self.__oldSyntax
        self.__newSyntax = self.__oldSyntax = None

    # Table column instance specifics

    # Create operation

    # noinspection PyUnusedLocal,PyAttributeOutsideInit
    def createTest(self, varBind, **context):
        name, val = varBind

        if name == self.name:
            try:
                self.__newSyntax = self.setValue(val, name, **context)

            except error.MibOperationError:
                # SMI exceptions may carry additional content
                why = sys.exc_info()[1]
                if "syntax" in why:
                    self.__newSyntax = why["syntax"]
                else:
                    raise error.WrongValueError(
                        name=name, idx=context.get("idx"), msg=sys.exc_info()[1]
                    )
        else:
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def createCommit(self, varBind, **context):
        name, val = varBind

        if val is not None:
            self.writeCommit(varBind, **context)

    def createCleanup(self, varBind, **context):
        self.branchVersionId += 1
        name, val = varBind

        debug.logger & debug.FLAG_INS and debug.logger(f"createCleanup: {name}={val!r}")

        if val is not None:
            self.writeCleanup(varBind, **context)

    def createUndo(self, varBind, **context):
        name, val = varBind

        if val is not None:
            self.writeUndo(varBind, **context)

    # Destroy operation

    # noinspection PyUnusedLocal,PyAttributeOutsideInit
    def destroyTest(self, varBind, **context):
        name, val = varBind

        if name == self.name:
            try:
                self.__newSyntax = self.setValue(val, name, **context)

            except error.MibOperationError:
                # SMI exceptions may carry additional content
                why = sys.exc_info()[1]
                if "syntax" in why:
                    self.__newSyntax = why["syntax"]
        else:
            raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def destroyCommit(self, varBind, **context):
        pass

    # noinspection PyUnusedLocal
    def destroyCleanup(self, varBind, **context):
        self.branchVersionId += 1

    def destroyUndo(self, varBind, **context):
        pass


# Conceptual table classes


class MibTableColumn(MibScalar):
    """MIB table column. Manages a set of column instance variables"""

    protoInstance = MibScalarInstance

    def __init__(self, name, syntax):
        MibScalar.__init__(self, name, syntax)
        self.__createdInstances = {}
        self.__destroyedInstances = {}
        self.__rowOpWanted = {}

    #
    # Subtree traversal
    #
    # Missing leaves are indicated by the NoSuchInstanceError exception.
    #

    def getBranch(self, name, **context):
        if name in self._vars:
            return self._vars[name]
        raise error.NoSuchInstanceError(name=name, idx=context.get("idx"))

    def setProtoInstance(self, protoInstance):
        self.protoInstance = protoInstance

    # Column creation (this should probably be converted into some state
    # machine for clarity). Also, it might be a good idea to inidicate
    # defaulted cols creation in a clearer way than just a val == None.

    def createTest(self, varBind, **context):
        name, val = varBind

        # Make sure creation allowed, create a new column instance but
        # do not replace the old one
        if name == self.name:
            raise error.NoAccessError(name=name, idx=context.get("idx"))

        acFun = context.get("acFun")
        if acFun:
            if (
                val is not None
                and self.maxAccess != "read-create"
                or acFun("write", (name, self.syntax), **context)
            ):
                debug.logger & debug.FLAG_ACL and debug.logger(
                    f"createTest: {name}={val!r} {self.maxAccess} at {self.name}"
                )
                raise error.NoCreationError(name=name, idx=context.get("idx"))

        # Create instances if either it does not yet exist (row creation)
        # or a value is passed (multiple OIDs in SET PDU)
        if val is None and name in self.__createdInstances:
            return

        self.__createdInstances[name] = self.protoInstance(
            self.name, name[len(self.name) :], self.syntax.clone()
        )

        self.__createdInstances[name].createTest(varBind, **context)

    def createCommit(self, varBind, **context):
        name, val = varBind

        # Commit new instance value
        if name in self._vars:  # XXX
            if name in self.__createdInstances:
                self._vars[name].createCommit(varBind, **context)
            return

        self.__createdInstances[name].createCommit(varBind, **context)

        # ...commit new column instance
        self._vars[name], self.__createdInstances[name] = self.__createdInstances[
            name
        ], self._vars.get(name)

    def createCleanup(self, varBind, **context):
        name, val = varBind

        # Drop previous column instance
        self.branchVersionId += 1
        if name in self.__createdInstances:
            if self.__createdInstances[name] is not None:
                self.__createdInstances[name].createCleanup(varBind, **context)
            del self.__createdInstances[name]

        elif name in self._vars:
            self._vars[name].createCleanup(varBind, **context)

    def createUndo(self, varBind, **context):
        name, val = varBind

        # Set back previous column instance, drop the new one
        if name in self.__createdInstances:
            self._vars[name] = self.__createdInstances[name]
            del self.__createdInstances[name]
            # Remove new instance on rollback
            if self._vars[name] is None:
                del self._vars[name]

            else:
                # Catch half-created instances (hackerish)
                try:
                    self._vars[name] == 0

                except PyAsn1Error:
                    del self._vars[name]

                else:
                    self._vars[name].createUndo(varBind, **context)

    # Column destruction

    def destroyTest(self, varBind, **context):
        name, val = varBind

        # Make sure destruction is allowed
        if name == self.name:
            raise error.NoAccessError(name=name, idx=context.get("idx"))

        if name not in self._vars:
            return

        acFun = context.get("acFun")
        if acFun:
            if (
                val is not None
                and self.maxAccess != "read-create"
                or acFun("write", (name, self.syntax), **context)
            ):
                raise error.NoAccessError(name=name, idx=context.get("idx"))

        self._vars[name].destroyTest(varBind, **context)

    def destroyCommit(self, varBind, **context):
        name, val = varBind

        # Make a copy of column instance and take it off the tree
        if name in self._vars:
            self._vars[name].destroyCommit(varBind, **context)
            self.__destroyedInstances[name] = self._vars[name]
            del self._vars[name]

    def destroyCleanup(self, varBind, **context):
        name, val = varBind

        # Drop instance copy
        self.branchVersionId += 1

        if name in self.__destroyedInstances:
            self.__destroyedInstances[name].destroyCleanup(varBind, **context)
            debug.logger & debug.FLAG_INS and debug.logger(
                f"destroyCleanup: {name}={val!r}"
            )
            del self.__destroyedInstances[name]

    def destroyUndo(self, varBind, **context):
        name, val = varBind

        # Set back column instance
        if name in self.__destroyedInstances:
            self._vars[name] = self.__destroyedInstances[name]
            self._vars[name].destroyUndo(varBind, **context)
            del self.__destroyedInstances[name]

    # Set/modify column

    def writeTest(self, varBind, **context):
        name, val = varBind

        # Besides common checks, request row creation on no-instance
        try:
            # First try the instance
            MibScalar.writeTest(self, varBind, **context)

        # ...otherwise proceed with creating new column
        except (error.NoSuchInstanceError, error.RowCreationWanted):
            excValue = sys.exc_info()[1]
            if isinstance(excValue, error.RowCreationWanted):
                self.__rowOpWanted[name] = excValue
            else:
                self.__rowOpWanted[name] = error.RowCreationWanted()
            self.createTest(varBind, **context)

        except error.RowDestructionWanted:
            self.__rowOpWanted[name] = error.RowDestructionWanted()
            self.destroyTest(varBind, **context)

        if name in self.__rowOpWanted:
            debug.logger & debug.FLAG_INS and debug.logger(
                f"{self.__rowOpWanted[name]} flagged by {name}={val!r}, exception {sys.exc_info()[1]}"
            )
            raise self.__rowOpWanted[name]

    def __delegateWrite(self, subAction, varBind, **context):
        name, val = varBind

        if name not in self.__rowOpWanted:
            actionFun = getattr(MibScalar, "write" + subAction)
            return actionFun(self, varBind, **context)

        if isinstance(self.__rowOpWanted[name], error.RowCreationWanted):
            actionFun = getattr(self, "create" + subAction)
            return actionFun(varBind, **context)

        if isinstance(self.__rowOpWanted[name], error.RowDestructionWanted):
            actionFun = getattr(self, "destroy" + subAction)
            return actionFun(varBind, **context)

    def writeCommit(self, varBind, **context):
        name, val = varBind

        self.__delegateWrite("Commit", varBind, **context)
        if name in self.__rowOpWanted:
            raise self.__rowOpWanted[name]

    def writeCleanup(self, varBind, **context):
        name, val = varBind

        self.branchVersionId += 1

        self.__delegateWrite("Cleanup", varBind, **context)

        if name in self.__rowOpWanted:
            e = self.__rowOpWanted[name]
            del self.__rowOpWanted[name]
            debug.logger & debug.FLAG_INS and debug.logger(
                f"{e} dropped by {name}={val!r}"
            )
            raise e

    def writeUndo(self, varBind, **context):
        name, val = varBind

        if name in self.__rowOpWanted:
            self.__rowOpWanted[name] = error.RowDestructionWanted()

        self.__delegateWrite("Undo", varBind, **context)

        if name in self.__rowOpWanted:
            e = self.__rowOpWanted[name]
            del self.__rowOpWanted[name]
            debug.logger & debug.FLAG_INS and debug.logger(
                f"{e} dropped by {name}={val!r}"
            )
            raise e


class MibTableRow(MibTree):
    r"""MIB table row (SMI 'Entry').

    Manages a set of table columns.
    Implements row creation/destruction.
    """

    def __init__(self, name):
        MibTree.__init__(self, name)
        self.__idToIdxCache = cache.Cache()
        self.__idxToIdCache = cache.Cache()
        self.indexNames = ()
        self.augmentingRows = {}

    # Table indices resolution. Handle almost all possible rfc1902 types
    # explicitly rather than by means of isSuperTypeOf() method because
    # some subtypes may be implicitly tagged what renders base tag
    # unavailable.

    __intBaseTag = Integer32.tagSet.getBaseTag()
    __strBaseTag = OctetString.tagSet.getBaseTag()
    __oidBaseTag = ObjectIdentifier.tagSet.getBaseTag()
    __ipaddrTagSet = IpAddress.tagSet
    __bitsBaseTag = Bits.tagSet.getBaseTag()

    def setFromName(self, obj, value, impliedFlag=None, parentIndices=None):
        if not value:
            raise error.SmiError(f"Short OID for index {obj!r}")
        if hasattr(obj, "cloneFromName"):
            return obj.cloneFromName(
                value, impliedFlag, parentRow=self, parentIndices=parentIndices
            )
        baseTag = obj.getTagSet().getBaseTag()
        if baseTag == self.__intBaseTag:
            return obj.clone(value[0]), value[1:]
        elif self.__ipaddrTagSet.isSuperTagSetOf(obj.getTagSet()):
            return obj.clone(".".join([str(x) for x in value[:4]])), value[4:]
        elif baseTag == self.__strBaseTag:
            # rfc1902, 7.7
            if impliedFlag:
                return obj.clone(tuple(value)), ()
            elif obj.is_fixed_length():
                fixed_length = obj.get_fixed_length()
                return obj.clone(tuple(value[:fixed_length])), value[fixed_length:]
            else:
                return obj.clone(tuple(value[1 : value[0] + 1])), value[value[0] + 1 :]
        elif baseTag == self.__oidBaseTag:
            if impliedFlag:
                return obj.clone(value), ()
            else:
                return obj.clone(value[1 : value[0] + 1]), value[value[0] + 1 :]
        # rfc2578, 7.1
        elif baseTag == self.__bitsBaseTag:
            return obj.clone(tuple(value[1 : value[0] + 1])), value[value[0] + 1 :]
        else:
            raise error.SmiError(f"Unknown value type for index {obj!r}")

    def getAsName(self, obj, impliedFlag=None, parentIndices=None):
        if hasattr(obj, "cloneAsName"):
            return obj.cloneAsName(
                impliedFlag, parentRow=self, parentIndices=parentIndices
            )
        baseTag = obj.getTagSet().getBaseTag()
        if baseTag == self.__intBaseTag:
            # noinspection PyRedundantParentheses
            return (int(obj),)
        elif self.__ipaddrTagSet.isSuperTagSetOf(obj.getTagSet()):
            return obj.asNumbers()
        elif baseTag == self.__strBaseTag:
            if impliedFlag or obj.is_fixed_length():
                initial = ()
            else:
                initial = (len(obj),)
            return initial + obj.asNumbers()
        elif baseTag == self.__oidBaseTag:
            if impliedFlag:
                return tuple(obj)
            else:
                return (len(obj),) + tuple(obj)
        # rfc2578, 7.1
        elif baseTag == self.__bitsBaseTag:
            return (len(obj),) + obj.asNumbers()
        else:
            raise error.SmiError(f"Unknown value type for index {obj!r}")

    # Fate sharing mechanics

    def announceManagementEvent(self, action, varBind, **context):
        name, val = varBind

        # Convert OID suffix into index vals
        instId = name[len(self.name) + 1 :]
        baseIndices = []
        indices = []
        for impliedFlag, modName, symName in self.indexNames:
            (mibObj,) = mibBuilder.import_symbols(modName, symName)
            syntax, instId = self.setFromName(
                mibObj.syntax, instId, impliedFlag, indices
            )

            if self.name == mibObj.name[:-1]:
                baseIndices.append((mibObj.name, syntax))

            indices.append(syntax)

        if instId:
            raise error.SmiError(
                f"Excessive instance identifier sub-OIDs left at {self}: {instId}"
            )

        if not baseIndices:
            return

        for modName, mibSym in self.augmentingRows:
            (mibObj,) = mibBuilder.import_symbols(modName, mibSym)
            debug.logger & debug.FLAG_INS and debug.logger(
                f"announceManagementEvent {action} to {mibObj}"
            )
            mibObj.receiveManagementEvent(action, (baseIndices, val), **context)

    def receiveManagementEvent(self, action, varBind, **context):
        baseIndices, val = varBind

        # The default implementation supports one-to-one rows dependency
        newSuffix = ()
        # Resolve indices intersection
        for impliedFlag, modName, symName in self.indexNames:
            (mibObj,) = mibBuilder.import_symbols(modName, symName)
            parentIndices = []
            for name, syntax in baseIndices:
                if name == mibObj.name:
                    newSuffix += self.getAsName(syntax, impliedFlag, parentIndices)
                parentIndices.append(syntax)

        if newSuffix:
            debug.logger & debug.FLAG_INS and debug.logger(
                f"receiveManagementEvent {action} for suffix {newSuffix}"
            )
            self.__manageColumns(action, (), (newSuffix, val), **context)

    def registerAugmentions(self, *names):
        for modName, symName in names:
            if (modName, symName) in self.augmentingRows:
                raise error.SmiError(
                    f"Row {self.name} already augmented by {modName}::{symName}"
                )
            self.augmentingRows[(modName, symName)] = 1
        return self

    def setIndexNames(self, *names):
        for name in names:
            self.indexNames += (name,)
        return self

    def getIndexNames(self):
        return self.indexNames

    def __manageColumns(self, action, excludeName, varBind, **context):
        nameSuffix, val = varBind

        # Build a map of index names and values for automatic initialization
        indexVals = {}
        instId = nameSuffix
        indices = []
        for impliedFlag, modName, symName in self.indexNames:
            (mibObj,) = mibBuilder.import_symbols(modName, symName)
            syntax, instId = self.setFromName(
                mibObj.syntax, instId, impliedFlag, indices
            )
            indexVals[mibObj.name] = syntax
            indices.append(syntax)

        for name, var in self._vars.items():
            if name == excludeName:
                continue

            actionFun = getattr(var, action)

            if name in indexVals:
                # NOTE(etingof): disable VACM call
                _context = context.copy()
                _context.pop("acFun", None)

                actionFun((name + nameSuffix, indexVals[name]), **_context)
            else:
                actionFun((name + nameSuffix, val), **context)

            debug.logger & debug.FLAG_INS and debug.logger(
                "__manageColumns: action {} name {} suffix {} {}value {!r}".format(
                    action,
                    name,
                    nameSuffix,
                    name in indexVals and "index " or "",
                    indexVals.get(name, val),
                )
            )

    def __delegate(self, subAction, varBind, **context):
        name, val = varBind

        # Relay operation request to column, expect row operation request.
        rowIsActive = False

        try:
            writeFun = getattr(self.getBranch(name, **context), "write" + subAction)
            writeFun(varBind, **context)

        except error.RowCreationWanted:
            createAction = "create" + subAction

            self.__manageColumns(
                createAction,
                name[: len(self.name) + 1],
                (name[len(self.name) + 1 :], None),
                **context,
            )

            self.announceManagementEvent(createAction, (name, None), **context)

            # watch for RowStatus == 'stActive'
            rowIsActive = sys.exc_info()[1].get("syntax", 0) == 1

        except error.RowDestructionWanted:
            destroyAction = "destroy" + subAction

            self.__manageColumns(
                destroyAction,
                name[: len(self.name) + 1],
                (name[len(self.name) + 1 :], None),
                **context,
            )

            self.announceManagementEvent(destroyAction, (name, None), **context)

        return rowIsActive

    def writeTest(self, varBind, **context):
        self.__delegate("Test", varBind, **context)

    def writeCommit(self, varBind, **context):
        name, val = varBind
        rowIsActive = self.__delegate("Commit", varBind, **context)
        if rowIsActive:
            for mibNode in self._vars.values():
                colNode = mibNode.getNode(
                    mibNode.name + name[len(self.name) + 1 :], **context
                )
                if not colNode.syntax.hasValue():
                    raise error.InconsistentValueError(
                        msg="Row consistency check failed for %r" % colNode
                    )

    def writeCleanup(self, varBind, **context):
        self.branchVersionId += 1
        self.__delegate("Cleanup", varBind, **context)

    def writeUndo(self, varBind, **context):
        self.__delegate("Undo", varBind, **context)

    # Table row management

    # Table row access by instance name

    def getInstName(self, colId, instId):
        return self.name + (colId,) + instId

    # Table index management

    def getIndicesFromInstId(self, instId):
        """Return index values for instance identification"""
        if instId in self.__idToIdxCache:
            return self.__idToIdxCache[instId]

        indices = []
        for impliedFlag, modName, symName in self.indexNames:
            (mibObj,) = mibBuilder.import_symbols(modName, symName)
            try:
                syntax, instId = self.setFromName(
                    mibObj.syntax, instId, impliedFlag, indices
                )
            except PyAsn1Error:
                debug.logger & debug.FLAG_INS and debug.logger(
                    f"error resolving table indices at {self.__class__.__name__}, {instId}: {sys.exc_info()[1]}"
                )
                indices = [instId]
                instId = ()
                break

            indices.append(syntax)  # to avoid cyclic refs

        if instId:
            raise error.SmiError(
                f"Excessive instance identifier sub-OIDs left at {self}: {instId}"
            )

        indices = tuple(indices)
        self.__idToIdxCache[instId] = indices

        return indices

    def getInstIdFromIndices(self, *indices):
        """Return column instance identification from indices"""
        try:
            return self.__idxToIdCache[indices]
        except TypeError:
            cacheable = False
        except KeyError:
            cacheable = True
        idx = 0
        instId = ()
        parentIndices = []
        for impliedFlag, modName, symName in self.indexNames:
            if idx >= len(indices):
                break
            (mibObj,) = mibBuilder.import_symbols(modName, symName)
            syntax = mibObj.syntax.clone(indices[idx])
            instId += self.getAsName(syntax, impliedFlag, parentIndices)
            parentIndices.append(syntax)
            idx += 1
        if cacheable:
            self.__idxToIdCache[indices] = instId
        return instId

    # Table access by index

    def getInstNameByIndex(self, colId, *indices):
        """Build column instance name from components"""
        return self.name + (colId,) + self.getInstIdFromIndices(*indices)

    def getInstNamesByIndex(self, *indices):
        """Build column instance names from indices"""
        instNames = []
        for columnName in self._vars.keys():
            instNames.append(self.getInstNameByIndex(*(columnName[-1],) + indices))

        return tuple(instNames)

    ## compatibility with legacy code
    def get_index_names(self):
        return self.getIndexNames()

    def set_index_names(self, *names):
        self.setIndexNames(*names)
        return self

    def register_augmentions(self, *names):
        self.registerAugmentions(*names)
        return self


class MibTable(MibTree):
    """MIB table. Manages a set of TableRow's"""

    def __init__(self, name):
        MibTree.__init__(self, name)


# OID tree
itu_t = MibTree((0,)).setLabel("itu-t")
iso = MibTree((1,))
joint_iso_itu_t = MibTree((2,)).setLabel("joint-iso-itu-t")

# Import base ASN.1 objects even if this MIB does not use it

(Integer, OctetString, ObjectIdentifier) = mibBuilder.import_symbols(
    "ASN1", "Integer", "OctetString", "ObjectIdentifier"
)

(NamedValues,) = mibBuilder.import_symbols("ASN1-ENUMERATION", "NamedValues")
(
    ConstraintsIntersection,
    ConstraintsUnion,
    SingleValueConstraint,
    ValueRangeConstraint,
    ValueSizeConstraint,
) = mibBuilder.import_symbols(
    "ASN1-REFINEMENT",
    "ConstraintsIntersection",
    "ConstraintsUnion",
    "SingleValueConstraint",
    "ValueRangeConstraint",
    "ValueSizeConstraint",
)

# Import SMI symbols from the MIBs this MIB depends on

# (ModuleCompliance,
#  NotificationGroup) = mibBuilder.import_symbols(
#     "SNMPv2-CONF",
#     "ModuleCompliance",
#     "NotificationGroup")

# (Bits,
#  Counter32,
#  Counter64,
#  Gauge32,
#  Integer32,
#  IpAddress,
#  ModuleIdentity,
#  MibIdentifier,
#  NotificationType,
#  ObjectIdentity,
#  MibScalar,
#  MibTable,
#  MibTableRow,
#  MibTableColumn,
#  TimeTicks,
#  Unsigned32,
#  iso) = mibBuilder.import_symbols(
#     "SNMPv2-SMI",
#     "Bits",
#     "Counter32",
#     "Counter64",
#     "Gauge32",
#     "Integer32",
#     "IpAddress",
#     "ModuleIdentity",
#     "MibIdentifier",
#     "NotificationType",
#     "ObjectIdentity",
#     "MibScalar",
#     "MibTable",
#     "MibTableRow",
#     "MibTableColumn",
#     "TimeTicks",
#     "Unsigned32",
#     "iso")

# (DisplayString,
#  TextualConvention) = mibBuilder.import_symbols(
#     "SNMPv2-TC",
#     "DisplayString",
#     "TextualConvention")


# MODULE-IDENTITY


# Types definitions


class ExtUTCTime(OctetString):
    """Custom type ExtUTCTime based on OctetString"""

    subtypeSpec = OctetString.subtypeSpec
    subtypeSpec += ConstraintsUnion(
        ValueSizeConstraint(11, 11),
        ValueSizeConstraint(13, 13),
    )


class ObjectName(ObjectIdentifier):
    """Custom type ObjectName based on ObjectIdentifier"""


class NotificationName(ObjectIdentifier):
    """Custom type NotificationName based on ObjectIdentifier"""


class TypeCoercionHackMixIn:  # XXX keep this old-style class till pyasn1 types becomes new-style
    # Reduce ASN1 type check to simple tag check as SMIv2 objects may
    # not be constraints-compatible with those used in SNMP PDU.
    def _verify_component(self, idx, value, **kwargs):
        componentType = self._componentType  # noqa: N806
        if componentType:
            if idx >= len(componentType):
                raise PyAsn1Error("Component type error out of range")
            t = componentType[idx].getType()
            if not t.getTagSet().isSuperTagSetOf(value.getTagSet()):
                raise PyAsn1Error(f"Component type error {t!r} vs {value!r}")


class SimpleSyntax(TypeCoercionHackMixIn, univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("integer-value", Integer()),
        namedtype.NamedType("string-value", OctetString()),
        namedtype.NamedType("objectID-value", univ.ObjectIdentifier()),
    )


class ApplicationSyntax(TypeCoercionHackMixIn, univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("ipAddress-value", IpAddress()),
        namedtype.NamedType("counter-value", Counter32()),
        namedtype.NamedType("timeticks-value", TimeTicks()),
        namedtype.NamedType("arbitrary-value", Opaque()),
        namedtype.NamedType("big-counter-value", Counter64()),
        # This conflicts with Counter32
        # namedtype.NamedType('unsigned-integer-value', Unsigned32()),
        namedtype.NamedType("gauge32-value", Gauge32()),
    )  # BITS misplaced?


class ObjectSyntax(univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("simple", SimpleSyntax()),
        namedtype.NamedType("application-wide", ApplicationSyntax()),
    )


# class Integer32(Integer32):
#     """Custom type Integer32 based on Integer32"""
#     subtypeSpec = Integer32.subtypeSpec
#     subtypeSpec += ConstraintsUnion(
#         ValueRangeConstraint(-2147483648, 2147483647),
#     )


# class IpAddress(OctetString):
#     """Custom type IpAddress based on OctetString"""
#     subtypeSpec = OctetString.subtypeSpec
#     subtypeSpec += ConstraintsUnion(
#         ValueSizeConstraint(4, 4),
#     )
#     fixedLength = 4


# class Counter32(Integer32):
#     """Custom type Counter32 based on Integer32"""
#     subtypeSpec = Integer32.subtypeSpec
#     subtypeSpec += ConstraintsUnion(
#         ValueRangeConstraint(0, 4294967295),
#     )


# class Gauge32(Integer32):
#     """Custom type Gauge32 based on Integer32"""
#     subtypeSpec = Integer32.subtypeSpec
#     subtypeSpec += ConstraintsUnion(
#         ValueRangeConstraint(0, 4294967295),
#     )


# class Unsigned32(Integer32):
#     """Custom type Unsigned32 based on Integer32"""
#     subtypeSpec = Integer32.subtypeSpec
#     subtypeSpec += ConstraintsUnion(
#         ValueRangeConstraint(0, 4294967295),
#     )


# class TimeTicks(Integer32):
#     """Custom type TimeTicks based on Integer32"""
#     subtypeSpec = Integer32.subtypeSpec
#     subtypeSpec += ConstraintsUnion(
#         ValueRangeConstraint(0, 4294967295),
#     )


# class Opaque(OctetString):
#     """Custom type Opaque based on OctetString"""


# class Counter64(Integer32):
#     """Custom type Counter64 based on Integer32"""
#     subtypeSpec = Integer32.subtypeSpec
#     subtypeSpec += ConstraintsUnion(
#         ValueRangeConstraint(0, 18446744073709551615),
#     )


# TEXTUAL-CONVENTIONS


# MIB Managed Objects in the order of their OIDs

_ZeroDotZero_ObjectIdentity = ObjectIdentity
zeroDotZero = _ZeroDotZero_ObjectIdentity((0, 0))
if mibBuilder.loadTexts:
    zeroDotZero.setStatus("current")
if mibBuilder.loadTexts:
    zeroDotZero.setDescription("A value used for null identifiers.")
_Org_ObjectIdentity = ObjectIdentity
org = _Org_ObjectIdentity((1, 3))
_Dod_ObjectIdentity = ObjectIdentity
dod = _Dod_ObjectIdentity((1, 3, 6))
_Internet_ObjectIdentity = ObjectIdentity
internet = _Internet_ObjectIdentity((1, 3, 6, 1))
_Directory_ObjectIdentity = ObjectIdentity
directory = _Directory_ObjectIdentity((1, 3, 6, 1, 1))
_Mgmt_ObjectIdentity = ObjectIdentity
mgmt = _Mgmt_ObjectIdentity((1, 3, 6, 1, 2))
_Mib_2_ObjectIdentity = ObjectIdentity
mib_2 = _Mib_2_ObjectIdentity((1, 3, 6, 1, 2, 1))
_Transmission_ObjectIdentity = ObjectIdentity
transmission = _Transmission_ObjectIdentity((1, 3, 6, 1, 2, 1, 10))
_Experimental_ObjectIdentity = ObjectIdentity
experimental = _Experimental_ObjectIdentity((1, 3, 6, 1, 3))
_Private_ObjectIdentity = ObjectIdentity
private = _Private_ObjectIdentity((1, 3, 6, 1, 4))
_Enterprises_ObjectIdentity = ObjectIdentity
enterprises = _Enterprises_ObjectIdentity((1, 3, 6, 1, 4, 1))
_Security_ObjectIdentity = ObjectIdentity
security = _Security_ObjectIdentity((1, 3, 6, 1, 5))
_SnmpV2_ObjectIdentity = ObjectIdentity
snmpV2 = _SnmpV2_ObjectIdentity((1, 3, 6, 1, 6))
_SnmpDomains_ObjectIdentity = ObjectIdentity
snmpDomains = _SnmpDomains_ObjectIdentity((1, 3, 6, 1, 6, 1))
_SnmpProxys_ObjectIdentity = ObjectIdentity
snmpProxys = _SnmpProxys_ObjectIdentity((1, 3, 6, 1, 6, 2))
_SnmpModules_ObjectIdentity = ObjectIdentity
snmpModules = _SnmpModules_ObjectIdentity((1, 3, 6, 1, 6, 3))

# Managed Objects groups


# Notification objects


# Notifications groups


# Agent capabilities


# Module compliance


# Export all MIB objects to the MIB builder

mibBuilder.export_symbols(
    "SNMPv2-SMI",
    **{
        "ExtUTCTime": ExtUTCTime,
        "ObjectName": ObjectName,
        "NotificationName": NotificationName,
        "SimpleSyntax": SimpleSyntax,
        "ApplicationSyntax": ApplicationSyntax,
        "ObjectSyntax": ObjectSyntax,
        "Integer32": Integer32,
        "IpAddress": IpAddress,
        "Counter32": Counter32,
        "Gauge32": Gauge32,
        "Unsigned32": Unsigned32,
        "TimeTicks": TimeTicks,
        "Opaque": Opaque,
        "Counter64": Counter64,
        "zeroDotZero": zeroDotZero,
        "org": org,
        "dod": dod,
        "internet": internet,
        "directory": directory,
        "mgmt": mgmt,
        "mib-2": mib_2,
        "transmission": transmission,
        "experimental": experimental,
        "private": private,
        "enterprises": enterprises,
        "security": security,
        "snmpV2": snmpV2,
        "snmpDomains": snmpDomains,
        "snmpProxys": snmpProxys,
        "snmpModules": snmpModules,
        "ModuleIdentity": ModuleIdentity,
        "ObjectIdentity": ObjectIdentity,
        "NotificationType": NotificationType,
        "MibNode": MibNode,
        "MibScalar": MibScalar,
        "MibScalarInstance": MibScalarInstance,
        "MibIdentifier": MibIdentifier,
        "MibTree": MibTree,
        "MibTableColumn": MibTableColumn,
        "MibTableRow": MibTableRow,
        "MibTable": MibTable,
        "Bits": Bits,
        "itu-t": itu_t,
        "iso": iso,
        "joint_iso_itu_t": joint_iso_itu_t,
        "mib_2": mib_2,
    },
)

# XXX
# getAsName/setFromName goes out of MibRow?
# revisit getNextNode() -- needs optimization
