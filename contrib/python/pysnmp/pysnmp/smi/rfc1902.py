#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import sys
import warnings
from typing import TYPE_CHECKING

from pyasn1.error import PyAsn1Error
from pyasn1.type.base import AbstractSimpleAsn1Item, SimpleAsn1Type
from pysnmp import debug
from pysnmp.proto import rfc1902, rfc1905
from pysnmp.proto.api import v2c
from pysnmp.smi.builder import ZipMibSource
from pysnmp.smi.compiler import add_mib_compiler
from pysnmp.smi.error import SmiError
from pysnmp.smi.view import MibViewController

__all__ = ["ObjectIdentity", "ObjectType", "NotificationType"]

if TYPE_CHECKING:
    from pysnmp.smi.types import MibNode


class ObjectIdentity:
    """Create an object representing MIB variable ID.

    At the protocol level, MIB variable is only identified by an OID.
    However, when interacting with humans, MIB variable can also be referred
    to by its MIB name. The *ObjectIdentity* class supports various forms
    of MIB variable identification, providing automatic conversion from
    one to others. At the same time *ObjectIdentity* objects behave like
    :py:obj:`tuples` of py:obj:`int` sub-OIDs.

    See :RFC:`1902#section-2` for more information on OBJECT-IDENTITY
    SMI definitions.

    Parameters
    ----------
    args
        initial MIB variable identity. Recognized variants:

        * single :py:obj:`tuple` or integers representing OID
        * single :py:obj:`str` representing OID in dot-separated
          integers form
        * single :py:obj:`str` representing MIB variable in
          dot-separated labels form
        * single :py:obj:`str` representing MIB name. First variable
          defined in MIB is assumed.
        * pair of :py:obj:`str` representing MIB name and variable name
        * pair of :py:obj:`str` representing MIB name and variable name
          followed by an arbitrary number of :py:obj:`str` and/or
          :py:obj:`int` values representing MIB variable instance
          identification.

    Other parameters
    ----------------
    kwargs
        MIB resolution options(object):

        * whenever only MIB name is given, resolve into last variable defined
          in MIB if last=True.  Otherwise resolves to first variable (default).

    Notes
    -----
        Actual conversion between MIB variable representation formats occurs
        upon :py:meth:`~pysnmp.smi.rfc1902.ObjectIdentity.resolve_with_mib`
        invocation.

    Examples
    --------
    >>> from pysnmp.smi.rfc1902 import ObjectIdentity
    >>> ObjectIdentity((1, 3, 6, 1, 2, 1, 1, 1, 0))
    ObjectIdentity((1, 3, 6, 1, 2, 1, 1, 1, 0))
    >>> ObjectIdentity('1.3.6.1.2.1.1.1.0')
    ObjectIdentity('1.3.6.1.2.1.1.1.0')
    >>> ObjectIdentity('iso.org.dod.internet.mgmt.mib-2.system.sysDescr.0')
    ObjectIdentity('iso.org.dod.internet.mgmt.mib-2.system.sysDescr.0')
    >>> ObjectIdentity('SNMPv2-MIB', 'system')
    ObjectIdentity('SNMPv2-MIB', 'system')
    >>> ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)
    ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)
    >>> ObjectIdentity('IP-MIB', 'ipAdEntAddr', '127.0.0.1', 123)
    ObjectIdentity('IP-MIB', 'ipAdEntAddr', '127.0.0.1', 123)

    """

    ST_DIRTY, ST_CLEAN = 1, 2
    __mibNode: "MibNode"
    __label: tuple[str, ...]
    __modName: str
    __symName: str

    def __init__(self, *args, **kwargs):
        """Create an ObjectIdentity instance."""
        self.__args = args
        self.__kwargs = kwargs
        self.__mibSourcesToAdd = self.__modNamesToLoad = None
        self.__asn1SourcesToAdd = self.__asn1SourcesOptions = None
        self.__state = self.ST_DIRTY
        self.__indices = self.__oid = self.__label = ()
        self.__modName = self.__symName = ""
        self.__mibNode = None  # type: ignore

    def get_mib_symbol(self):
        """Returns MIB variable symbolic identification.

        Returns
        -------
        str
             MIB module name
        str
             MIB variable symbolic name
        : :py:class:`~pysnmp.proto.rfc1902.ObjectName`
             class instance representing MIB variable instance index.

        Raises
        ------
        SmiError
            If MIB variable conversion has not been performed.

        Examples
        --------
        >>> objectIdentity = ObjectIdentity('1.3.6.1.2.1.1.1.0')
        >>> objectIdentity.resolve_with_mib(mibViewController)
        >>> objectIdentity.get_mib_symbol()
        ('SNMPv2-MIB', 'sysDescr', (0,))
        >>>

        """
        if self.__state & self.ST_CLEAN:
            return self.__modName, self.__symName, self.__indices
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def get_oid(self):
        """Returns OID identifying MIB variable.

        Returns
        -------
        : :py:class:`~pysnmp.proto.rfc1902.ObjectName`
            full OID identifying MIB variable including possible index part.

        Raises
        ------
        SmiError
           If MIB variable conversion has not been performed.

        Examples
        --------
        >>> objectIdentity = ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)
        >>> objectIdentity.resolve_with_mib(mibViewController)
        >>> objectIdentity.get_oid()
        ObjectName('1.3.6.1.2.1.1.1.0')
        >>>

        """
        if self.__state & self.ST_CLEAN:
            return self.__oid
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def get_label(self) -> tuple[str, ...]:
        """Returns symbolic path to this MIB variable.

        Meaning a sequence of symbolic identifications for each of parent
        MIB objects in MIB tree.

        Returns
        -------
        tuple
            sequence of names of nodes in a MIB tree from the top of the tree
            towards this MIB variable.

        Raises
        ------
        SmiError
           If MIB variable conversion has not been performed.

        Notes
        -----
        Returned sequence may not contain full path to this MIB variable
        if some symbols are now known at the moment of MIB look up.

        Examples
        --------
        >>> objectIdentity = ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)
        >>> objectIdentity.resolve_with_mib(mibViewController)
        >>> objectIdentity.get_oid()
        ('iso', 'org', 'dod', 'internet', 'mgmt', 'mib-2', 'system', 'sysDescr')
        >>>

        """
        if self.__state & self.ST_CLEAN:
            return self.__label
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def get_mib_node(self) -> "MibNode":
        """Returns MIB node object representing this MIB variable.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            class instance representing MIB variable.

        Raises
        ------
        SmiError
           If MIB variable conversion has not been performed.
        """
        if self.__state & self.ST_CLEAN:
            return self.__mibNode
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def is_fully_resolved(self):
        """Returns `True` if MIB variable conversion has been performed.

        Returns
        -------
        bool
            `True` if MIB variable conversion has been performed.

        Examples
        --------
        >>> objectIdentity = ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)
        >>> objectIdentity.is_fully_resolved()
        False
        >>> objectIdentity.resolve_with_mib(mibViewController)
        >>> objectIdentity.is_fully_resolved()
        True
        >>>

        """
        return self.__state & self.ST_CLEAN

    #
    # A gateway to MIBs manipulation routines
    #

    def add_asn1_mib_source(self, *asn1Sources, **kwargs):
        """Adds path to a repository to search ASN.1 MIB files.

        Parameters
        ----------
        asn1Sources :
            one or more URL in form of :py:obj:`str` identifying local or
            remote ASN.1 MIB repositories. Path must include the *@mib@*
            component which will be replaced with MIB module name at the
            time of search.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
            reference to itself

        Notes
        -----
        Please refer to :py:class:`~pysmi.reader.localfile.FileReader`and
        :py:class:`~pysmi.reader.httpclient.HttpReader` classes for
        in-depth information on ASN.1 MIB lookup.

        Examples
        --------
        >>> ObjectIdentity('SNMPv2-MIB', 'sysDescr').add_asn1_mib_source('https://mibs.pysnmp.com/asn1/@mib@')
        ObjectIdentity('SNMPv2-MIB', 'sysDescr')
        >>>

        """
        if self.__asn1SourcesToAdd is None:
            self.__asn1SourcesToAdd = asn1Sources
        else:
            self.__asn1SourcesToAdd += asn1Sources
        if self.__asn1SourcesOptions:
            self.__asn1SourcesOptions.update(kwargs)
        else:
            self.__asn1SourcesOptions = kwargs
        return self

    def add_mib_source(self, *mibSources):
        """Adds path to repository to search PySNMP MIB files.

        Parameters
        ----------
        mibSources :
            one or more paths to search or Python package names to import
            and search for PySNMP MIB modules.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
            reference to itself

        Notes
        -----
        Normally, ASN.1-to-Python MIB modules conversion is performed
        automatically through PySNMP/PySMI interaction. ASN1 MIB modules
        could also be manually compiled into Python via the
        `mibdump.py <https://www.pysnmp.com/pysmi/mibdump.html>`_
        tool.

        Examples
        --------
        >>> ObjectIdentity('SNMPv2-MIB', 'sysDescr').add_mib_source('/opt/pysnmp/mibs', 'pysnmp_mibs')
        ObjectIdentity('SNMPv2-MIB', 'sysDescr')
        >>>

        """
        if self.__mibSourcesToAdd is None:
            self.__mibSourcesToAdd = mibSources
        else:
            self.__mibSourcesToAdd += mibSources
        return self

    # provides deferred MIBs load
    def load_mibs(self, *modNames):
        """Schedules search and load of given MIB modules.

        Parameters
        ----------
        modNames:
            one or more MIB module names to load up and use for MIB
            variables resolution purposes.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
            reference to itself

        Examples
        --------
        >>> ObjectIdentity('SNMPv2-MIB', 'sysDescr').load_mibs('IF-MIB', 'TCP-MIB')
        ObjectIdentity('SNMPv2-MIB', 'sysDescr')
        >>>

        """
        if self.__modNamesToLoad is None:
            self.__modNamesToLoad = modNames
        else:
            self.__modNamesToLoad += modNames
        return self

    # this would eventually be called by an entity which posses a
    # reference to MibViewController
    def resolve_with_mib(self, mibViewController: MibViewController, ignoreErrors=True):
        """Perform MIB variable ID conversion.

        Parameters
        ----------
        mibViewController : :py:class:`~pysnmp.smi.view.MibViewController`
            class instance representing MIB browsing functionality.

        Other Parameters
        ----------------
        ignoreErrors: :py:class:`bool`
            If `True` (default), ignore MIB object name casting
            failures if possible.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
            reference to itself

        Raises
        ------
        SmiError
           In case of fatal MIB handling error

        Notes
        -----
        Calling this method might cause the following sequence of
        events (exact details depends on many factors):

        * ASN.1 MIB file downloaded and handed over to
          :py:class:`~pysmi.compiler.MibCompiler` for conversion into
          Python MIB module (based on pysnmp classes)
        * Python MIB module is imported by SNMP engine, internal indices
          created
        * :py:class:`~pysnmp.smi.view.MibViewController` looks up the
          rest of MIB identification information based on whatever information
          is already available, :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
          class instance
          gets updated and ready for further use.

        Examples
        --------
        >>> objectIdentity = ObjectIdentity('SNMPv2-MIB', 'sysDescr')
        >>> objectIdentity.resolve_with_mib(mibViewController)
        ObjectIdentity('SNMPv2-MIB', 'sysDescr')
        >>>

        """
        if self.__mibSourcesToAdd is not None:
            debug.logger & debug.FLAG_MIB and debug.logger(
                "adding MIB sources %s" % ", ".join(self.__mibSourcesToAdd)
            )
            mibViewController.mibBuilder.add_mib_sources(
                *[ZipMibSource(x) for x in self.__mibSourcesToAdd]
            )
            self.__mibSourcesToAdd = None

        if self.__asn1SourcesToAdd is None:
            add_mib_compiler(
                mibViewController.mibBuilder, ifAvailable=True, ifNotAdded=True
            )
        else:
            debug.logger & debug.FLAG_MIB and debug.logger(
                "adding MIB compiler with source paths %s"
                % ", ".join(self.__asn1SourcesToAdd)
            )
            add_mib_compiler(
                mibViewController.mibBuilder,
                sources=self.__asn1SourcesToAdd,
                searchers=self.__asn1SourcesOptions.get("searchers"),
                borrowers=self.__asn1SourcesOptions.get("borrowers"),
                destination=self.__asn1SourcesOptions.get("destination"),
                ifAvailable=self.__asn1SourcesOptions.get("ifAvailable"),
                ifNotAdded=self.__asn1SourcesOptions.get("ifNotAdded"),
            )
            self.__asn1SourcesToAdd = self.__asn1SourcesOptions = None

        if self.__modNamesToLoad is not None:
            debug.logger & debug.FLAG_MIB and debug.logger(
                "loading MIB modules %s" % ", ".join(self.__modNamesToLoad)
            )
            mibViewController.mibBuilder.load_modules(*self.__modNamesToLoad)
            self.__modNamesToLoad = None

        if self.__state & self.ST_CLEAN:
            return self

        MibScalar, MibTableColumn = mibViewController.mibBuilder.import_symbols(
            "SNMPv2-SMI", "MibScalar", "MibTableColumn"
        )

        self.__indices = ()

        if isinstance(self.__args[0], ObjectIdentity):
            self.__args[0].resolve_with_mib(mibViewController, ignoreErrors)

        if len(self.__args) == 1:  # OID or label or MIB module
            debug.logger & debug.FLAG_MIB and debug.logger(
                "resolving %s as OID or label" % self.__args
            )
            try:
                # pyasn1 ObjectIdentifier or sequence of ints or string OID
                self.__oid = rfc1902.ObjectName(self.__args[0])  # OID
            except PyAsn1Error:
                # sequence of sub-OIDs and labels
                if isinstance(self.__args[0], (list, tuple)):
                    prefix, label, suffix = mibViewController.get_node_name(
                        self.__args[0]
                    )
                # string label
                elif "." in self.__args[0]:
                    prefix, label, suffix = mibViewController.get_node_name_by_oid(
                        tuple(self.__args[0].split("."))
                    )
                # MIB module name
                else:
                    modName = self.__args[0]
                    mibViewController.mibBuilder.load_modules(modName)
                    if self.__kwargs.get("last"):
                        prefix, label, suffix = mibViewController.get_last_node_name(
                            modName
                        )
                    else:
                        prefix, label, suffix = mibViewController.get_first_node_name(
                            modName
                        )

                if suffix:
                    try:
                        suffix = tuple(int(x) for x in suffix)
                    except ValueError:
                        raise SmiError(f"Unknown object name component {suffix!r}")
                self.__oid = rfc1902.ObjectName(prefix + suffix)
            else:
                prefix, label, suffix = mibViewController.get_node_name_by_oid(
                    self.__oid
                )

            debug.logger & debug.FLAG_MIB and debug.logger(
                f"resolved {self.__args!r} into prefix {prefix!r} and suffix {suffix!r}"
            )

            modName, symName, _ = mibViewController.get_node_location(prefix)

            self.__modName = modName
            self.__symName = symName

            self.__label = label

            (mibNode,) = mibViewController.mibBuilder.import_symbols(modName, symName)

            self.__mibNode = mibNode

            debug.logger & debug.FLAG_MIB and debug.logger(
                f"resolved prefix {prefix!r} into MIB node {mibNode!r}"
            )

            if isinstance(mibNode, MibTableColumn):  # table column
                if suffix:
                    rowModName, rowSymName, _ = mibViewController.get_node_location(
                        mibNode.name[:-1]
                    )
                    (rowNode,) = mibViewController.mibBuilder.import_symbols(
                        rowModName, rowSymName
                    )
                    self.__indices = rowNode.getIndicesFromInstId(suffix)
            else:
                if suffix:
                    self.__indices = (rfc1902.ObjectName(suffix),)
            self.__state |= self.ST_CLEAN

            debug.logger & debug.FLAG_MIB and debug.logger(
                f"resolved indices are {self.__indices!r}"
            )

            return self
        elif len(self.__args) > 1:  # MIB, symbol[, index, index ...]
            # MIB, symbol, index, index
            if self.__args[0] and self.__args[1]:
                self.__modName = self.__args[0].__str__()
                self.__symName = self.__args[1]
            # MIB, ''
            elif self.__args[0]:
                mibViewController.mibBuilder.load_modules(self.__args[0])
                if self.__kwargs.get("last"):
                    prefix, label, suffix = mibViewController.get_last_node_name(
                        self.__args[0].__str__()
                    )
                else:
                    prefix, label, suffix = mibViewController.get_first_node_name(
                        self.__args[0].__str__()
                    )
                self.__modName, self.__symName, _ = mibViewController.get_node_location(
                    prefix
                )
            # '', symbol, index, index
            else:
                prefix, label, suffix = mibViewController.get_node_name(self.__args[1:])
                self.__modName, self.__symName, _ = mibViewController.get_node_location(
                    prefix
                )

            (mibNode,) = mibViewController.mibBuilder.import_symbols(
                self.__modName, self.__symName
            )

            self.__mibNode = mibNode

            self.__oid = rfc1902.ObjectName(mibNode.getName())

            prefix, label, suffix = mibViewController.get_node_name_by_oid(self.__oid)
            self.__label = label

            debug.logger & debug.FLAG_MIB and debug.logger(
                f"resolved {self.__args!r} into prefix {prefix!r} and suffix {suffix!r}"
            )

            if isinstance(mibNode, MibTableColumn):  # table
                rowModName, rowSymName, _ = mibViewController.get_node_location(
                    mibNode.name[:-1]
                )
                (rowNode,) = mibViewController.mibBuilder.import_symbols(
                    rowModName, rowSymName
                )
                if self.__args[2:]:
                    try:
                        instIds = rowNode.getInstIdFromIndices(*self.__args[2:])
                        self.__oid += instIds
                        self.__indices = rowNode.getIndicesFromInstId(instIds)
                    except PyAsn1Error:
                        raise SmiError(
                            "Instance index {!r} to OID conversion failure at object {!r}: {}".format(
                                self.__args[2:], mibNode.getLabel(), sys.exc_info()[1]
                            )
                        )
            elif self.__args[2:]:  # any other kind of MIB node with indices
                if self.__args[2:]:
                    instId = rfc1902.ObjectName(
                        ".".join([str(x) for x in self.__args[2:]])
                    )
                    self.__oid += instId
                    self.__indices = (instId,)
            self.__state |= self.ST_CLEAN

            debug.logger & debug.FLAG_MIB and debug.logger(
                f"resolved indices are {self.__indices!r}"
            )

            return self
        else:
            raise SmiError("Non-OID, label or MIB symbol")

    def prettyPrint(self):  # noqa: N802
        """Return a human-friendly representation of the object."""
        if self.__state & self.ST_CLEAN:
            s = rfc1902.OctetString()
            return "{}::{}{}{}".format(
                self.__modName,
                self.__symName,
                self.__indices and "." or "",
                ".".join(
                    [
                        x.isSuperTypeOf(s, matchConstraints=False)
                        and '"%s"' % x.prettyPrint()
                        or x.prettyPrint()
                        for x in self.__indices
                    ]
                ),
            )
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def __repr__(self):
        """Return a string representation of the object."""
        return "{}({})".format(
            self.__class__.__name__, ", ".join([repr(x) for x in self.__args])
        )

    # Redirect some attrs access to the OID object to behave alike

    def __str__(self):
        """Return a string representation of the object."""
        if self.__state & self.ST_CLEAN:
            return str(self.__oid)
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __eq__(self, other):
        """Compare OID with another OID."""
        if self.__state & self.ST_CLEAN:
            return self.__oid == other
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __ne__(self, other):
        """Compare OID with another OID."""
        if self.__state & self.ST_CLEAN:
            return self.__oid != other
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __lt__(self, other):
        """Compare OID with another OID."""
        if self.__state & self.ST_CLEAN:
            return self.__oid < other
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __le__(self, other):
        """Compare OID with another OID."""
        if self.__state & self.ST_CLEAN:
            return self.__oid <= other
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __gt__(self, other):
        """Compare OID with another OID."""
        if self.__state & self.ST_CLEAN:
            return self.__oid > other
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __ge__(self, other):
        """Compare OID with another OID."""
        if self.__state & self.ST_CLEAN:
            return self.__oid > other
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __nonzero__(self):
        """Return True if the OID is not empty."""
        if self.__state & self.ST_CLEAN:
            return self.__oid != 0
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __bool__(self):
        """Return True if the OID is not empty."""
        if self.__state & self.ST_CLEAN:
            return bool(self.__oid)
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __getitem__(self, i):
        """Return the i-th element of the OID."""
        if self.__state & self.ST_CLEAN:
            return self.__oid[i]
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __len__(self):
        """Return the length of the OID."""
        if self.__state & self.ST_CLEAN:
            return len(self.__oid)
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __add__(self, other):
        """Add OID to the right of the other object."""
        if self.__state & self.ST_CLEAN:
            return self.__oid + other
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __radd__(self, other):
        """Add OID to the left of the other object."""
        if self.__state & self.ST_CLEAN:
            return other + self.__oid
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    def __hash__(self):
        """Return a hash value of the object."""
        if self.__state & self.ST_CLEAN:
            return hash(self.__oid)
        else:
            raise SmiError(
                "%s object not properly initialized" % self.__class__.__name__
            )

    # Compatibility with PySNMP older versions
    deprecated_attributes = {
        "getOid": "get_oid",
        "getMibSymbol": "get_mib_symbol",
        "getMibNode": "get_mib_node",
        "getLabel": "get_label",
        "isFullyResolved": "is_fully_resolved",
        "addAsn1MibSource": "add_asn1_mib_source",
        "addMibSource": "add_mib_source",
        "loadMibs": "load_mibs",
        "resolveWithMib": "resolve_with_mib",
    }

    def __getattr__(self, attr):
        """Redirect some attributes."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )

            # Redirect the deprecated attribute access to the new one.
            return getattr(self, new_attr)

        # Redirect some attrs access to the OID object to behave alike.
        if self.__state & self.ST_CLEAN:
            if attr in (
                "asTuple",
                "clone",
                "subtype",
                "isPrefixOf",
                "isSameTypeWith",
                "isSuperTypeOf",
                "getTagSet",
                "getEffectiveTagSet",
                "getTagMap",
                "tagSet",
                "index",
            ):
                return getattr(self.__oid, attr)
            raise AttributeError(attr)
        else:
            raise SmiError(
                f"{self.__class__.__name__} object not properly initialized for accessing {attr}"
            )


# A two-element sequence of ObjectIdentity and SNMP data type object
class ObjectType:
    """Create an object representing MIB variable.

    Instances of :py:class:`~pysnmp.smi.rfc1902.ObjectType` class are
    containers incorporating :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
    class instance (identifying MIB variable) and optional value belonging
    to one of SNMP types (:RFC:`1902`).

    Typical MIB variable is defined like this (from *SNMPv2-MIB.txt*):

    .. code-block:: asn1

       sysDescr OBJECT-TYPE
           SYNTAX      DisplayString (SIZE (0..255))
           MAX-ACCESS  read-only
           STATUS      current
           DESCRIPTION
                   "A textual description of the entity.  This value should..."
           ::= { system 1 }

    Corresponding ObjectType instantiation would look like this:

    .. code-block:: python

        ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr'), 'Linux i386 box')

    In order to behave like SNMP variable-binding (:RFC:`1157#section-4.1.1`),
    :py:class:`~pysnmp.smi.rfc1902.ObjectType` objects also support
    sequence protocol addressing `objectIdentity` as its 0-th element
    and `objectSyntax` as 1-st.

    See :RFC:`1902#section-2` for more information on OBJECT-TYPE SMI
    definitions.

    Parameters
    ----------
    objectIdentity : :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
        Class instance representing MIB variable identification.
    objectSyntax :
        Represents a value associated with this MIB variable. Values of
        built-in Python types will be automatically converted into SNMP
        object as specified in OBJECT-TYPE->SYNTAX field.

    Notes
    -----
        Actual conversion between MIB variable representation formats occurs
        upon :py:meth:`~pysnmp.smi.rfc1902.ObjectType.resolve_with_mib`
        invocation.

    Examples
    --------
    >>> from pysnmp.smi.rfc1902 import *
    >>> ObjectType(ObjectIdentity('1.3.6.1.2.1.1.1.0'))
    ObjectType(ObjectIdentity('1.3.6.1.2.1.1.1.0'), Null(''))
    >>> ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0), 'Linux i386')
    ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0), 'Linux i386')

    """

    ST_DIRTY, ST_CLEAN = 1, 2

    def __init__(
        self, objectIdentity, objectSyntax: SimpleAsn1Type = rfc1905.unSpecified
    ):
        """Create an ObjectType instance."""
        if not isinstance(objectIdentity, ObjectIdentity):
            raise SmiError(
                f"initializer should be ObjectIdentity instance, not {objectIdentity!r}"
            )
        self.__args = [objectIdentity, objectSyntax]
        self.__state = self.ST_DIRTY

    def __getitem__(self, i):
        """Return the i-th element of the ObjectType."""
        if self.__state & self.ST_CLEAN:
            return self.__args[i]
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def __str__(self):
        """Return a string representation of the object."""
        return self.prettyPrint()

    def __repr__(self):
        """Return a string representation of the object."""
        return "{}({})".format(
            self.__class__.__name__, ", ".join([repr(x) for x in self.__args])
        )

    def is_fully_resolved(self):
        """Returns `True` if MIB variable conversion has been performed.

        Returns
        -------
        bool
            `True` if MIB variable conversion has been performed.
        """
        return self.__state & self.ST_CLEAN

    def add_asn1_mib_source(self, *asn1Sources, **kwargs):
        """Adds path to a repository to search ASN.1 MIB files.

        Parameters
        ----------
        asn1Sources :
            one or more URL in form of :py:obj:`str` identifying local or
            remote ASN.1 MIB repositories. Path must include the *@mib@*
            component which will be replaced with MIB module name at the
            time of search.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            reference to itself

        Notes
        -----
        Please refer to :py:class:`~pysmi.reader.localfile.FileReader` and
        :py:class:`~pysmi.reader.httpclient.HttpReader` classes for
        in-depth information on ASN.1 MIB lookup.

        Examples
        --------
        >>> ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr')).add_asn1_mib_source('https://mibs.pysnmp.com/asn1/@mib@')
        ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr'))
        >>>

        """
        self.__args[0].addAsn1MibSource(*asn1Sources, **kwargs)
        return self

    def add_mib_source(self, *mibSources):
        """Adds path to repository to search PySNMP MIB files.

        Parameters
        ----------
        mibSources :
            one or more paths to search or Python package names to import
            and search for PySNMP MIB modules.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            reference to itself

        Notes
        -----
        Normally, ASN.1-to-Python MIB modules conversion is performed
        automatically through PySNMP/PySMI interaction. ASN1 MIB modules
        could also be manually compiled into Python via the
        `mibdump.py <https://www.pysnmp.com/pysmi/mibdump.html>`_
        tool.

        Examples
        --------
        >>> ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr')).add_mib_source('/opt/pysnmp/mibs', 'pysnmp_mibs')
        ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr'))
        >>>

        """
        self.__args[0].addMibSource(*mibSources)
        return self

    def load_mibs(self, *modNames):
        """Schedules search and load of given MIB modules.

        Parameters
        ----------
        modNames:
            one or more MIB module names to load up and use for MIB
            variables resolution purposes.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            reference to itself

        Examples
        --------
        >>> ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr')).load_mibs('IF-MIB', 'TCP-MIB')
        ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr'))
        >>>

        """
        self.__args[0].loadMibs(*modNames)
        return self

    def resolve_with_mib(
        self, mibViewController: MibViewController, ignoreErrors=True
    ) -> "ObjectType":
        """Perform MIB variable ID and associated value conversion.

        Parameters
        ----------
        mibViewController : :py:class:`~pysnmp.smi.view.MibViewController`
            class instance representing MIB browsing functionality.

        Other Parameters
        ----------------
        ignoreErrors: :py:class:`bool`
            If `True` (default), ignore MIB object name or value casting
            failures if possible.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            reference to itself

        Raises
        ------
        SmiError
           In case of fatal MIB handling error

        Notes
        -----
        Calling this method involves
        :py:meth:`~pysnmp.smi.rfc1902.ObjectIdentity.resolve_with_mib`
        method invocation.

        Examples
        --------
        >>> from pysmi.hlapi.v3arch.asyncio import varbinds
        >>> mibViewController = varbinds.MibViewControllerManager.get_mib_view_controller(engine.cache)
        >>> objectType = ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr'), 'Linux i386')
        >>> objectType.resolve_with_mib(mibViewController)
        ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr'), DisplayString('Linux i386'))
        >>> str(objectType)
        'SNMPv2-MIB::sysDescr."0" = Linux i386'
        >>>

        """
        if self.__state & self.ST_CLEAN:
            return self

        object_identity: ObjectIdentity = self.__args[0]
        object_identity.resolve_with_mib(mibViewController)

        MibScalar, MibTableColumn = mibViewController.mibBuilder.import_symbols(  # type: ignore
            "SNMPv2-SMI", "MibScalar", "MibTableColumn"
        )

        if not isinstance(object_identity.get_mib_node(), (MibScalar, MibTableColumn)):
            if ignoreErrors and not isinstance(self.__args[1], AbstractSimpleAsn1Item):
                raise SmiError(
                    f"MIB object {object_identity!r} is not OBJECT-TYPE (MIB not loaded?)"
                )
            self.__state |= self.ST_CLEAN
            return self

        if isinstance(
            self.__args[1],
            (
                rfc1905.UnSpecified,
                rfc1905.NoSuchObject,
                rfc1905.NoSuchInstance,
                rfc1905.EndOfMibView,
            ),
        ):
            self.__state |= self.ST_CLEAN
            return self

        try:
            keep_old_value = isinstance(self.__args[1], SimpleAsn1Type)
            if keep_old_value:
                old_value = self.__args[1]._value
                self.__args[1] = (
                    object_identity.get_mib_node()
                    .getSyntax()
                    .clone(tagSet=self.__args[1].getTagSet())
                )
                self.__args[1]._value = old_value  # force to keep the original value
            else:
                self.__args[1] = (
                    object_identity.get_mib_node().getSyntax().clone(self.__args[1])
                )
        except PyAsn1Error:
            err = "MIB object %r having type %r failed to cast value " "%r: %s" % (
                object_identity.prettyPrint(),
                object_identity.get_mib_node().getSyntax().__class__.__name__,
                self.__args[1],
                sys.exc_info()[1],
            )

            if not ignoreErrors or not isinstance(
                self.__args[1], AbstractSimpleAsn1Item
            ):
                raise SmiError(err)

        if rfc1902.ObjectIdentifier().isSuperTypeOf(
            self.__args[1], matchConstraints=False
        ):
            self.__args[1] = ObjectIdentity(self.__args[1]).resolve_with_mib(
                mibViewController
            )

        self.__state |= self.ST_CLEAN

        debug.logger & debug.FLAG_MIB and debug.logger(
            f"resolved {object_identity!r} syntax is {self.__args[1]!r}"
        )

        return self

    def prettyPrint(self):  # noqa: N802
        """Return a human-friendly representation of the object."""
        if self.__state & self.ST_CLEAN:
            return "{} = {}".format(
                self.__args[0].prettyPrint(), self.__args[1].prettyPrint()
            )
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    # Compatibility with PySNMP older versions
    deprecated_attributes = {
        "getOid": "get_oid",
        "getMibSymbol": "get_mib_symbol",
        "getMibNode": "get_mib_node",
        "getLabel": "get_label",
        "isFullyResolved": "is_fully_resolved",
        "addAsn1MibSource": "add_asn1_mib_source",
        "addMibSource": "add_mib_source",
        "loadMibs": "load_mibs",
        "resolveWithMib": "resolve_with_mib",
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


class NotificationType:
    """Create an object representing SNMP Notification.

    Instances of :py:class:`~pysnmp.smi.rfc1902.NotificationType` class are
    containers incorporating :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
    class instance (identifying particular notification) and a collection
    of MIB variables IDs that *NotificationOriginator* should gather
    and put into notification message.

    Typical notification is defined like this (from *IF-MIB.txt*):

    .. code-block:: asn1

       linkDown NOTIFICATION-TYPE
           OBJECTS { ifIndex, ifAdminStatus, ifOperStatus }
           STATUS  current
           DESCRIPTION
                  "A linkDown trap signifies that the SNMP entity..."
           ::= { snmpTraps 3 }

    Corresponding NotificationType instantiation would look like this:

    .. code-block:: python

        NotificationType(ObjectIdentity('IF-MIB', 'linkDown'))

    To retain similarity with SNMP variable-bindings,
    :py:class:`~pysnmp.smi.rfc1902.NotificationType` objects behave like
    a sequence of :py:class:`~pysnmp.smi.rfc1902.ObjectType` class
    instances.

    See :RFC:`1902#section-2` for more information on NOTIFICATION-TYPE SMI
    definitions.

    Parameters
    ----------
    objectIdentity: :py:class:`~pysnmp.smi.rfc1902.ObjectIdentity`
        Class instance representing MIB notification type identification.
    instanceIndex: :py:class:`~pysnmp.proto.rfc1902.ObjectName`
        Trailing part of MIB variables OID identification that represents
        concrete instance of a MIB variable. When notification is prepared,
        `instanceIndex` is appended to each MIB variable identification
        listed in NOTIFICATION-TYPE->OBJECTS clause.
    objects: dict
        Dictionary-like object that may return values by OID key. The
        `objects` dictionary is consulted when notification is being
        prepared. OIDs are taken from MIB variables listed in
        NOTIFICATION-TYPE->OBJECTS with `instanceIndex` part appended.

    Notes
    -----
        Actual notification type and MIB variables look up occurs
        upon :py:meth:`~pysnmp.smi.rfc1902.NotificationType.resolve_with_mib`
        invocation.

    Examples
    --------
    >>> from pysnmp.smi.rfc1902 import *
    >>> NotificationType(ObjectIdentity('1.3.6.1.6.3.1.1.5.3'))
    NotificationType(ObjectIdentity('1.3.6.1.6.3.1.1.5.3'), (), {})
    >>> NotificationType(ObjectIdentity('IP-MIB', 'linkDown'), ObjectName('3.5'))
    NotificationType(ObjectIdentity('1.3.6.1.6.3.1.1.5.3'), ObjectName('3.5'), {})

    """

    ST_DIRTY, ST_CLEAN = 1, 2

    def __init__(self, objectIdentity: ObjectIdentity, instanceIndex=(), objects={}):
        """Create a NotificationType instance."""
        if not isinstance(objectIdentity, ObjectIdentity):
            raise SmiError(
                f"initializer should be ObjectIdentity instance, not {objectIdentity!r}"
            )
        self.__objectIdentity = objectIdentity
        self.__instanceIndex = instanceIndex
        self.__objects = objects
        self.__varBinds = []
        self.__additionalVarBinds = []
        self.__state = self.ST_DIRTY

    def __getitem__(self, i):
        """Return the i-th element of the NotificationType."""
        if self.__state & self.ST_CLEAN:
            return self.__varBinds[i]
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def __repr__(self):
        """Return a string representation of the object."""
        return f"{self.__class__.__name__}({self.__objectIdentity!r}, {self.__instanceIndex!r}, {self.__objects!r})"

    def add_varbinds(self, *varBinds):
        """Appends variable-binding to notification.

        Parameters
        ----------
        varBinds : :py:class:`~pysnmp.smi.rfc1902.ObjectType`
            One or more :py:class:`~pysnmp.smi.rfc1902.ObjectType` class
            instances.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.NotificationType`
            reference to itself

        Notes
        -----
        This method can be used to add custom variable-bindings to
        notification message in addition to MIB variables specified
        in NOTIFICATION-TYPE->OBJECTS clause.

        Examples
        --------
        >>> nt = NotificationType(ObjectIdentity('IP-MIB', 'linkDown'))
        >>> nt.add_varbinds(ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)))
        NotificationType(ObjectIdentity('IP-MIB', 'linkDown'), (), {})
        >>>

        """
        debug.logger & debug.FLAG_MIB and debug.logger(
            f"additional var-binds: {varBinds!r}"
        )
        if self.__state & self.ST_CLEAN:
            raise SmiError("%s object is already sealed" % self.__class__.__name__)
        else:
            self.__additionalVarBinds.extend(varBinds)
        return self

    def add_asn1_mib_source(self, *asn1Sources, **kwargs):
        """Adds path to a repository to search ASN.1 MIB files.

        Parameters
        ----------
        asn1Sources :
            one or more URL in form of :py:obj:`str` identifying local or
            remote ASN.1 MIB repositories. Path must include the *@mib@*
            component which will be replaced with MIB module name at the
            time of search.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.NotificationType`
            reference to itself

        Notes
        -----
        Please refer to :py:class:`~pysmi.reader.localfile.FileReader` and
        :py:class:`~pysmi.reader.httpclient.HttpReader` classes for
        in-depth information on ASN.1 MIB lookup.

        Examples
        --------
        >>> NotificationType(ObjectIdentity('IF-MIB', 'linkDown'), (), {}).add_asn1_mib_source('https://mibs.pysnmp.com/asn1/@mib@')
        NotificationType(ObjectIdentity('IF-MIB', 'linkDown'), (), {})
        >>>

        """
        self.__objectIdentity.add_asn1_mib_source(*asn1Sources, **kwargs)
        return self

    def add_mib_source(self, *mibSources):
        """Adds path to repository to search PySNMP MIB files.

        Parameters
        ----------
        mibSources :
            one or more paths to search or Python package names to import
            and search for PySNMP MIB modules.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.NotificationType`
            reference to itself

        Notes
        -----
        Normally, ASN.1-to-Python MIB modules conversion is performed
        automatically through PySNMP/PySMI interaction. ASN1 MIB modules
        could also be manually compiled into Python via the
        `mibdump.py <https://www.pysnmp.com/pysmi/mibdump.html>`_
        tool.

        Examples
        --------
        >>> NotificationType(ObjectIdentity('IF-MIB', 'linkDown'), (), {}).add_mib_source('/opt/pysnmp/mibs', 'pysnmp_mibs')
        NotificationType(ObjectIdentity('IF-MIB', 'linkDown'), (), {})
        >>>

        """
        self.__objectIdentity.add_mib_source(*mibSources)
        return self

    def load_mibs(self, *modNames):
        """Schedules search and load of given MIB modules.

        Parameters
        ----------
        modNames:
            one or more MIB module names to load up and use for MIB
            variables resolution purposes.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.NotificationType`
            reference to itself

        Examples
        --------
        >>> NotificationType(ObjectIdentity('IF-MIB', 'linkDown'), (), {}).load_mibs('IF-MIB', 'TCP-MIB')
        NotificationType(ObjectIdentity('IF-MIB', 'linkDown'), (), {})
        >>>

        """
        self.__objectIdentity.load_mibs(*modNames)
        return self

    def is_fully_resolved(self):
        """Return if the object is fully resolved."""
        return self.__state & self.ST_CLEAN

    def resolve_with_mib(
        self, mibViewController: MibViewController, ignoreErrors=True
    ) -> "NotificationType":
        """Perform MIB variable ID conversion and notification objects expansion.

        Parameters
        ----------
        mibViewController : :py:class:`~pysnmp.smi.view.MibViewController`
            class instance representing MIB browsing functionality.

        Other Parameters
        ----------------
        ignoreErrors: :py:class:`bool`
            If `True` (default), ignore MIB object name or value casting
            failures if possible.

        Returns
        -------
        : :py:class:`~pysnmp.smi.rfc1902.NotificationType`
            reference to itself

        Raises
        ------
        SmiError
           In case of fatal MIB handling error

        Notes
        -----
        Calling this method might cause the following sequence of
        events (exact details depends on many factors):

        * :py:meth:`pysnmp.smi.rfc1902.ObjectIdentity.resolve_with_mib` is called
        * MIB variables names are read from NOTIFICATION-TYPE->OBJECTS clause,
          :py:class:`~pysnmp.smi.rfc1902.ObjectType` instances are created
          from MIB variable OID and `indexInstance` suffix.
        * `objects` dictionary is queried for each MIB variable OID,
          acquired values are added to corresponding MIB variable

        Examples
        --------
        >>> notificationType = NotificationType(ObjectIdentity('IF-MIB', 'linkDown'))
        >>> notificationType.resolve_with_mib(mibViewController)
        NotificationType(ObjectIdentity('IF-MIB', 'linkDown'), (), {})
        >>>

        """
        if self.__state & self.ST_CLEAN:
            return self

        self.__objectIdentity.resolve_with_mib(mibViewController)

        self.__varBinds.append(
            ObjectType(
                ObjectIdentity(v2c.apiTrapPDU.snmpTrapOID), self.__objectIdentity
            ).resolve_with_mib(mibViewController, ignoreErrors)
        )

        (SmiNotificationType,) = mibViewController.mibBuilder.import_symbols(
            "SNMPv2-SMI", "NotificationType"
        )

        mibNode = self.__objectIdentity.get_mib_node()

        varBindsLocation = {}

        if isinstance(mibNode, SmiNotificationType):
            for notificationObject in mibNode.getObjects():
                objectIdentity = ObjectIdentity(
                    *notificationObject + self.__instanceIndex
                ).resolve_with_mib(mibViewController, ignoreErrors)
                self.__varBinds.append(
                    ObjectType(
                        objectIdentity,
                        self.__objects.get(notificationObject, rfc1905.unSpecified),
                    ).resolve_with_mib(mibViewController, ignoreErrors)
                )
                varBindsLocation[objectIdentity] = len(self.__varBinds) - 1
        else:
            debug.logger & debug.FLAG_MIB and debug.logger(
                f"WARNING: MIB object {self.__objectIdentity!r} is not NOTIFICATION-TYPE (MIB not loaded?)"
            )

        for varBinds in self.__additionalVarBinds:
            if not isinstance(varBinds, ObjectType):
                varBinds = ObjectType(ObjectIdentity(varBinds[0]), varBinds[1])
            varBinds.resolve_with_mib(mibViewController, ignoreErrors)
            if varBinds[0] in varBindsLocation:
                self.__varBinds[varBindsLocation[varBinds[0]]] = varBinds
            else:
                self.__varBinds.append(varBinds)

        self.__additionalVarBinds = []

        self.__state |= self.ST_CLEAN

        debug.logger & debug.FLAG_MIB and debug.logger(
            f"resolved {self.__objectIdentity!r} into {self.__varBinds!r}"
        )

        return self

    def prettyPrint(self):  # noqa: N802
        """Return a human-friendly representation of the object."""
        if self.__state & self.ST_CLEAN:
            return " ".join(
                [
                    f"{x[0].prettyPrint()} = {x[1].prettyPrint()}"
                    for x in self.__varBinds
                ]
            )
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    def to_varbinds(self) -> "tuple[ObjectType, ...]":
        """Return a sequence of MIB variables."""
        if self.__state & self.ST_CLEAN:
            return tuple(self.__varBinds)
        else:
            raise SmiError("%s object not fully initialized" % self.__class__.__name__)

    # Old to new attribute mapping
    deprecated_attributes = {
        "addVarBinds": "add_varbinds",
        "addAsn1MibSource": "add_asn1_mib_source",
        "addMibSource": "add_mib_source",
        "loadMibs": "load_mibs",
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
