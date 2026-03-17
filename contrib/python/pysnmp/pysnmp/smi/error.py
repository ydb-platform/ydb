#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.error import PyAsn1Error
from pysnmp.error import PySnmpError


class SmiError(PySnmpError, PyAsn1Error):
    """Base class for all SNMP SMI-related exceptions."""

    pass


class MibLoadError(SmiError):
    """Base class for all MIB loading exceptions."""

    pass


class MibNotFoundError(MibLoadError):
    """MIB file not found."""

    pass


class MibOperationError(SmiError):
    """Base class for all MIB operation exceptions."""

    def __init__(self, **kwargs):
        """MIB operation error."""
        self.__outArgs = kwargs

    def __str__(self):
        """Return error indication as a string."""
        return f"{self.__class__.__name__}({self.__outArgs})"

    def __getitem__(self, key):
        """Return error indication value by key."""
        return self.__outArgs[key]

    def __contains__(self, key):
        """Return True if key is present in the error indication."""
        return key in self.__outArgs

    def get(self, key, defVal=None):
        """Return error indication value by key."""
        return self.__outArgs.get(key, defVal)

    def keys(self):
        """Return error indication keys."""
        return self.__outArgs.keys()

    def update(self, d):
        """Update error indication."""
        self.__outArgs.update(d)


# Aligned with SNMPv2 PDU error-status values


class TooBigError(MibOperationError):
    """Value too big."""

    pass


class NoSuchNameError(MibOperationError):
    """No such name."""

    pass


class BadValueError(MibOperationError):
    """Bad value."""

    pass


class ReadOnlyError(MibOperationError):
    """Read-only."""

    pass


class GenError(MibOperationError):
    """General error."""

    pass


class NoAccessError(MibOperationError):
    """No access."""

    pass


class WrongTypeError(MibOperationError):
    """Wrong type."""

    pass


class WrongLengthError(MibOperationError):
    """Wrong length."""

    pass


class WrongEncodingError(MibOperationError):
    """Wrong encoding."""

    pass


class WrongValueError(MibOperationError):
    """Wrong value."""

    pass


class NoCreationError(MibOperationError):
    """No creation."""

    pass


class InconsistentValueError(MibOperationError):
    """Inconsistent value."""

    pass


class ResourceUnavailableError(MibOperationError):
    """Resource unavailable."""

    pass


class CommitFailedError(MibOperationError):
    """Commit failed."""

    pass


class UndoFailedError(MibOperationError):
    """Undo failed."""

    pass


class AuthorizationError(MibOperationError):
    """Authorization error."""

    pass


class NotWritableError(MibOperationError):
    """Not writable."""

    pass


class InconsistentNameError(MibOperationError):
    """Inconsistent name."""

    pass


# Aligned with SNMPv2 PDU exceptions or error-status values


class NoSuchObjectError(NoSuchNameError):
    """No such object."""

    pass


class NoSuchInstanceError(NoSuchNameError):
    """No such instance."""

    pass


class EndOfMibViewError(NoSuchNameError):
    """End of MIB view."""

    pass


# SNMP table management exceptions


class TableRowManagement(MibOperationError):
    """Base class for all SNMP table row management exceptions."""

    pass


class RowCreationWanted(TableRowManagement):
    """Row creation wanted."""

    pass


class RowDestructionWanted(TableRowManagement):
    """Row destruction wanted."""

    pass
