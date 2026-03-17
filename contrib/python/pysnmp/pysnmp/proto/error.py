#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.error import PyAsn1Error
from pysnmp import debug
from pysnmp.error import PySnmpError


class ProtocolError(PySnmpError, PyAsn1Error):
    """Base class for all SNMP protocol errors."""

    pass


# SNMP v3 exceptions


class SnmpV3Error(ProtocolError):
    """Base class for all SNMPv3 exceptions."""

    pass


class StatusInformation(SnmpV3Error):
    """SNMPv3 status information object."""

    def __init__(self, **kwargs):
        """Create an SNMPv3 status information object."""
        SnmpV3Error.__init__(self)
        self.__errorIndication = kwargs
        debug.logger & (
            debug.FLAG_DSP | debug.FLAG_MP | debug.FLAG_SM | debug.FLAG_ACL
        ) and debug.logger("StatusInformation: %s" % kwargs)

    def __str__(self):
        """Return error indication as a string."""
        return str(self.__errorIndication)

    def __getitem__(self, key):
        """Return error indication value by key."""
        return self.__errorIndication[key]

    def __contains__(self, key):
        """Return True if key is present in the error indication."""
        return key in self.__errorIndication

    def get(self, key, defVal=None):
        """Return error indication value by key or default value."""
        return self.__errorIndication.get(key, defVal)


class CacheExpiredError(SnmpV3Error):
    """SNMPv3 cache expired exception."""

    pass


class InternalError(SnmpV3Error):
    """SNMPv3 internal error exception."""

    pass


class MessageProcessingError(SnmpV3Error):
    """SNMPv3 message processing error exception."""

    pass


class RequestTimeout(SnmpV3Error):
    """SNMPv3 request timeout exception."""

    pass
