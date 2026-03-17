#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#


class MibInfo:
    #: actual MIB name
    name = ""

    #: possible alternative to MIB name
    alias = ""

    #: URL to MIB file
    path = ""

    #: MIB file name
    file = ""

    #: MIB file modification time
    mtime = 0

    #: module OID
    oid = ""

    #: MIB revision as `datetime`
    revision = None

    #: all OIDs defined in this module
    oids = ()

    #: MODULE-IDENTITY OID
    identity = ""

    #: Enterprise OID
    enterprise = ()

    #: MODULE-COMPLIANCE OIDs
    compliance = ()

    #: imported MIB names
    imported: tuple[str, ...] = ()

    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
