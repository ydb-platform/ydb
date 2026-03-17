#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import TYPE_CHECKING


from pysnmp import debug
from pysnmp.proto import errind, error

if TYPE_CHECKING:
    from pysnmp.entity.engine import SnmpEngine


# rfc3415 3.2
# noinspection PyUnusedLocal
class Vacm:
    """Void Access Control Model."""

    ACCESS_MODEL_ID = 0

    def is_access_allowed(
        self,
        snmpEngine: "SnmpEngine",
        securityModel,
        securityName,
        securityLevel,
        viewType,
        contextName,
        variableName,
    ):
        """Return whether access is allowed to a MIB object."""
        debug.logger & debug.FLAG_ACL and debug.logger(
            f"isAccessAllowed: viewType {viewType} for variableName {variableName} - OK"
        )

        # rfc3415 3.2.5c
        return error.StatusInformation(errorIndication=errind.accessAllowed)
