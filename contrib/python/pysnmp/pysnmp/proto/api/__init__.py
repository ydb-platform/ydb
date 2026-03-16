# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
"""
This module initializes the SNMP protocol API for different versions.

It imports the necessary submodules for SNMP version 1 and version 2c, and
defines constants for these versions. It also provides a mapping of protocol
modules to their respective versions and exposes a function to decode the
message version.

Attributes:
 * SNMP_VERSION_1 (int): Constant for SNMP version 1.
 * SNMP_VERSION_2C (int): Constant for SNMP version 2c.
 * PROTOCOL_MODULES (dict): Mapping of SNMP version constants to their
        respective protocol modules.
 * decodeMessageVersion (function): Function to decode the message version.
"""
from pysnmp.proto.api import v1, v2c, verdec

# Protocol versions
SNMP_VERSION_1 = 0
SNMP_VERSION_2C = 1
PROTOCOL_MODULES = {SNMP_VERSION_1: v1, SNMP_VERSION_2C: v2c}

decodeMessageVersion = verdec.decode_message_version  # noqa: N816
