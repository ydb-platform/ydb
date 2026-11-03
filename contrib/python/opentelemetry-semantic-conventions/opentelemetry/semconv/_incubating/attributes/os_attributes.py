# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

OS_BUILD_ID: Final = "os.build_id"
"""
Unique identifier for a particular build or compilation of the operating system.
"""

OS_DESCRIPTION: Final = "os.description"
"""
Human readable (not intended to be parsed) OS version information, like e.g. reported by `ver` or `lsb_release -a` commands.
"""

OS_NAME: Final = "os.name"
"""
Human readable operating system name.
"""

OS_TYPE: Final = "os.type"
"""
The operating system type.
"""

OS_VERSION: Final = "os.version"
"""
The version string of the operating system as defined in [Version Attributes](/docs/resource/README.md#version-attributes).
"""


class OsTypeValues(Enum):
    WINDOWS = "windows"
    """Microsoft Windows."""
    LINUX = "linux"
    """Linux."""
    DARWIN = "darwin"
    """Apple Darwin."""
    FREEBSD = "freebsd"
    """FreeBSD."""
    NETBSD = "netbsd"
    """NetBSD."""
    OPENBSD = "openbsd"
    """OpenBSD."""
    DRAGONFLYBSD = "dragonflybsd"
    """DragonFly BSD."""
    HPUX = "hpux"
    """HP-UX (Hewlett Packard Unix)."""
    AIX = "aix"
    """AIX (Advanced Interactive eXecutive)."""
    SOLARIS = "solaris"
    """SunOS, Oracle Solaris."""
    Z_OS = "z_os"
    """Deprecated: Replaced by `zos`."""
    ZOS = "zos"
    """IBM z/OS."""
