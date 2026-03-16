#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import getpass
import os
import sys


def os_arch():
    """
    Return a tuple for the current the OS and architecture.
    """
    if sys.maxsize > 2 ** 32:
        arch = '64'
    else:
        arch = '32'

    sys_platform = str(sys.platform).lower()
    if sys_platform.startswith('linux'):
        os = 'linux'
    elif 'win32' in sys_platform:
        os = 'win'
    elif 'darwin' in sys_platform:
        os = 'mac'
    elif 'freebsd' in sys_platform:
        os = 'freebsd'
    else:
        raise Exception('Unsupported OS/platform %r' % sys_platform)
    return os, arch


#
# OS/Arch
#
current_os, current_arch = os_arch()
on_windows = current_os == 'win'
on_windows_32 = on_windows and current_arch == '32'
on_windows_64 = on_windows and current_arch == '64'
on_mac = current_os == 'mac'
on_linux = current_os == 'linux'
on_freebsd = current_os == 'freebsd'
on_posix = not on_windows and (on_mac or on_linux or on_freebsd)

current_os_arch = '%(current_os)s-%(current_arch)s' % locals()
noarch = 'noarch'
current_os_noarch = '%(current_os)s-%(noarch)s' % locals()

del os_arch


def is_on_macos_14_or_higher():
    """
    Return True if the current OS is macOS 14 or higher.
    It uses APFS by default and has a different behavior wrt. unicode and
    filesystem encodings.
    """
    import platform
    macos_ver = platform.mac_ver()
    macos_ver = macos_ver[0]
    macos_ver = macos_ver.split('.')
    return macos_ver > ['10', '14']


on_macos_14_or_higher = is_on_macos_14_or_higher()

del is_on_macos_14_or_higher


def has_case_sensitive_fs():
    """
    Return True if the current FS is case sensitive.

    Windows is not case sensitive, and while older macOS HPFS+ were POSIX and
    case sensitive by default, newer macOS use APFS which is no longer case
    sensitive by default.

    From https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/APFS_Guide/FAQ/FAQ.html
        How does Apple File System handle filenames?
        APFS accepts only valid UTF-8 encoded filenames for creation, and preserves
        both case and normalization of the filename on disk in all variants. APFS,
        like HFS+, is case-sensitive on iOS and is available in case-sensitive and
        case-insensitive variants on macOS, with case-insensitive being the default.
    """
    return not os.path.exists(__file__.upper())


is_case_sensitive_fs = has_case_sensitive_fs()

#
# Shared library file extensions
#
if on_windows:
    lib_ext = '.dll'
if on_mac:
    lib_ext = '.dylib'
if on_linux or on_freebsd:
    lib_ext = '.so'

#
# Python versions
#
_sys_v0 = sys.version_info[0]
py2 = _sys_v0 == 2
py3 = _sys_v0 == 3

_sys_v1 = sys.version_info[1]
py36 = py3 and _sys_v1 == 6
py37 = py3 and _sys_v1 == 7
py38 = py3 and _sys_v1 == 8
py39 = py3 and _sys_v1 == 9
py310 = py3 and _sys_v1 == 10

# Do not let Windows error pop up messages with default SetErrorMode
# See http://msdn.microsoft.com/en-us/library/ms680621(VS100).aspx
#
# SEM_FAILCRITICALERRORS:
# The system does not display the critical-error-handler message box.
# Instead, the system sends the error to the calling process.
#
# SEM_NOGPFAULTERRORBOX:
# The system does not display the Windows Error Reporting dialog.
if on_windows:
    import ctypes
    # 3 is SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX
    ctypes.windll.kernel32.SetErrorMode(3)  # @UndefinedVariable
