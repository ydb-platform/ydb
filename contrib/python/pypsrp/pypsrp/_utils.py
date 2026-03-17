# Copyright: (c) 2018, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import pkgutil
import typing
from urllib.parse import urlparse


def to_bytes(
    obj: typing.Any,
    encoding: str = "utf-8",
) -> bytes:
    """
    Makes sure the string is encoded as a byte string.

    :param obj: Python 2 string, Python 3 byte string, Unicode string to encode
    :param encoding: The encoding to use
    :return: The byte string that was encoded
    """
    if isinstance(obj, bytes):
        return obj

    return obj.encode(encoding)


def to_unicode(
    obj: typing.Any,
    encoding: str = "utf-8",
) -> str:
    """
    Makes sure the string is unicode string.

    :param obj: Python 2 string, Python 3 byte string, Unicode string to decode
    :param encoding: The encoding to use
    :return: THe unicode string the was decoded
    """
    if obj is None:
        obj = str(None)

    if isinstance(obj, str):
        return obj

    return obj.decode(encoding)


"""
Python 2 and 3 handle native strings differently, 2 is like a byte string while
3 uses unicode as the native string. The function to_string is used to easily
convert an existing string like object to the native version that is required
"""
to_string = to_unicode


def version_equal_or_newer(
    version: str,
    reference_version: str,
) -> bool:
    """
    Compares the 2 version strings and returns a bool that states whether
    version is newer than or equal to the reference version.

    This is quite strict and splits the string by . and compares the int
    values in them

    :param version: The version string to compare
    :param reference_version: The version string to check version against
    :return: True if version is newer than or equal to reference_version
    """
    version_parts = version.split(".")
    reference_version_parts = reference_version.split(".")

    # pad the version parts by 0 so the comparisons after won't fail with an
    # index error
    if len(version_parts) < len(reference_version_parts):
        diff = len(reference_version_parts) - len(version_parts)
        version_parts.extend(["0"] * diff)
    if len(reference_version_parts) < len(version_parts):
        diff = len(version_parts) - len(reference_version_parts)
        reference_version_parts.extend(["0"] * diff)

    newer = True
    for idx, version in enumerate(version_parts):
        current_version = int(reference_version_parts[idx])
        if int(version) < current_version:
            newer = False
            break
        elif int(version) > current_version:
            break

    return newer


def get_hostname(url: str) -> typing.Optional[str]:
    return urlparse(url).hostname


def get_pwsh_script(name: str) -> str:
    """
    Get the contents of a script stored in pypsrp/pwsh_scripts. Will also strip out any empty lines and comments to
    reduce the data we send across as much as possible.

    :param name: The filename of the script in pypsrp/pwsh_scripts to get.
    :return: The script contents.
    """
    script = to_unicode(pkgutil.get_data("pypsrp.pwsh_scripts", name))

    block_comment = False
    new_lines = []
    for line in script.splitlines():
        line = line.strip()
        if block_comment:
            block_comment = not line.endswith("#>")
        elif line.startswith("<#"):
            block_comment = True
        elif line and not line.startswith("#"):
            new_lines.append(line)

    return "\n".join(new_lines)
