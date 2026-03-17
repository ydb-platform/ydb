# Copyright (c) 2022, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

from typing import Tuple

version_info = (3, 1, 0, "final")


def _imapclient_version_string(vinfo: Tuple[int, int, int, str]) -> str:
    major, minor, micro, releaselevel = vinfo
    v = "%d.%d.%d" % (major, minor, micro)
    if releaselevel != "final":
        v += "-" + releaselevel
    return v


version = _imapclient_version_string(version_info)

maintainer = "IMAPClient Maintainers"
maintainer_email = "imapclient@groups.io"

author = "Menno Finlay-Smits"
author_email = "hello@menno.io"
