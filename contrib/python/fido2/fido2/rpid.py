# Copyright (c) 2018 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""
These functions validate RP_ID and APP_ID according to simplified TLD+1 rules,
using a bundled copy of the public suffix list fetched from:

  https://publicsuffix.org/list/public_suffix_list.dat

Advanced APP_ID values pointing to JSON files containing valid facets are not
supported by this implementation.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse


import io
import pkgutil
with io.BytesIO(pkgutil.get_data(__package__, "public_suffix_list.dat")) as f:
    suffixes = [
        entry
        for entry in (line.decode("utf8").strip() for line in f.readlines())
        if entry and not entry.startswith("//")
    ]


def verify_rp_id(rp_id: str, origin: str) -> bool:
    """Checks if a Webauthn RP ID is usable for a given origin.

    :param rp_id: The RP ID to validate.
    :param origin: The origin of the request.
    :return: True if the RP ID is usable by the origin, False if not.
    """
    if not rp_id:
        return False

    url = urlparse(origin)
    if url.scheme != "https":
        return False
    host = url.hostname
    if host == rp_id:
        return True
    if host and host.endswith("." + rp_id) and rp_id not in suffixes:
        return True
    return False
