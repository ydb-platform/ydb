# -----------------------------------------------------------------------------
#  Copyright (c) The Jupyter Development Team
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE, distributed as part of this software.
# -----------------------------------------------------------------------------
import sys

from jupyter_client import localinterfaces


def test_load_ips():
    # Override the machinery that skips it if it was called before
    localinterfaces._load_ips.called = False  # type:ignore[attr-defined]

    # Just check this doesn't error
    localinterfaces._load_ips(suppress_exceptions=False)

    localinterfaces.is_local_ip("8.8.8.8")
    localinterfaces.is_public_ip("127.0.0.1")
    ips = localinterfaces.local_ips()
    assert "127.0.0.1" in ips

    localinterfaces._load_ips_gethostbyname()
    localinterfaces._load_ips_dumb()

    if sys.platform == "linux":
        localinterfaces._load_ips_ip()
        localinterfaces._load_ips_ifconfig()
