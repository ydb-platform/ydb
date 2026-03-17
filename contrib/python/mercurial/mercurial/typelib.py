# typelib.py - type hint aliases and support
#
# Copyright 2022 Matt Harbison <matt_harbison@yahoo.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import typing

from typing import (
    Callable,
)

# Note: this is slightly different from pycompat.TYPE_CHECKING, as using
# pycompat causes the BinaryIO_Proxy type to be resolved to ``object`` when
# used as the base class during a pytype run.
TYPE_CHECKING = typing.TYPE_CHECKING


# The BinaryIO class provides empty methods, which at runtime means that
# ``__getattr__`` on the proxy classes won't get called for the methods that
# should delegate to the internal object.  So to avoid runtime changes because
# of the required typing inheritance, just use BinaryIO when typechecking, and
# ``object`` otherwise.
if TYPE_CHECKING:
    from typing import (
        BinaryIO,
    )

    from . import (
        node,
    )

    BinaryIO_Proxy = BinaryIO
    NodeConstants = node.sha1nodeconstants
else:
    from typing import Any

    BinaryIO_Proxy = object
    NodeConstants = Any

# scmutil.getuipathfn() related callback.
UiPathFn = Callable[[bytes], bytes]
