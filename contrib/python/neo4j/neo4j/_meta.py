# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import platform
import sys

from . import _typing as t
from ._codec.packstream import RUST_AVAILABLE


if t.TYPE_CHECKING:
    _FuncT = t.TypeVar("_FuncT", bound=t.Callable)


# Can be automatically overridden in builds
package = "neo4j"
version = "6.1.0"


def _compute_bolt_agent() -> dict[str, str]:
    def format_version_info(version_info):
        return "{}.{}.{}-{}-{}".format(*version_info)

    language = "Python"
    if RUST_AVAILABLE:
        language += "-Rust"

    return {
        "product": f"neo4j-python/{version}",
        "platform": (
            f"{platform.system() or 'Unknown'} "
            f"{platform.release() or 'unknown'}; "
            f"{platform.machine() or 'unknown'}"
        ),
        "language": f"{language}/{format_version_info(sys.version_info)}",
        "language_details": (
            f"{platform.python_implementation()}; "
            f"{format_version_info(sys.implementation.version)} "
            f"({', '.join(platform.python_build())}) "
            f"[{platform.python_compiler()}]"
        ),
    }


BOLT_AGENT_DICT = _compute_bolt_agent()


def _compute_user_agent() -> str:
    return (
        f"{BOLT_AGENT_DICT['product']} "
        f"{BOLT_AGENT_DICT['language']} "
        f"({sys.platform})"
    )


USER_AGENT = _compute_user_agent()


# Undocumented but exposed.
# Other official drivers also provide means to access the default user agent.
# Hence, we'll leave this here for now.
def get_user_agent():
    """
    Obtain the driver's default user agent string.

    The user agent is sent to the server after a successful handshake.
    """
    return USER_AGENT


def _id(x):
    return x


def copy_signature(_: _FuncT) -> t.Callable[[t.Callable], _FuncT]:
    return _id
