# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2017 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import secrets
from ._environ_config import (
    bool_var,
    config,
    generate_help,
    group,
    to_config,
    var,
)
from .exceptions import MissingEnvValueError


__all__ = [
    "MissingEnvValueError",
    "bool_var",
    "config",
    "generate_help",
    "group",
    "secrets",
    "to_config",
    "var",
]


def __getattr__(name: str) -> str:
    dunder_to_metadata = {
        "__version__": "version",
        "__description__": "summary",
        "__uri__": "",
        "__email__": "",
    }
    if name not in dunder_to_metadata:
        msg = f"module {__name__} has no attribute {name}"
        raise AttributeError(msg)

    import warnings

    from importlib.metadata import metadata

    warnings.warn(
        f"Accessing environ_config.{name} is deprecated and will be "
        "removed in a future release. Use importlib.metadata directly "
        "to query for environ_config's packaging metadata.",
        DeprecationWarning,
        stacklevel=2,
    )

    meta = metadata("environ-config")

    if name == "__uri__":
        return meta["Project-URL"].split(" ", 1)[-1]

    if name == "__email__":
        return meta["Author-email"].split("<", 1)[1].rstrip(">")

    return meta[dunder_to_metadata[name]]
