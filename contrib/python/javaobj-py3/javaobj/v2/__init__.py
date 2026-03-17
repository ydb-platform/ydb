#!/usr/bin/env python3
"""
Rewritten version of the un-marshalling process of javaobj.

The previous process had issues in some cases that

This package is based on the approach of the jdeserialize project (in Java)
See: https://github.com/frohoff/jdeserialize

The object transformer concept of javaobj has been adapted to work with this
approach.

This package should handle more files than before, in read-only mode.
The writing mode should be handled by the "classic" javaobj code.

:authors: Thomas Calmant
:license: Apache License 2.0
:version: 0.4.4
:status: Alpha

..

    Copyright 2024 Thomas Calmant

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

from . import api, beans, core, main, stream, transformers  # noqa: 401
from .main import load, loads  # noqa: 401

# ------------------------------------------------------------------------------

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"
