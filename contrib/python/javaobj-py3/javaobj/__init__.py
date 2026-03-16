#!/usr/bin/python
# -- Content-Encoding: utf-8 --
"""
Provides functions for reading and writing (writing is WIP currently) Java
objects serialized or will be deserialized by ObjectOutputStream. This form of
object representation is a standard data interchange format in Java world.

javaobj module exposes an API familiar to users of the standard library
marshal, pickle and json modules.

See:
http://download.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html

:authors: Volodymyr Buell, Thomas Calmant
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

# Imports giving access to what the javaobj module provides
from javaobj.v1.beans import *  # noqa: F401,F403
from javaobj.v1.core import *  # noqa: F401,F403
from javaobj.v1.transformers import *  # noqa: F401,F403

# ------------------------------------------------------------------------------

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"
