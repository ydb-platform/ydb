# flake8: noqa
"""
LICENSE
=======

Copyright 2012 Jacob Alheid

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

import importlib.metadata

try:
    __version__ = importlib.metadata.version("humbledb")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0-dev"


# We only want to allow * imports for the most common classes. If you want
# anything else, import it directly.
__all__ = [
    "Index",
    "Mongo",
    "Document",
    "Embed",
]

# Shortcuts to pymongo index directions
import pymongo

# Shortcut to pytool.lang.UNSET
from pytool.lang import UNSET

DESC = pymongo.DESCENDING
ASC = pymongo.ASCENDING
del pymongo  # Clean up the namespace

# Exceptions module
# Import array and report framework modules. These need to be imported last or
# it causes with circular imports
from . import array, errors, report
from .document import Document, Embed

# Import shortcuts to HumbleDB document basics
from .index import Index
from .mongo import Mongo

# To make Pyflakes happy
array = array
report = report
errors = errors
UNSET = UNSET
