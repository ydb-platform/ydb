#
# Copyright (c) SAS Institute Inc.
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
#


"""
Exceptions used by pymaven
"""


# Generic errors
class PymavenError(Exception):
    """Base class for all errors in PymavenError

    Do not raise directly, but make a subclass
    """
    _template = None

    def __init__(self, *args, **kwargs):
        if self._template is None:
            super(PymavenError, self).__init__(*args, **kwargs)
        else:
            super(PymavenError, self).__init__(
                self._template.format(*args, **kwargs))


# Maven repo errors
class RepositoryError(PymavenError):
    """Generic errors raised by maven repositories"""


class MissingPathError(RepositoryError):
    """Raised when a repository accesses a path that does not exist"""
    _template = "No such directory: {0}"


# Maven Client errors
class ClientError(PymavenError):
    """Generic errors raised by maven clients"""


class MissingArtifactError(ClientError):
    """Raised when a client tries to fetch an artifact that does not exist"""
    _template = "No artifact found matching '{0}'"


# Parser errors
class ParseError(PymavenError):
    """Generic error in parsing a format"""


class ArtifactParseError(ParseError):
    """Raised when an error is encountered parsing maven coordinate"""


class RestrictionParseError(ParseError):
    """Raised when an error is encountered parsing a restriction spec"""


class VersionRangeParseError(ParseError):
    """Raised when an error is encountered parsing a range spec"""
