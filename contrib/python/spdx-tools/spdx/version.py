
# Copyright (c) 2014 Ahmed H. Ismail
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from functools import total_ordering
import re


@total_ordering
class Version(object):
    """
    Version number composed of major and minor.
    Fields:
    - major: Major number, int.
    - minor: Minor number, int.
    """
    VERS_STR_REGEX = re.compile(r'(\d+)\.(\d+)')

    def __init__(self, major, minor):
        self.major = int(major)
        self.minor = int(minor)

    @classmethod
    def from_str(cls, value):
        """Constructs a Version from a string.
        Returns None if string not in N.N form where N represents a
        number.
        """
        m = cls.VERS_STR_REGEX.match(value)
        if m is not None:
            return cls(int(m.group(1)), int(m.group(2)))
        else:
            return None

    def __repr__(self):
        return 'Version' + repr((self.major, self.minor))

    def __str__(self):
        return 'SPDX-{major}.{minor}'.format(**self.__dict__)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.major == other.major and
            self.minor == other.minor
        )

    def __lt__(self, other):
        return (self.major < other.major
            or (self.major == other.major and self.minor < other.minor))
