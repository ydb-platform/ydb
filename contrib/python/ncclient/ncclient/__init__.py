# Copyright 2009 Shikhar Bhushan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__version__ = (0,6,12)

import sys

if sys.version_info < (3, 5):
    raise RuntimeError('You need Python 3.5+ for this module.')

class NCClientError(Exception):
    "Base type for all NCClient errors"
    pass

from . import _version
__version__ = _version.get_versions()['version']
