# Copyright 2011-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Motor, an asynchronous driver for MongoDB."""

version_tuple = (2, 5, 1)


def get_version_string():
    return '.'.join(map(str, version_tuple))


version = get_version_string()
"""Current version of Motor."""


try:
    import tornado
except ImportError:
    tornado = None
else:
    # For backwards compatibility with Motor 0.4, export Motor's Tornado classes
    # at module root. This may change in the future. First get __all__.
    from .motor_tornado import *

    # Now some classes that aren't in __all__ but might be expected.
    from .motor_tornado import (MotorCollection,
                                MotorDatabase,
                                MotorGridFSBucket,
                                MotorGridIn,
                                MotorGridOut)

    # Make "from motor import *" the same as "from motor.motor_tornado import *"
    from .motor_tornado import __all__
