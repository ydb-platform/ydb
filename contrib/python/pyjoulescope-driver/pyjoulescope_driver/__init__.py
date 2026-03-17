# Copyright 2022 Jetperch LLC
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

from .version import *
from . import time64
from .record import Record

try:
    from .binding import Driver, ElementType, Field, ErrorCode, LogLevel, SubscribeFlags, calibration_hash
except (ModuleNotFoundError, ImportError):
    print('Could not import cython binding')


__all__ = [
    'Driver', 'Record',
    'ElementType', 'Field', 'ErrorCode', 'LogLevel', 'SubscribeFlags',
    'calibration_hash',
    'time64',
    '__version__', '__title__', '__description__', '__url__',
    '__author__', '__author_email__', '__license__',
    '__copyright__']
