# -----------------------------------------------------------------------------
# Copyright (c) 2020, 2023, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# constructors.py
#
# Contains the constructors mandated by the Python Database API.
# -----------------------------------------------------------------------------

import datetime

from . import errors

# synonyms for the types mandated by the database API
Binary = bytes
Date = datetime.date
Timestamp = datetime.datetime


def Time(hour: int, minute: int, second: int) -> None:
    """
    Constructor mandated by the database API for creating a time value. Since
    Oracle doesn't support time only values, an exception is raised when this
    method is called.
    """
    errors._raise_err(errors.ERR_TIME_NOT_SUPPORTED)


def DateFromTicks(ticks: float) -> datetime.date:
    """
    Constructor mandated by the database API for creating a date value given
    the number of seconds since the epoch (January 1, 1970). This is equivalent
    to using datetime.date.fromtimestamp() and that should be used instead.
    """
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> None:
    """
    Constructor mandated by the database API for creating a time value given
    the number of seconds since the epoch (January 1, 1970). Since Oracle
    doesn't support time only values, an exception is raised when this method
    is called.
    """
    errors._raise_err(errors.ERR_TIME_NOT_SUPPORTED)


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """
    Constructor mandated by the database API for creating a timestamp value
    given the number of seconds since the epoch (January 1, 1970). This is
    equivalent to using datetime.datetime.fromtimestamp() and that should be
    used instead.
    """
    return datetime.datetime.fromtimestamp(ticks)
