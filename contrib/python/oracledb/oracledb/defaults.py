# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
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
# defaults.py
#
# Contains the Defaults class used for managing default values used throughout
# the module.
# -----------------------------------------------------------------------------

from . import base_impl
from . import __name__ as MODULE_NAME
from . import errors


class Defaults:
    """
    Identifies the default values used by the driver.
    """

    __module__ = MODULE_NAME

    def __init__(self) -> None:
        self._impl = base_impl.DEFAULTS

    @property
    def arraysize(self) -> int:
        """
        Specifies the default arraysize to use when cursors are created.
        """
        return self._impl.arraysize

    @arraysize.setter
    def arraysize(self, value: int):
        self._impl.arraysize = value

    @property
    def config_dir(self) -> str:
        """
        Specifies the directory to search for tnsnames.ora.
        """
        return self._impl.config_dir

    @config_dir.setter
    def config_dir(self, value: str):
        self._impl.config_dir = value

    @property
    def fetch_lobs(self) -> bool:
        """
        Specifies whether queries that contain LOBs should return LOB objects
        or their contents instead.
        """
        return self._impl.fetch_lobs

    @fetch_lobs.setter
    def fetch_lobs(self, value: str):
        self._impl.fetch_lobs = value

    @property
    def fetch_decimals(self) -> bool:
        """
        Specifies whether queries that contain numbers should return
        decimal.Decimal objects or floating point numbers.
        """
        return self._impl.fetch_decimals

    @fetch_decimals.setter
    def fetch_decimals(self, value: str):
        self._impl.fetch_decimals = value

    @property
    def prefetchrows(self) -> int:
        """
        Specifies the default number of rows to prefetch when cursors are
        executed.
        """
        return self._impl.prefetchrows

    @prefetchrows.setter
    def prefetchrows(self, value: int):
        self._impl.prefetchrows = value

    @property
    def stmtcachesize(self) -> int:
        """
        Specifies the default size of the statement cache.
        """
        return self._impl.stmtcachesize

    @stmtcachesize.setter
    def stmtcachesize(self, value: int):
        self._impl.stmtcachesize = value

    @property
    def program(self) -> str:
        """
        Specifies the program name connected to the Oracle Database.
        """
        return self._impl.program

    @program.setter
    def program(self, value: str):
        if base_impl.sanitize(value) != value:
            errors._raise_err(errors.ERR_INVALID_NETWORK_NAME, name="program")
        self._impl.program = value

    @property
    def machine(self) -> str:
        """
        Specifies the machine name connected to the Oracle Database.
        """
        return self._impl.machine

    @machine.setter
    def machine(self, value: str):
        if base_impl.sanitize(value) != value:
            errors._raise_err(errors.ERR_INVALID_NETWORK_NAME, name="machine")
        self._impl.machine = value

    @property
    def terminal(self) -> str:
        """
        Specifies the terminal identifier from which the connection originates.
        """
        return self._impl.terminal

    @terminal.setter
    def terminal(self, value: str):
        self._impl.terminal = value

    @property
    def osuser(self) -> str:
        """
        Specifies the os user that initiates the connection to the
        Oracle Database.
        """
        return self._impl.osuser

    @osuser.setter
    def osuser(self, value: str):
        if base_impl.sanitize(value) != value:
            errors._raise_err(errors.ERR_INVALID_NETWORK_NAME, name="osuser")
        self._impl.osuser = value

    @property
    def driver_name(self) -> str:
        """
        Specifies the driver used for the connection.
        """
        return self._impl.driver_name

    @driver_name.setter
    def driver_name(self, value: str):
        self._impl.driver_name = value


defaults = Defaults()
