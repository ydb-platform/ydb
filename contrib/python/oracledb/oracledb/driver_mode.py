# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2023 Oracle and/or its affiliates.
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
# driver_mode.py
#
# Contains a simple method for checking and returning which mode the driver is
# currently using. The driver only supports creating connections and pools with
# either the thin implementation or the thick implementation, not both
# simultaneously.
# -----------------------------------------------------------------------------

import threading

from . import errors


# The DriverModeHandler class is used to manage which mode the driver is using.
#
# The "thin_mode" flag contains the current state:
#     None: neither thick nor thin implementation has been used yet
#     False: thick implementation is being used
#     True: thin implementation is being used
#
# The "requested_thin_mode" flag is set to the mode that is being requested:
#     False: thick implementation is being initialized
#     True: thin implementation is being initialized
class DriverModeManager:
    """
    Manages the mode the driver is using. The "thin_mode" flag contains the
    current state:
        None: neither thick nor thin implementation has been used yet
        False: thick implementation is being used
        True: thin implementation is being used
    The "requested_thin_mode" is set to the mode that is being requested, but
    only while initialization is taking place (otherwise, it contains the value
    None):
        False: thick implementation is being initialized
        True: thin implementation is being initialized
    The condition is used to ensure that only one thread is performing
    initialization.
    """

    def __init__(self):
        self.thin_mode = None
        self.requested_thin_mode = None
        self.condition = threading.Condition()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        with self.condition:
            if (
                exc_type is None
                and exc_value is None
                and exc_tb is None
                and self.requested_thin_mode is not None
            ):
                self.thin_mode = self.requested_thin_mode
            self.requested_thin_mode = None
            self.condition.notify()

    @property
    def thin(self):
        if self.requested_thin_mode is not None:
            return self.requested_thin_mode
        return self.thin_mode


manager = DriverModeManager()


def get_manager(requested_thin_mode=None):
    """
    Returns the manager, but only after ensuring that no other threads are
    attempting to initialize the mode.
    """
    with manager.condition:
        if manager.thin_mode is None:
            if manager.requested_thin_mode is not None:
                manager.condition.wait()
            if manager.thin_mode is None:
                if requested_thin_mode is None:
                    manager.requested_thin_mode = True
                else:
                    manager.requested_thin_mode = requested_thin_mode
        elif (
            requested_thin_mode is not None
            and requested_thin_mode != manager.thin_mode
        ):
            if requested_thin_mode:
                errors._raise_err(errors.ERR_THICK_MODE_ENABLED)
            else:
                errors._raise_err(errors.ERR_THIN_CONNECTION_ALREADY_CREATED)
    return manager


def is_thin_mode() -> bool:
    """
    Return a boolean specifying whether the driver is using thin mode (True) or
    thick mode (False).

    Immediately after python-oracledb is imported, this function will return
    True indicating that python-oracledb defaults to Thin mode. If
    oracledb.init_oracle_client() is called successfully, then a subsequent
    call to is_thin_mode() will return False indicating that Thick mode is
    enabled.  Once the first standalone connection or connection pool is
    created succesfully, or a call to oracledb.init_oracle_client() is made
    successfully, then python-oracledb's mode is fixed and the value returned
    by is_thin_mode() will never change for the lifetime of the process.

    """
    if manager.thin_mode is not None:
        return manager.thin_mode
    return True
