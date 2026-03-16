# -----------------------------------------------------------------------------
# Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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
# utils.py
#
# Contains utility classes and methods.
# -----------------------------------------------------------------------------

from typing import Callable, Union

from . import base_impl
from . import driver_mode
from . import errors


def enable_thin_mode():
    """
    Makes python-oracledb be in Thin mode. After this method is called, Thick
    mode cannot be enabled. If python-oracledb is already in Thick mode, then
    calling ``enable_thin_mode()`` will fail. If connections have already been
    opened, or a connection pool created, in Thin mode, then calling
    ``enable_thin_mode()`` is a no-op.

    Since python-oracledb defaults to Thin mode, almost all applications do not
    need to call this method. However, because it bypasses python-oracledb's
    internal mode-determination heuristic, it may be useful for applications
    that are using standalone connections in multiple threads to concurrently
    create connections when the application starts.
    """
    with driver_mode.get_manager(requested_thin_mode=True):
        pass


def params_initer(f):
    """
    Decorator function which is used on the ConnectParams and PoolParams
    classes. It creates the implementation object using the implementation
    class stored on the parameter class. It first, however, calls the original
    method to ensure that the keyword parameters supplied are valid (the
    original method itself does nothing).
    """

    def wrapped_f(self, *args, **kwargs):
        f(self, *args, **kwargs)
        self._impl = self._impl_class()
        if kwargs:
            self._impl.set(kwargs)

    return wrapped_f


def params_setter(f):
    """
    Decorator function which is used on the ConnectParams and PoolParams
    classes. It calls the set() method on the parameter implementation object
    with the supplied keyword arguments. It first, however, calls the original
    method to ensure that the keyword parameters supplied are valid (the
    original method itself does nothing).
    """

    def wrapped_f(self, *args, **kwargs):
        f(self, *args, **kwargs)
        self._impl.set(kwargs)

    return wrapped_f


def register_protocol(protocol: str, hook_function: Callable) -> None:
    """
    Registers a user function to be called prior to connection or pool creation
    when an Easy Connect connection string prefixed with the specified protocol
    is being parsed internally by python-oracledb in Thin mode. The registered
    function will also be invoked by ConnectParams.parse_connect_string() in
    Thin and Thick modes. Your hook function is expected to find or construct a
    valid connection string. If the supplied function is None, the registration
    is removed.
    """
    if not isinstance(protocol, str):
        raise TypeError("protocol must be a string")
    if hook_function is not None and not callable(hook_function):
        raise TypeError("hook_function must be a callable")
    protocol = protocol.lower()
    if hook_function is None:
        base_impl.REGISTERED_PROTOCOLS.pop(protocol)
    else:
        base_impl.REGISTERED_PROTOCOLS[protocol] = hook_function


def verify_stored_proc_args(
    parameters: Union[list, tuple], keyword_parameters: dict
) -> None:
    """
    Verifies that the arguments to a call to a stored procedure or function
    are acceptable.
    """
    if parameters is not None and not isinstance(parameters, (list, tuple)):
        errors._raise_err(errors.ERR_ARGS_MUST_BE_LIST_OR_TUPLE)
    if keyword_parameters is not None and not isinstance(
        keyword_parameters, dict
    ):
        errors._raise_err(errors.ERR_KEYWORD_ARGS_MUST_BE_DICT)
