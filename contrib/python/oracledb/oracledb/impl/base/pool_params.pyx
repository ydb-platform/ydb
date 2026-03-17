#------------------------------------------------------------------------------
# Copyright (c) 2022, Oracle and/or its affiliates.
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# pool_params.pyx
#
# Cython file defining the base PoolParams implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

cdef class PoolParamsImpl(ConnectParamsImpl):

    def __init__(self):
        ConnectParamsImpl.__init__(self)
        self.min = 1
        self.max = 2
        self.increment = 1
        self.getmode = POOL_GETMODE_WAIT
        self.homogeneous = True
        self.ping_interval = 60
        self.ping_timeout = 5000

    cdef int _copy(self, ConnectParamsImpl other_params) except -1:
        """
        Internal method for copying attributes from another set of parameters.
        """
        cdef PoolParamsImpl pool_params = <PoolParamsImpl> other_params
        ConnectParamsImpl._copy(self, other_params)
        self.min = pool_params.min
        self.max = pool_params.max
        self.increment = pool_params.increment
        self.connectiontype = pool_params.connectiontype
        self.getmode = pool_params.getmode
        self.homogeneous = pool_params.homogeneous
        self.timeout = pool_params.timeout
        self.wait_timeout = pool_params.wait_timeout
        self.max_lifetime_session = pool_params.max_lifetime_session
        self.session_callback = pool_params.session_callback
        self.max_sessions_per_shard = pool_params.max_sessions_per_shard
        self.soda_metadata_cache = pool_params.soda_metadata_cache
        self.ping_interval = pool_params.ping_interval
        self.ping_timeout = pool_params.ping_timeout

    def copy(self):
        """
        Creates a copy of the connection parameters and returns it.
        """
        cdef PoolParamsImpl new_params
        new_params = PoolParamsImpl.__new__(PoolParamsImpl)
        new_params._copy(self)
        return new_params

    def set(self, dict args):
        """
        Sets the property values based on the supplied arguments. All values
        not supplied will be left unchanged.
        """
        ConnectParamsImpl.set(self, args)
        _set_uint_param(args, "min", &self.min)
        _set_uint_param(args, "max", &self.max)
        _set_uint_param(args, "increment", &self.increment)
        _set_obj_param(args, "connectiontype", self)
        _set_uint_param(args, "getmode", &self.getmode)
        _set_bool_param(args, "homogeneous", &self.homogeneous)
        _set_uint_param(args, "timeout", &self.timeout)
        _set_uint_param(args, "wait_timeout", &self.wait_timeout)
        _set_uint_param(args, "max_lifetime_session",
                        &self.max_lifetime_session)
        _set_obj_param(args, "session_callback", self)
        _set_uint_param(args, "max_sessions_per_shard",
                        &self.max_sessions_per_shard)
        _set_bool_param(args, "soda_metadata_cache", &self.soda_metadata_cache)
        _set_int_param(args, "ping_interval", &self.ping_interval)
        _set_uint_param(args, "ping_timeout", &self.ping_timeout)

        # if the pool is dynamically sized (min != max) then ensure that the
        # increment value is non-zero (as otherwise the pool would never grow!)
        if self.max != self.min and self.increment == 0:
            self.increment = 1
