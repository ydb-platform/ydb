#------------------------------------------------------------------------------
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# pool.pyx
#
# Cython file defining the base Pool implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

cdef class BasePoolImpl:

    def acquire(self, str user, str password, str cclass, uint32_t purity,
                str tag, bint matchanytag, list shardingkey,
                list supershardingkey):
        errors._raise_not_supported("acquiring a connection from a pool")

    def close(self, bint force):
        errors._raise_not_supported("closing a pool")

    def drop(self, conn_impl):
        errors._raise_not_supported("dropping a connection from a pool")

    def get_busy_count(self):
        errors._raise_not_supported(
            "getting the number of busy connections in a pool"
        )

    def get_getmode(self):
        errors._raise_not_supported("getting the 'get' mode of a pool")

    def get_max_lifetime_session(self):
        errors._raise_not_supported(
            "getting the maximum lifetime of a connection in a pool"
        )

    def get_max_sessions_per_shard(self):
        errors._raise_not_supported(
            "getting the maximum sessions per shard in a pool"
        )

    def get_open_count(self):
        errors._raise_not_supported(
            "getting the number of connections open in a pool"
        )

    def get_ping_interval(self):
        errors._raise_not_supported("getting the ping interval of a pool")

    def get_soda_metadata_cache(self):
        errors._raise_not_supported(
            "getting whether the SODA metadata cache is enabled"
        )

    def get_stmt_cache_size(self):
        errors._raise_not_supported(
            "getting the size of the statement cache in a pool"
        )

    def get_timeout(self):
        errors._raise_not_supported(
            "getting the timeout for idle connections in a pool"
        )

    def get_wait_timeout(self):
        errors._raise_not_supported("getting the wait timeout for a pool")

    def reconfigure(self, uint32_t min, uint32_t max, uint32_t increment):
        errors._raise_not_supported("reconfiguring a pool")

    def set_getmode(self, uint8_t value):
        errors._raise_not_supported("setting the 'get' mode of a pool")

    def set_max_lifetime_session(self, uint32_t value):
        errors._raise_not_supported(
            "setting the maximum lifetime of a connection a pool"
        )

    def set_max_sessions_per_shard(self, uint32_t value):
        errors._raise_not_supported("setting the maximum sessions per shard")

    def set_ping_interval(self, int value):
        errors._raise_not_supported("setting the ping interval")

    def set_soda_metadata_cache(self, bint value):
        errors._raise_not_supported(
            "setting whether the SODA metadata cache is enabled"
        )

    def set_stmt_cache_size(self, uint32_t value):
        errors._raise_not_supported(
            "setting the size of the statement cache in a pool"
        )

    def set_timeout(self, uint32_t value):
        errors._raise_not_supported(
            "setting the timeout for idle connections in a pool"
        )

    def set_wait_timeout(self, uint32_t value):
        errors._raise_not_supported("setting the wait timeout for a pool")
