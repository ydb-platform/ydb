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
# subscr.pyx
#
# Cython file defining the base Subscription implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseSubscrImpl:

    def register_query(self, str sql, object args):
        errors._raise_not_supported("registering a query on a subscription")

    def subscribe(self, object subscr, BaseConnImpl conn_impl):
        errors._raise_not_supported("creating a subscription")

    def unsubscribe(self, BaseConnImpl conn_impl):
        errors._raise_not_supported("destroying a subscription")


cdef class Message:
    pass


cdef class MessageQuery:
    pass


cdef class MessageRow:
    pass


cdef class MessageTable:
    pass
