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
# queue.pyx
#
# Cython file defining the base Queue implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseQueueImpl:

    def deq_many(self, uint32_t max_num_messages):
        errors._raise_not_supported("dequeuing multiple messages")

    def deq_one(self):
        errors._raise_not_supported("dequeuing a single message")

    def enq_many(self, list props_impls):
        errors._raise_not_supported("enqueueing multiple messages")

    def enq_one(self, BaseMsgPropsImpl props_impl):
        errors._raise_not_supported("enqueuing a single message")

    def initialize(self, BaseConnImpl conn_impl, str name,
                   BaseDbObjectImpl payload_type, bint is_json):
        errors._raise_not_supported("initializing a queue")


cdef class BaseDeqOptionsImpl:

    def get_condition(self):
        errors._raise_not_supported("getting the condition")

    def get_consumer_name(self):
        errors._raise_not_supported("getting the consumer name")

    def get_correlation(self):
        errors._raise_not_supported("getting the correlation")

    def get_message_id(self):
        errors._raise_not_supported("getting the message id")

    def get_mode(self):
        errors._raise_not_supported("getting the mode")

    def get_navigation(self):
        errors._raise_not_supported("getting the navigation")

    def get_transformation(self):
        errors._raise_not_supported("getting the transformation")

    def get_visibility(self):
        errors._raise_not_supported("getting the visibility")

    def get_wait(self):
        errors._raise_not_supported("getting the wait time")

    def set_condition(self, str value):
        errors._raise_not_supported("setting the condition")

    def set_consumer_name(self, str value):
        errors._raise_not_supported("setting the consumer name")

    def set_correlation(self, str value):
        errors._raise_not_supported("setting the correlation")

    def set_delivery_mode(self, uint16_t value):
        errors._raise_not_supported("setting the delivery mode")

    def set_mode(self, uint32_t value):
        errors._raise_not_supported("setting the mode")

    def set_message_id(self, bytes value):
        errors._raise_not_supported("setting the message id")

    def set_navigation(self, uint32_t value):
        errors._raise_not_supported("setting the navigation")

    def set_transformation(self, str value):
        errors._raise_not_supported("setting the transformation")

    def set_visibility(self, uint32_t value):
        errors._raise_not_supported("setting the visibility")

    def set_wait(self, uint32_t value):
        errors._raise_not_supported("setting the wait time")


cdef class BaseEnqOptionsImpl:

    def get_transformation(self):
        errors._raise_not_supported("getting the transformation")

    def get_visibility(self):
        errors._raise_not_supported("getting the visibility")

    def set_delivery_mode(self, uint16_t value):
        errors._raise_not_supported("setting the delivery mode")

    def set_transformation(self, str value):
        errors._raise_not_supported("setting the transformation")

    def set_visibility(self, uint32_t value):
        errors._raise_not_supported("setting the visibility")


cdef class BaseMsgPropsImpl:

    def get_num_attempts(self):
        errors._raise_not_supported("getting the number of attempts")

    def get_correlation(self):
        errors._raise_not_supported("getting the correlation")

    def get_delay(self):
        errors._raise_not_supported("getting the delay")

    def get_delivery_mode(self):
        errors._raise_not_supported("getting the delivery mode")

    def get_enq_time(self):
        errors._raise_not_supported("getting the enqueue time")

    def get_exception_queue(self):
        errors._raise_not_supported("getting the name of the exception queue")

    def get_expiration(self):
        errors._raise_not_supported("getting the expiration")

    def get_message_id(self):
        errors._raise_not_supported("getting the message id")

    def get_priority(self):
        errors._raise_not_supported("getting the priority")

    def get_state(self):
        errors._raise_not_supported("getting the message state")

    def set_correlation(self, str value):
        errors._raise_not_supported("setting the correlation")

    def set_delay(self, int32_t value):
        errors._raise_not_supported("setting the delay")

    def set_exception_queue(self, str value):
        errors._raise_not_supported("setting the name of the exception queue")

    def set_expiration(self, int32_t value):
        errors._raise_not_supported("setting the expiration")

    def set_payload_bytes(self, bytes value):
        errors._raise_not_supported("setting the payload from bytes")

    def set_payload_object(self, BaseDbObjectImpl value):
        errors._raise_not_supported(
            "setting the payload from a database object"
        )

    def set_priority(self, int32_t value):
        errors._raise_not_supported("setting the priority")

    def set_recipients(self, list value):
        errors._raise_not_supported("setting recipients list")
