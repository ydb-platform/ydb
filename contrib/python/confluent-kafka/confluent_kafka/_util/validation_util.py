# Copyright 2022 Confluent Inc.
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

from ..cimpl import KafkaError

try:
    string_type = basestring
except NameError:
    string_type = str


class ValidationUtil:
    @staticmethod
    def check_multiple_not_none(obj, vars_to_check):
        for param in vars_to_check:
            ValidationUtil.check_not_none(obj, param)

    @staticmethod
    def check_not_none(obj, param):
        if getattr(obj, param) is None:
            raise ValueError("Expected %s to be not None" % (param,))

    @staticmethod
    def check_multiple_is_string(obj, vars_to_check):
        for param in vars_to_check:
            ValidationUtil.check_is_string(obj, param)

    @staticmethod
    def check_is_string(obj, param):
        param_value = getattr(obj, param)
        if param_value is not None and not isinstance(param_value, string_type):
            raise TypeError("Expected %s to be a string" % (param,))

    @staticmethod
    def check_kafka_errors(errors):
        if not isinstance(errors, list):
            raise TypeError("errors should be None or a list")
        for error in errors:
            if not isinstance(error, KafkaError):
                raise TypeError("Expected list of KafkaError")

    @staticmethod
    def check_kafka_error(error):
        if not isinstance(error, KafkaError):
            raise TypeError("Expected error to be a KafkaError")
