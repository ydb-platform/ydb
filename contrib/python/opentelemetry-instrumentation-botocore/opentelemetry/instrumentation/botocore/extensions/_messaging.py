# Copyright The OpenTelemetry Authors
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

import logging
from typing import Any, MutableMapping

from opentelemetry.propagate import get_global_textmap, inject
from opentelemetry.propagators.textmap import CarrierT, Setter

_logger = logging.getLogger(__name__)

_MAX_MESSAGE_ATTRIBUTES = 10


class MessageAttributesSetter(Setter[CarrierT]):
    def set(self, carrier: CarrierT, key: str, value: str):
        carrier[key] = {
            "DataType": "String",
            "StringValue": value,
        }


message_attributes_setter = MessageAttributesSetter()


def inject_propagation_context(
    carrier: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    if carrier is None:
        carrier = {}

    fields = get_global_textmap().fields
    if len(carrier.keys()) + len(fields) <= _MAX_MESSAGE_ATTRIBUTES:
        inject(carrier, setter=message_attributes_setter)
    else:
        _logger.warning(
            "botocore instrumentation: cannot set context propagation on "
            "SQS/SNS message due to maximum amount of MessageAttributes"
        )

    return carrier
