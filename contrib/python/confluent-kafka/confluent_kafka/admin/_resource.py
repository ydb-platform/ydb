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

from enum import Enum
from .. import cimpl as _cimpl


class ResourceType(Enum):
    """
    Enumerates the different types of Kafka resources.
    """
    UNKNOWN = _cimpl.RESOURCE_UNKNOWN  #: Resource type is not known or not set.
    ANY = _cimpl.RESOURCE_ANY  #: Match any resource, used for lookups.
    TOPIC = _cimpl.RESOURCE_TOPIC  #: Topic resource. Resource name is topic name.
    GROUP = _cimpl.RESOURCE_GROUP  #: Group resource. Resource name is group.id.
    BROKER = _cimpl.RESOURCE_BROKER  #: Broker resource. Resource name is broker id.
    TRANSACTIONAL_ID = _cimpl.RESOURCE_TRANSACTIONAL_ID  #: Transactional ID resource.

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value


class ResourcePatternType(Enum):
    """
    Enumerates the different types of Kafka resource patterns.
    """
    UNKNOWN = _cimpl.RESOURCE_PATTERN_UNKNOWN  #: Resource pattern type is not known or not set.
    ANY = _cimpl.RESOURCE_PATTERN_ANY  #: Match any resource, used for lookups.
    MATCH = _cimpl.RESOURCE_PATTERN_MATCH  #: Match: will perform pattern matching
    LITERAL = _cimpl.RESOURCE_PATTERN_LITERAL  #: Literal: A literal resource name
    PREFIXED = _cimpl.RESOURCE_PATTERN_PREFIXED  #: Prefixed: A prefixed resource name

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value
