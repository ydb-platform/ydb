# Copyright 2023 Confluent Inc.
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

from abc import ABC, abstractmethod
from .. import cimpl


class OffsetSpec(ABC):
    """
    Used in `AdminClient.list_offsets` to specify the desired offsets
    of the partition being queried.
    """
    _values = {}

    @property
    @abstractmethod
    def _value(self):
        pass

    @classmethod
    def _fill_values(cls):
        cls._max_timestamp = MaxTimestampSpec()
        cls._earliest = EarliestSpec()
        cls._latest = LatestSpec()
        cls._values.update({
            cimpl.OFFSET_SPEC_MAX_TIMESTAMP: cls._max_timestamp,
            cimpl.OFFSET_SPEC_EARLIEST: cls._earliest,
            cimpl.OFFSET_SPEC_LATEST: cls._latest,
        })

    @classmethod
    def earliest(cls):
        return cls._earliest

    @classmethod
    def latest(cls):
        return cls._latest

    @classmethod
    def max_timestamp(cls):
        return cls._max_timestamp

    @classmethod
    def for_timestamp(cls, timestamp):
        return TimestampSpec(timestamp)

    def __new__(cls, index):
        # Trying to instantiate returns one of the subclasses.
        # Subclasses can be instantiated but aren't accessible externally.
        if index < 0:
            return cls._values[index]
        else:
            return cls.for_timestamp(index)

    def __lt__(self, other):
        if not isinstance(other, OffsetSpec):
            return NotImplemented
        return self._value < other._value


class TimestampSpec(OffsetSpec):
    """
    Used in a `AdminClient.list_offsets` call to retrieve the earliest offset
    whose timestamp is greater than or equal to the given timestamp in the
    corresponding partition.

    Parameters
    ----------
    timestamp: int
        timestamp in milliseconds.
    """

    @property
    def _value(self):
        return self.timestamp

    def __new__(cls, _):
        return object.__new__(cls)

    def __init__(self, timestamp):
        self.timestamp = timestamp


class MaxTimestampSpec(OffsetSpec):
    """
    Used in a `AdminClient.list_offsets` call to retrieve the offset with the
    largest timestamp, that could not correspond to the latest one as timestamps
    can be specified client-side.
    """

    def __new__(cls):
        return object.__new__(cls)

    @property
    def _value(self):
        return cimpl.OFFSET_SPEC_MAX_TIMESTAMP


class LatestSpec(OffsetSpec):
    """
    Used in a `AdminClient.list_offsets` call to retrieve the queried partition latest offset.
    """

    def __new__(cls):
        return object.__new__(cls)

    @property
    def _value(self):
        return cimpl.OFFSET_SPEC_LATEST


class EarliestSpec(OffsetSpec):
    """
    Used in a `AdminClient.list_offsets` call to retrieve the queried partition earliest offset.
    """

    def __new__(cls):
        return object.__new__(cls)

    @property
    def _value(self):
        return cimpl.OFFSET_SPEC_EARLIEST


OffsetSpec._fill_values()


class ListOffsetsResultInfo:
    """
    ListOffsetsResultInfo
    Result of a `AdminClient.list_offsets` call associated to a partition.

    Parameters
    ----------
    offset: int
        The offset returned by the list_offsets call.
    timestamp: int
        The timestamp in milliseconds corresponding to the offset.
        Not available (-1) when querying for the earliest or the latest offsets.
    leader_epoch: int
        The leader epoch corresponding to the offset (optional).
    """
    def __init__(self, offset, timestamp, leader_epoch):
        self.offset = offset
        self.timestamp = timestamp
        self.leader_epoch = leader_epoch
        if self.leader_epoch < 0:
            self.leader_epoch = None
