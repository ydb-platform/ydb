# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import copy
import datetime
import threading
from collections import deque
from collections.abc import MutableMapping, Sequence

from typing_extensions import deprecated


def ns_to_iso_str(nanoseconds):
    """Get an ISO 8601 string from time_ns value."""
    ts = datetime.datetime.fromtimestamp(
        nanoseconds / 1e9, tz=datetime.timezone.utc
    )
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def get_dict_as_key(labels):
    """Converts a dict to be used as a unique key"""
    return tuple(
        sorted(
            map(
                lambda kv: (
                    (kv[0], tuple(kv[1])) if isinstance(kv[1], list) else kv
                ),
                labels.items(),
            )
        )
    )


class BoundedList(Sequence):
    """An append only list with a fixed max size.

    Calls to `append` and `extend` will drop the oldest elements if there is
    not enough room.
    """

    def __init__(self, maxlen: int | None):
        self.dropped = 0
        self._dq = deque(maxlen=maxlen)  # type: deque
        self._lock = threading.Lock()

    def __deepcopy__(self, memo):
        copy_ = BoundedList(0)
        memo[id(self)] = copy_
        with self._lock:
            copy_.dropped = self.dropped
            copy_._dq = copy.deepcopy(self._dq, memo)
        return copy_

    def __repr__(self):
        return f"{type(self).__name__}({list(self._dq)}, maxlen={self._dq.maxlen})"

    def __getitem__(self, index):
        return self._dq[index]

    def __len__(self):
        return len(self._dq)

    def __iter__(self):
        with self._lock:
            return iter(list(self._dq))

    def append(self, item):
        with self._lock:
            if (
                self._dq.maxlen is not None
                and len(self._dq) == self._dq.maxlen
            ):
                self.dropped += 1
            self._dq.append(item)

    def extend(self, seq):
        with self._lock:
            if self._dq.maxlen is not None:
                to_drop = len(seq) + len(self._dq) - self._dq.maxlen
                if to_drop > 0:
                    self.dropped += to_drop
            self._dq.extend(seq)

    @classmethod
    def from_seq(cls, maxlen, seq):
        seq = tuple(seq)
        bounded_list = cls(maxlen)
        bounded_list.extend(seq)
        return bounded_list


@deprecated("Deprecated since version 1.4.0.")
class BoundedDict(MutableMapping):
    """An ordered dict with a fixed max capacity.

    Oldest elements are dropped when the dict is full and a new element is
    added.
    """

    def __init__(self, maxlen: int | None):
        if maxlen is not None:
            if not isinstance(maxlen, int):
                raise ValueError
            if maxlen < 0:
                raise ValueError
        self.maxlen = maxlen
        self.dropped = 0
        self._dict = {}  # type: dict
        self._lock = threading.Lock()  # type: threading.Lock

    def __repr__(self):
        return (
            f"{type(self).__name__}({dict(self._dict)}, maxlen={self.maxlen})"
        )

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        with self._lock:
            if self.maxlen is not None and self.maxlen == 0:
                self.dropped += 1
                return

            if key in self._dict:
                del self._dict[key]
            elif self.maxlen is not None and len(self._dict) == self.maxlen:
                del self._dict[next(iter(self._dict.keys()))]
                self.dropped += 1
            self._dict[key] = value

    def __delitem__(self, key):
        del self._dict[key]

    def __iter__(self):
        with self._lock:
            return iter(self._dict.copy())

    def __len__(self):
        return len(self._dict)

    @classmethod
    def from_map(cls, maxlen, mapping):
        mapping = dict(mapping)
        bounded_dict = cls(maxlen)
        for key, value in mapping.items():
            bounded_dict[key] = value
        return bounded_dict
