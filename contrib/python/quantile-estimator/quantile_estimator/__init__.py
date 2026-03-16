"""
Copyright 2013 matt.proud@gmail.com  (Matt T. Proud)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Python Implementation of Graham Cormode and S. Muthukrishnan's Effective
Computation of Biased Quantiles over Data Streams in ICDE'05
"""
import math
import time

from .version import __version__

_BUFFER_SIZE = 512


def time_ms():
    return time.time_ns() // 1_000_000


class Estimator:
    """Estimator estimates quantile values from sample streams in a time- and
    memory-efficient manner subject to allowed error constraints.
    """
    def __init__(self, *invariants):
        """Initialize an Estimator.

        Estimator is not concurrency safe.

        Attributes:
            invariants: A list of floating point doubles containing the target
                quantile value and allowed error.   [(0.5, 0.01), (0.99, 0.001)]
                are the default if none are provided, signifying that the median
                will be provided at a one percent error limit and the 99th
                percentile at the a 0.1 percent error limit.
        """
        if not invariants:
            self._invariants = _DEFAULT_INVARIANTS
        else:
            self._invariants = [_Quantile(q, e) for (q, e) in invariants]
        self._buffer = []
        self._head = None
        self._observations = 0
        self._items = 0

    def observe(self, value):
        """Samples an observation's value.

        Args:
            value: A numeric value signifying the value to be sampled.
        """
        self._buffer.append(value)
        if len(self._buffer) == _BUFFER_SIZE:
            self._flush()

    def query(self, rank):
        """Retrieves the value estimate for the requested quantile rank.

        The requested quantile rank must be registered in the estimator's
        invariants a priori!

        Args:
            rank: A floating point quantile rank along the interval [0, 1].

        Returns:
            A numeric value for the quantile estimate.
        """
        self._flush()

        current = self._head
        if not current:
            return float("nan")

        if rank == 0.0:
            return current._value

        if rank == 1.0:
            while current._successor:
                current = current._successor
            return current._value

        mid_rank = math.floor(rank * self._observations)
        max_rank = mid_rank + math.floor(self._invariant(mid_rank, self._observations) / 2)

        rank = 0.0
        while current._successor:
            rank += current._rank
            if rank + current._successor._rank + current._successor._delta > max_rank:
                return current._value

            current = current._successor

        return current._value

    def _flush(self):
        """Purges the buffer and commits all pending values into the estimator."""
        self._buffer.sort()
        self._replace_batch()
        self._buffer = []
        self._compress()

    def _replace_batch(self):
        """Incorporates all pending values into the estimator."""
        if not self._head and not self._buffer:
            return

        if not self._head:
            self._head, self._buffer = self._record(self._buffer[0], 1, 0, None), self._buffer[1:]

        rank = 0.0
        current = self._head

        for b in self._buffer:
            if b < self._head._value:
                self._head = self._record(b, 1, 0, self._head)

            while current._successor and current._value < b:
                rank += current._rank
                current = current._successor

            if not current._successor:
                current._successor = self._record(b, 1, 0, None)

            current._successor = self._record(b, 1, self._invariant(rank, self._observations) - 1, current._successor)

    def _record(self, value, rank, delta, successor):
        """Catalogs a sample."""
        self._observations += 1
        self._items += 1

        return _Sample(value, rank, delta, successor)

    def _invariant(self, rank, n):
        """Computes the delta value for the sample."""
        minimum = n + 1

        for i in self._invariants:
            delta = i._delta(rank, n)
            if delta < minimum:
                minimum = delta

        return math.floor(minimum)

    def _compress(self):
        """Prunes the cataloged observations."""
        rank = 0.0
        current = self._head

        while current and current._successor:
            if current._rank + current._successor._rank + current._successor._delta <= self._invariant(rank, self._observations):
                removed = current._successor

                current._value = removed._value
                current._rank += removed._rank
                current._delta = removed._delta
                current._successor = removed._successor

            rank += current._rank
            current = current._successor


class _Quantile:
    """_Quantile is an internal representation of an estimation target
    invariant.

    Attributes:
        quantile: A floating point value for the requested quantile along the
            [0, 1] interval.
        inaccuracy: A floating point value for the allowed error for the
            estimate along the [0, 1] interval.
    """
    def __init__(self, quantile, inaccuracy):
        self._quantile = quantile
        self._inaccuracy = inaccuracy
        self._coefficient_i = (2.0 * inaccuracy) / (1.0 - quantile) if quantile != 1. else float("nan")
        self._coefficient_ii = 2.0 * inaccuracy / quantile if quantile != .0 else float("nan")

    """Computes the delta for the observation."""
    def _delta(self, rank, n):
        if rank <= math.floor((self._quantile * n)):
            return self._coefficient_i * (n - rank)

        return self._coefficient_ii * rank


_DEFAULT_INVARIANTS = [_Quantile(0.50, 0.01), _Quantile(0.99, 0.001)]


class _Sample:
    """_Sample models an observational value."""
    def __init__(self, value, rank, delta, successor):
        self._value = value
        self._rank = rank
        self._delta = delta
        self._successor = successor


class TimeWindowEstimator:
    """Wrapper around Estimator.

    Maintains a ring buffer of Estimators to provide quantiles over a sliding windows of time.

    Original idea:
    https://github.com/prometheus/client_java/blob/master/simpleclient/src/main/java/io/prometheus/client/TimeWindowQuantiles.java
    """

    def __init__(self, *invariants, max_age_seconds=10 * 60, age_buckets=5):
        self.invariants = invariants
        self.max_age_seconds = max_age_seconds
        self.age_buckets = age_buckets
        self.ring_buckets = [self.get_new_bucket() for _ in range(self.age_buckets)]
        self.current_bucket = 0
        self.last_rotate_time_ms = time_ms()
        self.duration_between_rotates_ms = self.max_age_seconds * 1000 // age_buckets

        # needed for original aioprometheus Summary metric
        # https://github.com/claws/aioprometheus
        # TODO: probably need to drop it, as aioprometheus project looks abandoned
        self._observations = 0
        self._sum = 0
        self._invariants = self.ring_buckets[0]._invariants

    def get_new_bucket(self):
        return Estimator(*self.invariants)

    def observe(self, value):
        self.rotate_buckets()
        for bucket in self.ring_buckets:
            bucket.observe(value)

        self._observations += 1
        self._sum += value

    def query(self, rank):
        self.rotate_buckets()
        return self.ring_buckets[self.current_bucket].query(rank)

    def rotate_buckets(self):
        time_since_last_rotate_ms = time_ms() - self.last_rotate_time_ms
        while time_since_last_rotate_ms > self.duration_between_rotates_ms:
            self.ring_buckets[self.current_bucket] = self.get_new_bucket()
            self.current_bucket += 1
            if self.current_bucket >= self.age_buckets:
                self.current_bucket = 0

            time_since_last_rotate_ms -= self.duration_between_rotates_ms
            self.last_rotate_time_ms += self.duration_between_rotates_ms
