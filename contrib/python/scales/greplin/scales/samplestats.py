# Copyright 2011 The scales Authors.
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

"""Sample statistics. Based on the Java code in Yammer metrics."""

import random
from math import sqrt, floor, exp

from .clock import getClock

def _bounded_exp(value):
  """ Emulate the Java version of exp """
  try:
    return exp(value)
  except OverflowError:
    return float("inf")

class Sampler(object):
  """Class to do simple sampling over values"""
  def __init__(self):
    self.min = float('inf')
    self.max = float('-inf')

  def __len__(self):
    return 0

  def samples(self):
    return []

  def update(self, value):
    self.min = min(self.min, value)
    self.max = max(self.max, value)

  @property
  def mean(self):
    """Return the sample mean."""
    if len(self) == 0:
      return float('NaN')
    arr = self.samples()
    return sum(arr) / float(len(arr))


  @property
  def stddev(self):
    """Return the sample standard deviation."""
    if len(self) < 2:
      return float('NaN')
    # The stupidest algorithm, but it works fine.
    try:
      arr = self.samples()
      mean = sum(arr) / len(arr)
      bigsum = 0.0
      for x in arr:
        bigsum += (x - mean)**2
      return sqrt(bigsum / (len(arr) - 1))
    except ZeroDivisionError:
      return float('NaN')


  def percentiles(self, percentiles):
    """Given a list of percentiles (floats between 0 and 1), return a
    list of the values at those percentiles, interpolating if
    necessary."""
    try:
      scores = [0.0]*len(percentiles)

      if self.count > 0:
        values = self.samples()
        values.sort()

        for i in range(len(percentiles)):
          p = percentiles[i]
          pos = p * (len(values) + 1)
          if pos < 1:
            scores[i] = values[0]
          elif pos > len(values):
            scores[i] = values[-1]
          else:
            upper, lower = values[int(pos - 1)], values[int(pos)]
            scores[i] = lower + (pos - floor(pos)) * (upper - lower)

      return scores
    except IndexError:
      return [float('NaN')] * len(percentiles)


class ExponentiallyDecayingReservoir(Sampler):
  """
    An exponentially-decaying random reservoir of. Uses Cormode et al's
    forward-decaying priority reservoir sampling method to produce a statistically representative
    sampling reservoir, exponentially biased towards newer entries.

    `Cormode et al. Forward Decay: A Practical Time Decay Model for Streaming Systems. ICDE '09
      http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf`

    This is a straight transliteration of the Yammer metrics version from java to python, whilst
    staring gently at the Cormode paper.
  """

  DEFAULT_SIZE = 1028
  DEFAULT_ALPHA = 0.015
  DEFAULT_RESCALE_THRESHOLD = 3600

  def __init__(self, size=DEFAULT_SIZE, alpha=DEFAULT_ALPHA, rescale_threshold=DEFAULT_RESCALE_THRESHOLD, clock=getClock()):
    """
      Creates a new ExponentiallyDecayingReservoir of 1028 elements, which offers a 99.9%
      confidence level with a 5% margin of error assuming a normal distribution, and an alpha
      factor of 0.015, which heavily biases the reservoir to the past 5 minutes of measurements.

      @param size  the number of samples to keep in the sampling reservoir
      @param alpha the exponential decay factor; the higher this is, the more biased the reservoir
                will be towards newer values
      @param rescale_threshold the time period over which to decay
    """
    super(ExponentiallyDecayingReservoir, self).__init__()

    self.values = {}
    self.alpha = alpha
    self.size = size
    self.clock = clock
    self.rescale_threshold = rescale_threshold
    self.count = 0
    self.startTime = self.clock.time()
    self.nextScaleTime = self.clock.time() + self.rescale_threshold

  def __len__(self):
    return min(self.size, self.count)

  def clear(self):
    """ Clear the samples. """
    self.__init__(size=self.size, alpha=self.alpha, clock=self.clock)

  def update(self, value):
    """
      Adds an old value with a fixed timestamp to the reservoir.
      @param value     the value to be added
    """
    super(ExponentiallyDecayingReservoir, self).update(value)

    timestamp = self.clock.time()

    self.__rescaleIfNeeded()
    priority = self.__weight(timestamp - self.startTime) / random.random()

    self.count += 1
    if (self.count <= self.size):
      self.values[priority] = value
    else:
      first = min(self.values)

      if first < priority and priority not in self.values:
        self.values[priority] = value
        while first not in self.values:
          first = min(self.values)

        del self.values[first]

  def __rescaleIfNeeded(self):
    now = self.clock.time()
    nextTick = self.nextScaleTime
    if now >= nextTick:
      self.__rescale(now, nextTick)

  def __weight(self, t):
    weight = self.alpha * t
    return _bounded_exp(weight)

  def __rescale(self, now, nextValue):
    if self.nextScaleTime == nextValue:
      self.nextScaleTime = now + self.rescale_threshold
      oldStartTime = self.startTime;
      self.startTime = self.clock.time()
      keys = list(self.values.keys())
      keys.sort()
      delKeys = []

      for key in keys:
        value = self.values[key]
        delKeys.append(key)
        newKey = key * _bounded_exp(-self.alpha * (self.startTime - oldStartTime))
        self.values[newKey] = value

      for key in delKeys:
        del self.values[key]

      self.count = len(self.values)

  def samples(self):
    return list(self.values.values())[:len(self)]

class UniformSample(Sampler):
  """A uniform sample of values over time."""

  def __init__(self):
    """Create an empty sample."""
    super(UniformSample, self).__init__()

    self.sample = [0.0] * 1028
    self.count = 0

  def clear(self):
    """Clear the sample."""
    for i in range(len(self.sample)):
      self.sample[i] = 0.0
    self.count = 0

  def __len__(self):
    """Number of samples stored."""
    return min(len(self.sample), self.count)

  def update(self, value):
    """Add a value to the sample."""
    super(UniformSample, self).update(value)

    self.count += 1
    c = self.count
    if c < len(self.sample):
      self.sample[c-1] = value
    else:
      r = random.randint(0, c)
      if r < len(self.sample):
        self.sample[r] = value


  def __iter__(self):
    """Return an iterator of the values in the sample."""
    return iter(self.sample[:len(self)])

  def samples(self):
    return self.sample[:len(self)]

# vim: set et fenc=utf-8 ff=unix sts=2 sw=2 ts=2 :
