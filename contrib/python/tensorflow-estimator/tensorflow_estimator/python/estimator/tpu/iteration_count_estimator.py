# Copyright 2019 The TensorFlow Authors. All Rights Reserved.
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
# =============================================================================
"""Estimator that uses past runtime samples to estimate iterations count.

The estimator helps simplify determining the number of iterations count to spend
on a given alloted time budget. The estimate will get adjusted over time as the
estimator learns more from collecting per iteration runtime samples.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections

import numpy as np
import tensorflow as tf

RuntimeCounter = collections.namedtuple(
    "RuntimeCounter", ["runtime_secs", "steps", "step_time_secs"])


class IterationCountEstimator(object):
  """Estimates iterations count using past iterations runtime.

  The estimator collects iterations elapsed time (in seconds) and store it into
  a circular buffer. As it learns enough samples, it computes the mean value of
  the past observed iterations elapsed time to estimate the number of iterations
  count to run within the alloted time budget in seconds.

  To keep the buffer from growing indefinitely, we limit the size by the virtue
  of using circular buffer. As it uses the mean of iterations runtime to compute
  the iterations count estimate, setting a larger buffer size will smooth out
  the estimation. Once the buffer is getting filled up, older values will be
  dequeued in FIFO order. Setting larger buffer size will make the estimator
  less sensitive to runtime fluctuations but will result in slower convergence.
  For faster convergence buffer size can be set smaller but more prone to
  runtime fluctuations.

  As a safety feature, the estimator will return default iterations value,
  when:
  1. The circular buffer is empty (initially).
  2. The user input is invalid.
  """

  def __init__(self, capacity=20):
    """Constructs a new `IterationsEstimator` instance.

    Args:
      capacity: Size of circular buffer to hold timer values. Each timer value
        represents the time spent on the last iterations.

    Raises:
      ValueError: If one or more parameters specified is invalid.
    """
    self._reset(capacity=capacity)

  def _reset(self, capacity=20):
    """Resets internal variables."""
    if capacity <= 0:
      raise ValueError("IterationCountEstimator `capacity` must be positive. "
                       "Actual:%d." % capacity)
    # A circular buffer with fixed capacity to store the observation time values
    # and once the buffer is full, the oldest value will be evicted.
    self._buffer_wheel = collections.deque([])
    self._capacity = capacity
    self._min_iterations = 1
    self._last_iterations = self._min_iterations
    self._sample_count = 0

  def _mean_runtime_secs(self):
    return np.mean(self._buffer_wheel, axis=0)[0] if self._buffer_wheel else 0

  def _mean_step_time_secs(self):
    return np.mean(self._buffer_wheel, axis=0)[2] if self._buffer_wheel else 0

  def _std_step_time_secs(self):
    return np.std(self._buffer_wheel, axis=0)[2] if self._buffer_wheel else 0

  def _diff_less_than_percentage(self, actual, target, percentage):
    """Checks if `actual` value is within a `percentage` to `target` value.

    Args:
      actual: Actual value.
      target: Target value.
      percentage: Max percentage threshold.

    Returns:
      True if the ABS(`actual` - `target`) is less than or equal to `percentage`
        , otherwise False.

    Raise:
      ValueError: If `total_secs` value is not positive.
    """
    if actual == 0:
      raise ValueError("Invalid `actual` value. Value must not be zero.")
    if target == 0:
      raise ValueError("Invalid `target` value. Value must not be zero.")
    return (float(abs(target - actual)) / target) <= percentage * 0.01

  def _is_step_time_stable(self):
    """Checks if the step time has stabilized.

    We define stability a function of small stdev and after running for some
    time.

    Returns:
      True if stability is reached, False otherwise.
    """
    std = self._std_step_time_secs()
    return std < 0.03 and self._sample_count > self._capacity

  def update(self, runtime_secs, count):
    """Updates the unit time spent per iteration.

    Args:
      runtime_secs: The total elapsed time in seconds.
      count: The number of iterations.
    """
    if runtime_secs <= 0.0:
      tf.compat.v1.logging.debug(
          "Invalid `runtime_secs`. Value must be positive. Actual:%.3f.",
          runtime_secs)
      return
    if count <= 0.0:
      tf.compat.v1.logging.debug(
          "Invalid samples `count`. Value must be positive. Actual:%d.", count)
      return

    if len(self._buffer_wheel) >= self._capacity:
      self._buffer_wheel.popleft()
    step_time_secs = float(runtime_secs) / count
    self._buffer_wheel.append(
        RuntimeCounter(
            runtime_secs=runtime_secs,
            steps=count,
            step_time_secs=step_time_secs))
    self._sample_count += 1

  def get(self, total_secs):
    """Gets the iterations count estimate.

    If recent predicted iterations are stable, re-use the previous value.
    Otherwise, update the prediction value based on the delta between the
    current prediction and the expected number of iterations as determined by
    the per-step runtime.

    Args:
      total_secs: The target runtime in seconds.

    Returns:
      The number of iterations as estimate.

    Raise:
      ValueError: If `total_secs` value is not positive.
    """
    if total_secs <= 0:
      raise ValueError(
          "Invalid `total_secs`. It must be positive number. Actual:%d" %
          total_secs)
    if not self._buffer_wheel:
      tf.compat.v1.logging.debug(
          "IterationCountEstimator has no sample(s). Returns min iterations:%d.",
          self._min_iterations)
      return self._min_iterations

    mean_runtime_secs = self._mean_runtime_secs()
    mean_step_time_secs = self._mean_step_time_secs()
    std_step_time_secs = self._std_step_time_secs()
    projected_iterations = total_secs / mean_step_time_secs
    last_runtime_secs = self._buffer_wheel[-1].runtime_secs
    delta_iterations = projected_iterations - self._last_iterations
    # Stabilizes the search once it is close enough to the target runtime and
    # the step time is stable within range bound.
    if ((self._diff_less_than_percentage(last_runtime_secs, total_secs, 10) or
         self._diff_less_than_percentage(mean_runtime_secs, total_secs, 5)) and
        self._is_step_time_stable()):
      delta_iterations = 0
    self._last_iterations += delta_iterations
    self._last_iterations = max(self._last_iterations, self._min_iterations)
    tf.compat.v1.logging.info(
        "IterationCountEstimator -- target_runtime:%.3fs. last_runtime:%.3fs. "
        "mean_runtime:%.3fs. last_step_time:%.3f. std_step_time:%.3f. "
        "mean_step_time:%.3fs. delta_steps:%.2f. prev_steps:%.2f. "
        "next_steps:%.2f.", total_secs, last_runtime_secs, mean_runtime_secs,
        self._buffer_wheel[-1].step_time_secs, std_step_time_secs,
        mean_step_time_secs, delta_iterations, self._buffer_wheel[-1].steps,
        self._last_iterations)
    return int(self._last_iterations + 0.5)
