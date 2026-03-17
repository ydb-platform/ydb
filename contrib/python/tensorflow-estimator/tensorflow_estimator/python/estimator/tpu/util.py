# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
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
# ===================================================================
"""Utilities for the functionalities."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import re
import time
import numpy as np
import six
import tensorflow as tf

_ITERATIONS_PER_LOOP_VALUE_REGEX = re.compile(
    r'^(?P<value>[1-9]\d*)((?P<suffix>[s|m|h])$|$)')

IterationsPerLoopCounter = collections.namedtuple('IterationsPerLoopCounter',
                                                  ['value', 'unit'])


def check_positive_integer(value, name):
  """Checks whether `value` is a positive integer."""
  if not isinstance(value, (six.integer_types, np.integer)):
    raise TypeError('{} must be int, got {}'.format(name, type(value)))

  if value <= 0:
    raise ValueError('{} must be positive, got {}'.format(name, value))


def parse_iterations_per_loop(iterations_per_loop):
  """Parses the `iterations_per_loop` value.

  The parser expects the value of the `iterations_per_loop` value to be a
  positive integer value with unit:`count` or time-based value `<N><s|m|h>`
  where <N> is any positive integer and `s`, `m`, `h` are unit of time in
  seconds, minutes, hours respectively. Examples of valid values: `3600s`, `60m`
  , `1h`.

  Args:
    iterations_per_loop: Number of iterations or time alloted to spend on per
      device loop.

  Returns:
    A dictionary of `value` and `unit`. The `unit` value can be either a raw
    `count`, or time in `seconds`.
    {
      "value": <positive-integer>,
      "unit": <unit: `count` | `seconds`>
    }
  """
  m = _ITERATIONS_PER_LOOP_VALUE_REGEX.match(str(iterations_per_loop))
  if m is None:
    raise ValueError(
        'Invalid TPUConfig `iterations_per_loop` value. Value must be positive '
        'integer value or time-based value `<N><s|m|h>` where <N> is any'
        'positive integer and `s`, `m`, `h` are unit of time in seconds, '
        'minutes, hours respectively. Examples of valid values: `3600s`, `60m`,'
        ' `1h`.')
  unit_value = 'seconds' if m.group('suffix') in ['h', 'm', 's'] else 'count'
  value = int(m.group('value'))
  if m.group('suffix') == 'm':
    value *= 60
  elif m.group('suffix') == 'h':
    value *= 3600
  return IterationsPerLoopCounter(value, unit_value)


# TODO(b/118302029) Remove this copy of MultiHostDatasetInitializerHook after we
# release a tensorflow_estimator with MultiHostDatasetInitializerHook in
# python/estimator/util.py.
class MultiHostDatasetInitializerHook(tf.compat.v1.train.SessionRunHook):
  """Creates a SessionRunHook that initializes all passed iterators."""

  def __init__(self, dataset_initializers):
    self._initializers = dataset_initializers

  def after_create_session(self, session, coord):
    del coord
    start = time.time()
    session.run(self._initializers)
    tf.compat.v1.logging.info('Initialized dataset iterators in %d seconds',
                              time.time() - start)
