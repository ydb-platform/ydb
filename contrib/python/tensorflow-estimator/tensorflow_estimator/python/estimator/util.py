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
# ==============================================================================
"""Utilities for Estimators."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import tensorflow as tf
from tensorflow.python.data.ops import dataset_ops
from tensorflow.python.util import function_utils

fn_args = function_utils.fn_args

# When we create a timestamped directory, there is a small chance that the
# directory already exists because another process is also creating these
# directories. In this case we just wait one second to get a new timestamp and
# try again. If this fails several times in a row, then something is seriously
# wrong.
MAX_DIRECTORY_CREATION_ATTEMPTS = 10


def parse_input_fn_result(result):
  """Gets features, labels, and hooks from the result of an Estimator input_fn.

  Args:
    result: output of an input_fn to an estimator, which should be one of:
      * A 'tf.data.Dataset' object: Outputs of `Dataset` object must be a tuple
        (features, labels) with same constraints as below.
      * A tuple (features, labels): Where `features` is a `Tensor` or a
        dictionary of string feature name to `Tensor` and `labels` is a `Tensor`
        or a dictionary of string label name to `Tensor`. Both `features` and
        `labels` are consumed by `model_fn`. They should satisfy the expectation
        of `model_fn` from inputs.

  Returns:
    Tuple of features, labels, and input_hooks, where features are as described
    above, labels are as described above or None, and input_hooks are a list
    of SessionRunHooks to be included when running.

  Raises:
    ValueError: if the result is a list or tuple of length != 2.
  """
  input_hooks = []
  if isinstance(result, dataset_ops.DatasetV2):
    iterator = dataset_ops.make_initializable_iterator(result)
    input_hooks.append(_DatasetInitializerHook(iterator))
    result = iterator.get_next()
  return parse_iterator_result(result) + (input_hooks,)


def parse_iterator_result(result):
  """Gets features, labels from result."""
  if isinstance(result, (list, tuple)):
    if len(result) != 2:
      raise ValueError(
          'input_fn should return (features, labels) as a len 2 tuple.')
    return result[0], result[1]
  return result, None


class _DatasetInitializerHook(tf.compat.v1.train.SessionRunHook):
  """Creates a SessionRunHook that initializes the passed iterator."""

  def __init__(self, iterator):
    self._iterator = iterator

  def begin(self):
    self._initializer = self._iterator.initializer

  def after_create_session(self, session, coord):
    del coord
    session.run(self._initializer)


class DistributedIteratorInitializerHook(tf.compat.v1.train.SessionRunHook):
  """Creates a SessionRunHook that initializes the passed iterator."""

  def __init__(self, iterator):
    self._iterator = iterator

  def begin(self):
    self._initializer = self._iterator.initialize()

  def after_create_session(self, session, coord):
    del coord
    session.run(self._initializer)


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
