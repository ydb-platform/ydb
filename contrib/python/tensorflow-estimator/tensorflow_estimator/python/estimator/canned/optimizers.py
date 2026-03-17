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
"""Methods related to optimizers used in canned_estimators."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import inspect
import six
import tensorflow as tf
from tensorflow.python.keras.optimizer_v2 import adagrad as adagrad_v2
from tensorflow.python.keras.optimizer_v2 import adam as adam_v2
from tensorflow.python.keras.optimizer_v2 import ftrl as ftrl_v2
from tensorflow.python.keras.optimizer_v2 import gradient_descent as gradient_descent_v2
from tensorflow.python.keras.optimizer_v2 import optimizer_v2
from tensorflow.python.keras.optimizer_v2 import rmsprop as rmsprop_v2

_OPTIMIZER_CLS_NAMES = {
    'Adagrad': tf.compat.v1.train.AdagradOptimizer,
    'Adam': tf.compat.v1.train.AdamOptimizer,
    'Ftrl': tf.compat.v1.train.FtrlOptimizer,
    'RMSProp': tf.compat.v1.train.RMSPropOptimizer,
    'SGD': tf.compat.v1.train.GradientDescentOptimizer,
}

_OPTIMIZER_CLS_NAMES_V2 = {
    'Adagrad': adagrad_v2.Adagrad,
    'Adam': adam_v2.Adam,
    'Ftrl': ftrl_v2.Ftrl,
    'RMSProp': rmsprop_v2.RMSProp,
    'SGD': gradient_descent_v2.SGD,
}

# The default learning rate of 0.05 is a historical artifact of the initial
# implementation, but seems a reasonable choice.
_LEARNING_RATE = 0.05


def get_optimizer_instance(opt, learning_rate=None):
  """Returns an optimizer instance.

  Supports the following types for the given `opt`:
  * An `Optimizer` instance: Returns the given `opt`.
  * A string: Creates an `Optimizer` subclass with the given `learning_rate`.
    Supported strings:
    * 'Adagrad': Returns an `AdagradOptimizer`.
    * 'Adam': Returns an `AdamOptimizer`.
    * 'Ftrl': Returns an `FtrlOptimizer`.
    * 'RMSProp': Returns an `RMSPropOptimizer`.
    * 'SGD': Returns a `GradientDescentOptimizer`.

  Args:
    opt: An `Optimizer` instance, or string, as discussed above.
    learning_rate: A float. Only used if `opt` is a string.

  Returns:
    An `Optimizer` instance.

  Raises:
    ValueError: If `opt` is an unsupported string.
    ValueError: If `opt` is a supported string but `learning_rate` was not
      specified.
    ValueError: If `opt` is none of the above types.
  """
  if isinstance(opt, six.string_types):
    if opt in six.iterkeys(_OPTIMIZER_CLS_NAMES):
      if not learning_rate:
        raise ValueError('learning_rate must be specified when opt is string.')
      return _OPTIMIZER_CLS_NAMES[opt](learning_rate=learning_rate)
    raise ValueError(
        'Unsupported optimizer name: {}. Supported names are: {}'.format(
            opt, tuple(sorted(six.iterkeys(_OPTIMIZER_CLS_NAMES)))))
  if callable(opt):
    opt = opt()
  if not isinstance(opt, tf.compat.v1.train.Optimizer):
    raise ValueError(
        'The given object is not an Optimizer instance. Given: {}'.format(opt))
  return opt


def _optimizer_has_default_learning_rate(opt):
  signature = inspect.getargspec(opt.__init__)
  default_name_to_value = dict(zip(signature.args[::-1], signature.defaults))
  return 'learning_rate' in default_name_to_value


def get_optimizer_instance_v2(opt, learning_rate=None):
  """Returns an optimizer_v2.OptimizerV2 instance.

  Supports the following types for the given `opt`:
  * An `optimizer_v2.OptimizerV2` instance: Returns the given `opt`.
  * A string: Creates an `optimizer_v2.OptimizerV2` subclass with the given
  `learning_rate`.
    Supported strings:
    * 'Adagrad': Returns an tf.keras.optimizers.Adagrad.
    * 'Adam': Returns an tf.keras.optimizers.Adam.
    * 'Ftrl': Returns an tf.keras.optimizers.Ftrl.
    * 'RMSProp': Returns an tf.keras.optimizers.RMSProp.
    * 'SGD': Returns a tf.keras.optimizers.SGD.

  Args:
    opt: An `tf.keras.optimizers.Optimizer` instance, or string, as discussed
      above.
    learning_rate: A float. Only used if `opt` is a string. If None, and opt is
      string, it will use the default learning_rate of the optimizer.

  Returns:
    An `tf.keras.optimizers.Optimizer` instance.

  Raises:
    ValueError: If `opt` is an unsupported string.
    ValueError: If `opt` is a supported string but `learning_rate` was not
      specified.
    ValueError: If `opt` is none of the above types.
  """
  if isinstance(opt, six.string_types):
    if opt in six.iterkeys(_OPTIMIZER_CLS_NAMES_V2):
      if not learning_rate:
        if _optimizer_has_default_learning_rate(_OPTIMIZER_CLS_NAMES_V2[opt]):
          return _OPTIMIZER_CLS_NAMES_V2[opt]()
        else:
          return _OPTIMIZER_CLS_NAMES_V2[opt](learning_rate=_LEARNING_RATE)
      return _OPTIMIZER_CLS_NAMES_V2[opt](learning_rate=learning_rate)
    raise ValueError(
        'Unsupported optimizer name: {}. Supported names are: {}'.format(
            opt, tuple(sorted(six.iterkeys(_OPTIMIZER_CLS_NAMES_V2)))))
  if callable(opt):
    opt = opt()
  if not isinstance(opt, optimizer_v2.OptimizerV2):
    raise ValueError(
        'The given object is not a tf.keras.optimizers.Optimizer instance.'
        ' Given: {}'.format(opt))
  return opt
