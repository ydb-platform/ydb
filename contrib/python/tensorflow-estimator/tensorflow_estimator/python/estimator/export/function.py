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
# ==============================================================================
"""Defines class for wrapping an Estimator model function."""
# TODO(kathywu): support remaining outputs from the EstimatorSpec.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import tensorflow as tf
from tensorflow.python.eager import def_function
from tensorflow.python.eager import function
from tensorflow.python.eager import wrap_function
from tensorflow.python.framework import func_graph
from tensorflow.python.saved_model.model_utils import export_utils
from tensorflow.python.training.tracking import tracking
from tensorflow.python.util import function_utils
from tensorflow_estimator.python.estimator import model_fn as model_fn_lib
from tensorflow_estimator.python.estimator.mode_keys import ModeKeys


class ModelFunction(tracking.AutoTrackable):
  """A checkpointable ModelFunction object.

  This object stores a global mapping of variables and functions for each mode.
  """

  def __init__(self, config=None, params=None):
    self._config = config
    self._params = params
    self._functions = {}

    self._variable_holder = wrap_function.VariableHolder(share_variables=True)

    # Add reference to the variable holder's mapping of variables, which is a
    # trackable object.
    self._variables_by_name = self._variable_holder.variables

  @staticmethod
  def from_function(model_fn, all_modes=None, config=None, params=None):
    """Creates a new ModelFunction object from a model function."""
    if all_modes is None:
      all_modes = [ModeKeys.TRAIN, ModeKeys.EVAL, ModeKeys.PREDICT]
    else:
      all_modes = list(all_modes)

    obj = ModelFunction(config=config, params=params)
    for mode in all_modes:
      obj.add_mode(model_fn, mode)
    return obj

  @property
  def variables(self):
    return self._variables_by_name

  def add_mode(self, fn, mode, input_signature=None):
    if mode in self._functions:
      raise ValueError('ModelFunction object has multiple functions with name'
                       ' {}.'.format(mode))

    spec_fn = EstimatorSpecFunction(
        fn,
        mode,
        config=self._config,
        params=self._params,
        variable_holder=self._variable_holder,
        input_signature=input_signature)

    self._functions[mode] = spec_fn

  def train(self, features, labels):
    return self.call(ModeKeys.TRAIN, features, labels)

  def evaluate(self, features, labels):
    return self.call(ModeKeys.EVAL, features, labels)

  def predict(self, features):
    return self.call(ModeKeys.PREDICT, features)

  def call(self, mode, features, labels=None):
    if mode not in self._functions:
      raise ValueError(
          'Mode {} is not defined the ModelFunction. To add modes,'
          ' use the `add_mode()` function. Available modes: {}'.format(
              mode, self._functions.keys()))
    fn = self._functions[mode]
    if fn.expects_labels:
      return fn(features, labels)
    else:
      return fn(features)


def _wrap_and_verify_model_fn(model_fn,
                              mode=None,
                              config=None,
                              params=None,
                              input_signature=None):
  """Returns a function that only has only tensor arguments (features, labels).

  Args:
    model_fn: Model function. Must follow the signature defined in
      `tf.estimator.Estimator`.
    mode: Optional string `tf.estimstor.ModeKey`.
    config: Optional `estimator.RunConfig` object.
    params: Optional `dict` of hyperparameters.
    input_signature: Possibly nested TensorSpec of the tensor arguments.

  Returns:
    tuple of (
      function that only accepts tensor arguments (features and/or labels),
      whether the returned function expects a labels argument)
  """
  model_fn_lib.verify_model_fn_args(model_fn, params)
  args = function_utils.fn_args(model_fn)
  kwargs = {}
  if 'mode' in args:
    kwargs['mode'] = mode
  if 'params' in args:
    kwargs['params'] = params
  if 'config' in args:
    kwargs['config'] = config

  if 'labels' in args:
    if input_signature is None or len(input_signature) == 2:

      def wrapped_model_fn(features, labels=None):
        return model_fn(features=features, labels=labels, **kwargs)
    else:

      def wrapped_model_fn(features):
        return model_fn(features=features, labels=None, **kwargs)
  else:

    def wrapped_model_fn(features):
      return model_fn(features=features, **kwargs)

  return wrapped_model_fn, 'labels' in args


class EstimatorSpecFunction(def_function.Function):
  """Wraps graph functions defined for a function returning an EstimatorSpec.

  Instances of this class are revivable when attached to a checkpointable
  object.
  """

  def __init__(self,
               fn,
               mode,
               config=None,
               params=None,
               variable_holder=None,
               **kwargs):
    """Initializes an EstimatorSpecFunction.

    Args:
      fn: Python model function.
      mode: String mode to run the function.
      config: RunConfig that is passed to the `config` arg in the function.
      params: object that is passed to the `params` argument in the function.
      variable_holder: Optional `wrap_function.VariableHolder` object.
      **kwargs: Optional keyword arguments to pass to tf.function (e.g.
        input_signature).
    """
    python_function, self.expects_labels = _wrap_and_verify_model_fn(
        fn,
        mode=mode,
        config=config,
        params=params,
        input_signature=kwargs.get('input_signature', None))
    super(EstimatorSpecFunction, self).__init__(python_function, mode, **kwargs)
    self._variable_holder = variable_holder

  def _defun(self, fn):
    return _EstimatorSpecFunction(
        fn,
        name=self._name,
        variable_holder=self._variable_holder,
        input_signature=self.input_signature,
        autograph=self._autograph,
        autograph_options=self._experimental_autograph_options)


class _EstimatorSpecFunction(function.Function):
  """Wraps graph functions defined for a function returning an EstimatorSpec.

  This object handles creation of the graph functions.
  """

  def __init__(self, python_function, name, variable_holder=None, **kwargs):
    super(_EstimatorSpecFunction, self).__init__(python_function, name,
                                                 **kwargs)
    self._variable_holder = variable_holder

  def _create_graph_function(self, args, kwargs, **other_kwargs):
    _ = other_kwargs
    wrapped_graph = _EstimatorWrappedGraph(self._variable_holder)
    return wrapped_graph.wrap_model_fn(
        self._python_function,
        self._name,
        signature=self.input_signature,
        args=args,
        kwargs=kwargs)


class _EstimatorWrappedGraph(wrap_function.WrappedGraph):
  """WrappedGraph that handles global step creation and wraps estimator fns."""

  def __init__(self, *args, **kwargs):
    super(_EstimatorWrappedGraph, self).__init__(*args, **kwargs)
    # Create global step variable, which may be used by the input and model fns.
    self._global_step_read_fn = self.wrap_function(
        self._global_step, signature=[])

    self._concrete_model_fn = None

    # Original EstimatorSpec object returned by the model function. Only tensors
    # and ops are returned by the concrete model function.
    self._estimator_spec = None

  def _global_step(self):
    return tf.compat.v1.train.get_or_create_global_step()

  @property
  def global_step(self):
    return self._global_step_read_fn()

  @property
  def model_fn(self):
    return self._concrete_model_fn

  @property
  def estimator_spec(self):
    if self._concrete_model_fn is None:
      raise ValueError('Please wrap a model function first.')
    return self._estimator_spec

  def wrap_model_fn(self,
                    model_fn,
                    mode,
                    args=None,
                    kwargs=None,
                    signature=None):
    """Wraps a model function, and stores the returned estimator spec."""
    if self._concrete_model_fn is not None:
      raise ValueError('`wrap_model_fn` should be only called once per graph.')

    def fn(*args, **kwargs):
      """Returns tensor and op outputs from the returned spec."""
      ret = model_fn(*args, **kwargs)

      if isinstance(ret, model_fn_lib.EstimatorSpec):
        self._estimator_spec = ret
        return _filter_estimator_spec_outputs(ret)
      return ret

    name = 'model_fn_{}'.format(mode)
    self._concrete_model_fn = self._wrap_function(fn, args, kwargs, signature,
                                                  name)
    return self._concrete_model_fn

  def wrap_input_receiver_fn(self, input_receiver_fn):
    """Converts an input receiver function to one or more concrete functions.

    Input receiver functions are python functions with no arguments.
    Placeholders are created within the function and used to receive inputs to
    the model.

    The function (or multiple functions) generated depends on the InputReceiver
    object returned by `input_receiver_fn`.

    Generally, the returned function will have inputs and outputs:
      input_receiver(**receiver_tensors) --> features

    or (if the InputReceiver returns labels):
      input_receiver(**receiver_tensors) --> features, labels

    __Alternate Receiver Tensors__

    The InputReceiver may have alternate receiver tensors, in which case
    additional concrete functions are generated. Example:
      InputReceiver.receiver_tensors_alternatives = {
        'alt_input_1': Tensor,
        'alt_input_2': {
          'tensor_1': Tensor,
          'tensor_2': Tensor
        }
      }

    This will generate concrete functions:
      input_receiver_alt_input_1(input) --> features
      input_receiver_alt_input_2(tensor_1, tensor_2) --> features

    Args:
      input_receiver_fn: a no-argument function that returns an `InputReceiver`
        object.

    Returns:
      A list of tuples of (concrete function, receiver name). The name of the
      default input receiver is `None`.
    """
    ret = [None]

    def fn():
      ret[0] = input_receiver = input_receiver_fn()
      features = input_receiver.features
      labels = getattr(input_receiver, 'labels', None)

      if labels is None:
        return features
      return features, labels

    func_graph.func_graph_from_py_func(
        None,  # Name is unused.
        self._variable_holder.call_with_variable_creator_scope(fn),
        args=None,
        kwargs=None,
        signature=[],
        add_control_dependencies=False,
        func_graph=self.graph)

    functions = []
    input_receiver = ret[0]

    wrapped_input_receiver_fn = _prune_receiver_tensors(
        self._wrapped_function,
        receiver_tensors=input_receiver.receiver_tensors,
        outputs=self.graph.structured_outputs,
        name=_input_receiver_fn_name(None))
    functions.append((wrapped_input_receiver_fn, None))

    receiver_tensors_alternatives = getattr(input_receiver,
                                            'receiver_tensors_alternatives',
                                            None)

    if receiver_tensors_alternatives:
      for receiver_name, receiver_tensors_alt in (
          six.iteritems(receiver_tensors_alternatives)):
        receiver_tensors_alt = _canonicalize_receiver_tensors(
            receiver_tensors_alt)
        wrapped_input_receiver_fn = _prune_receiver_tensors(
            self._wrapped_function,
            receiver_tensors=receiver_tensors_alt,
            outputs=self.graph.structured_outputs,
            name=_input_receiver_fn_name(receiver_name))
        functions.append((wrapped_input_receiver_fn, receiver_name))
    return functions


def _filter_estimator_spec_outputs(spec):
  """Filters tensors and ops from an EstimatorSpec and returns a dictionary."""
  # TODO(kathywu): Add loss, export outputs, eval metrics depending on the mode.
  if spec.mode == ModeKeys.TRAIN:
    return dict(predictions=spec.predictions, train_op=spec.train_op)
  return dict(predictions=spec.predictions)


_RECEIVER_FN_NAME = '_input_receiver'


def _canonicalize_receiver_tensors(receiver_tensors):
  """Converts receiver tensors to the expected format of `as_signature_def`."""
  # TODO(b/129646028): Wrap function doesn't support composite tensors.
  for tensor in tf.nest.flatten(receiver_tensors):
    if not isinstance(tensor, tf.Tensor):
      raise ValueError('All receiver tensors must be tensors (composite '
                       'tensors are not yet supported).')

  if isinstance(receiver_tensors, dict):
    return receiver_tensors
  return {export_utils.SINGLE_RECEIVER_DEFAULT_NAME: receiver_tensors}


def _input_receiver_fn_name(name):
  if name is None:
    return _RECEIVER_FN_NAME
  else:
    return '{}_{}'.format(_RECEIVER_FN_NAME, name)


def _prune_receiver_tensors(wrapped_function, receiver_tensors, outputs, name):
  inputs = _canonicalize_receiver_tensors(receiver_tensors)
  return wrapped_function.prune(
      inputs,
      outputs,
      name=name,
      input_signature=(None, func_graph.convert_structure_to_signature(inputs)))
