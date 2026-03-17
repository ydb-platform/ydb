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
"""Configuration and utilities for receiving inputs at serving time.

Extends the export utils defined in core TensorFlow.

Please avoid importing this file directly, all of the public functions have
been exported to export_lib.py.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections

import six
import tensorflow as tf
from tensorflow.python.framework import ops
from tensorflow.python.saved_model.model_utils import export_utils
from tensorflow.python.saved_model.model_utils.export_utils import SINGLE_FEATURE_DEFAULT_NAME
from tensorflow.python.saved_model.model_utils.export_utils import SINGLE_LABEL_DEFAULT_NAME
from tensorflow.python.saved_model.model_utils.export_utils import SINGLE_RECEIVER_DEFAULT_NAME
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator import util

_SINGLE_TENSOR_DEFAULT_NAMES = {
    'feature': SINGLE_FEATURE_DEFAULT_NAME,
    'label': SINGLE_LABEL_DEFAULT_NAME,
    'receiver_tensor': SINGLE_RECEIVER_DEFAULT_NAME,
    'receiver_tensors_alternative': SINGLE_RECEIVER_DEFAULT_NAME
}


def wrap_and_check_input_tensors(tensors, field_name, allow_int_keys=False):
  """Ensure that tensors is a dict of str to Tensor mappings.

  Args:
    tensors: dict of `str` (or `int`s if `allow_int_keys=True`) to `Tensors`, or
      a single `Tensor`.
    field_name: name of the member field of `ServingInputReceiver` whose value
      is being passed to `tensors`.
    allow_int_keys: If set to true, the `tensor` dict keys may also be `int`s.

  Returns:
    dict of str to Tensors; this is the original dict if one was passed, or
    the original tensor wrapped in a dictionary.

  Raises:
    ValueError: if tensors is None, or has non-string keys,
      or non-Tensor values
  """
  if tensors is None:
    raise ValueError('{}s must be defined.'.format(field_name))
  if not isinstance(tensors, dict):
    tensors = {_SINGLE_TENSOR_DEFAULT_NAMES[field_name]: tensors}
  for name, tensor in tensors.items():
    _check_tensor_key(name, error_label=field_name, allow_ints=allow_int_keys)
    _check_tensor(tensor, name, error_label=field_name)
  return tensors


def _check_tensor(tensor, name, error_label='feature'):
  """Check that passed `tensor` is a Tensor or SparseTensor or RaggedTensor."""
  if not (isinstance(tensor, tf.Tensor) or
          isinstance(tensor, tf.sparse.SparseTensor) or
          isinstance(tensor, tf.RaggedTensor)):
    fmt_name = ' {}'.format(name) if name else ''
    value_error = ValueError('{}{} must be a Tensor, SparseTensor, or '
                             'RaggedTensor.'.format(error_label, fmt_name))
    # NOTE(ericmc): This if-else block is a specific carve-out for
    # LabeledTensor, which has a `.tensor` attribute and which is
    # convertible to tf.Tensor via ops.convert_to_tensor.
    # Allowing all types convertible to tf.Tensor is considered by soergel@
    # to be too permissive.
    # TODO(soergel): accept any type convertible to Tensor,
    # as in cl/193238295 snapshot #6.
    if hasattr(tensor, 'tensor'):
      try:
        ops.convert_to_tensor(tensor)
      except TypeError:
        raise value_error
    else:
      raise value_error


def _check_tensor_key(name, error_label='feature', allow_ints=False):
  if not isinstance(name, six.string_types):
    if not allow_ints:
      raise ValueError('{} keys must be strings: {}.'.format(error_label, name))
    elif not isinstance(name, six.integer_types):
      raise ValueError('{} keys must be strings or ints: {}.'.format(
          error_label, name))


@estimator_export('estimator.export.ServingInputReceiver')
class ServingInputReceiver(
    collections.namedtuple(
        'ServingInputReceiver',
        ['features', 'receiver_tensors', 'receiver_tensors_alternatives'])):
  """A return type for a serving_input_receiver_fn.

  Attributes:
    features: A `Tensor`, `SparseTensor`, or dict of string or int to `Tensor`
      or `SparseTensor`, specifying the features to be passed to the model.
      Note: if `features` passed is not a dict, it will be wrapped in a dict
        with a single entry, using 'feature' as the key.  Consequently, the
        model
      must accept a feature dict of the form {'feature': tensor}.  You may use
        `TensorServingInputReceiver` if you want the tensor to be passed as is.
    receiver_tensors: A `Tensor`, `SparseTensor`, or dict of string to `Tensor`
      or `SparseTensor`, specifying input nodes where this receiver expects to
      be fed by default.  Typically, this is a single placeholder expecting
      serialized `tf.Example` protos.
    receiver_tensors_alternatives: a dict of string to additional groups of
      receiver tensors, each of which may be a `Tensor`, `SparseTensor`, or dict
      of string to `Tensor` or`SparseTensor`. These named receiver tensor
      alternatives generate additional serving signatures, which may be used to
      feed inputs at different points within the input receiver subgraph.  A
      typical usage is to allow feeding raw feature `Tensor`s *downstream* of
      the tf.parse_example() op. Defaults to None.
  """

  def __new__(cls,
              features,
              receiver_tensors,
              receiver_tensors_alternatives=None):
    features = wrap_and_check_input_tensors(
        features, 'feature', allow_int_keys=True)

    receiver_tensors = wrap_and_check_input_tensors(receiver_tensors,
                                                    'receiver_tensor')

    if receiver_tensors_alternatives is not None:
      if not isinstance(receiver_tensors_alternatives, dict):
        raise ValueError(
            'receiver_tensors_alternatives must be a dict: {}.'.format(
                receiver_tensors_alternatives))
      for alternative_name, receiver_tensors_alt in (
          six.iteritems(receiver_tensors_alternatives)):
        # Updating dict during iteration is OK in this case.
        receiver_tensors_alternatives[alternative_name] = (
            wrap_and_check_input_tensors(receiver_tensors_alt,
                                         'receiver_tensors_alternative'))

    return super(ServingInputReceiver, cls).__new__(
        cls,
        features=features,
        receiver_tensors=receiver_tensors,
        receiver_tensors_alternatives=receiver_tensors_alternatives)


@estimator_export('estimator.export.TensorServingInputReceiver')
class TensorServingInputReceiver(
    collections.namedtuple(
        'TensorServingInputReceiver',
        ['features', 'receiver_tensors', 'receiver_tensors_alternatives'])):
  """A return type for a serving_input_receiver_fn.

  This is for use with models that expect a single `Tensor` or `SparseTensor`
  as an input feature, as opposed to a dict of features.

  The normal `ServingInputReceiver` always returns a feature dict, even if it
  contains only one entry, and so can be used only with models that accept such
  a dict.  For models that accept only a single raw feature, the
  `serving_input_receiver_fn` provided to `Estimator.export_saved_model()`
  should return this `TensorServingInputReceiver` instead.  See:
  https://github.com/tensorflow/tensorflow/issues/11674

  Note that the receiver_tensors and receiver_tensor_alternatives arguments
  will be automatically converted to the dict representation in either case,
  because the SavedModel format requires each input `Tensor` to have a name
  (provided by the dict key).

  Attributes:
    features: A single `Tensor` or `SparseTensor`, representing the feature to
      be passed to the model.
    receiver_tensors: A `Tensor`, `SparseTensor`, or dict of string to `Tensor`
      or `SparseTensor`, specifying input nodes where this receiver expects to
      be fed by default.  Typically, this is a single placeholder expecting
      serialized `tf.Example` protos.
    receiver_tensors_alternatives: a dict of string to additional groups of
      receiver tensors, each of which may be a `Tensor`, `SparseTensor`, or dict
      of string to `Tensor` or`SparseTensor`. These named receiver tensor
      alternatives generate additional serving signatures, which may be used to
      feed inputs at different points within the input receiver subgraph.  A
      typical usage is to allow feeding raw feature `Tensor`s *downstream* of
      the tf.parse_example() op. Defaults to None.
  """

  def __new__(cls,
              features,
              receiver_tensors,
              receiver_tensors_alternatives=None):
    if features is None:
      raise ValueError('features must be defined.')
    _check_tensor(features, None)

    receiver = ServingInputReceiver(
        features=features,
        receiver_tensors=receiver_tensors,
        receiver_tensors_alternatives=receiver_tensors_alternatives)

    return super(TensorServingInputReceiver, cls).__new__(
        cls,
        features=receiver.features[SINGLE_FEATURE_DEFAULT_NAME],
        receiver_tensors=receiver.receiver_tensors,
        receiver_tensors_alternatives=receiver.receiver_tensors_alternatives)


class UnsupervisedInputReceiver(ServingInputReceiver):
  """A return type for a training_input_receiver_fn or eval_input_receiver_fn.

  This differs from SupervisedInputReceiver in that it does not require a set
  of labels.

  Attributes:
    features: A `Tensor`, `SparseTensor`, or dict of string to `Tensor` or
      `SparseTensor`, specifying the features to be passed to the model.
    receiver_tensors: A `Tensor`, `SparseTensor`, or dict of string to `Tensor`
      or `SparseTensor`, specifying input nodes where this receiver expects to
      be fed by default.  Typically, this is a single placeholder expecting
      serialized `tf.Example` protos.
  """

  def __new__(cls, features, receiver_tensors):
    return super(UnsupervisedInputReceiver, cls).__new__(
        cls,
        features=features,
        receiver_tensors=receiver_tensors,
        receiver_tensors_alternatives=None)


class SupervisedInputReceiver(
    collections.namedtuple('SupervisedInputReceiver',
                           ['features', 'labels', 'receiver_tensors'])):
  """A return type for a training_input_receiver_fn or eval_input_receiver_fn.

  This differs from a ServingInputReceiver in that (1) this receiver expects
  a set of labels to be passed in with features, and (2) this receiver does
  not support receiver_tensors_alternatives, which are primarily used for
  serving.

  The expected return values are:
    features: A `Tensor`, `SparseTensor`, or dict of string or int to `Tensor`
      or `SparseTensor`, specifying the features to be passed to the model.
    labels: A `Tensor`, `SparseTensor`, or dict of string or int to `Tensor` or
      `SparseTensor`, specifying the labels to be passed to the model.
    receiver_tensors: A `Tensor`, `SparseTensor`, or dict of string to `Tensor`
      or `SparseTensor`, specifying input nodes where this receiver expects to
      be fed by default.  Typically, this is a single placeholder expecting
      serialized `tf.Example` protos.

  """

  def __new__(cls, features, labels, receiver_tensors):
    # Both features and labels can be dicts or raw tensors.
    # wrap_and_check_input_tensors is called here only to validate the tensors.
    # The wrapped dict that is returned is deliberately discarded.
    wrap_and_check_input_tensors(features, 'feature', allow_int_keys=True)
    wrap_and_check_input_tensors(labels, 'label', allow_int_keys=True)

    receiver_tensors = wrap_and_check_input_tensors(receiver_tensors,
                                                    'receiver_tensor')

    return super(SupervisedInputReceiver, cls).__new__(
        cls,
        features=features,
        labels=labels,
        receiver_tensors=receiver_tensors)


@estimator_export('estimator.export.build_parsing_serving_input_receiver_fn')
def build_parsing_serving_input_receiver_fn(feature_spec,
                                            default_batch_size=None):
  """Build a serving_input_receiver_fn expecting fed tf.Examples.

  Creates a serving_input_receiver_fn that expects a serialized tf.Example fed
  into a string placeholder.  The function parses the tf.Example according to
  the provided feature_spec, and returns all parsed Tensors as features.

  Args:
    feature_spec: a dict of string to `VarLenFeature`/`FixedLenFeature`.
    default_batch_size: the number of query examples expected per batch. Leave
      unset for variable batch size (recommended).

  Returns:
    A serving_input_receiver_fn suitable for use in serving.
  """

  def serving_input_receiver_fn():
    """An input_fn that expects a serialized tf.Example."""
    serialized_tf_example = tf.compat.v1.placeholder(
        dtype=tf.dtypes.string,
        shape=[default_batch_size],
        name='input_example_tensor')
    receiver_tensors = {'examples': serialized_tf_example}
    features = tf.compat.v1.io.parse_example(serialized_tf_example,
                                             feature_spec)
    return ServingInputReceiver(features, receiver_tensors)

  return serving_input_receiver_fn


def _placeholder_from_tensor(t, default_batch_size=None):
  """Creates a placeholder that matches the dtype and shape of passed tensor.

  Args:
    t: Tensor or EagerTensor
    default_batch_size: the number of query examples expected per batch. Leave
      unset for variable batch size (recommended).

  Returns:
    Placeholder that matches the passed tensor.
  """
  batch_shape = tf.TensorShape([default_batch_size])
  shape = batch_shape.concatenate(t.get_shape()[1:])

  # Reuse the feature tensor's op name (t.op.name) for the placeholder,
  # excluding the index from the tensor's name (t.name):
  # t.name = "%s:%d" % (t.op.name, t._value_index)
  try:
    name = t.op.name
  except AttributeError:
    # In Eager mode, tensors don't have ops or names, and while they do have
    # IDs, those are not maintained across runs. The name here is used
    # primarily for debugging, and is not critical to the placeholder.
    # So, in order to make this Eager-compatible, continue with an empty
    # name if none is available.
    name = None

  return tf.compat.v1.placeholder(dtype=t.dtype, shape=shape, name=name)


def _placeholders_from_receiver_tensors_dict(input_vals,
                                             default_batch_size=None):
  return {
      name: _placeholder_from_tensor(t, default_batch_size)
      for name, t in input_vals.items()
  }


@estimator_export('estimator.export.build_raw_serving_input_receiver_fn')
def build_raw_serving_input_receiver_fn(features, default_batch_size=None):
  """Build a serving_input_receiver_fn expecting feature Tensors.

  Creates an serving_input_receiver_fn that expects all features to be fed
  directly.

  Args:
    features: a dict of string to `Tensor`.
    default_batch_size: the number of query examples expected per batch. Leave
      unset for variable batch size (recommended).

  Returns:
    A serving_input_receiver_fn.
  """

  def serving_input_receiver_fn():
    """A serving_input_receiver_fn that expects features to be fed directly."""
    receiver_tensors = _placeholders_from_receiver_tensors_dict(
        features, default_batch_size)
    return ServingInputReceiver(receiver_tensors, receiver_tensors)

  return serving_input_receiver_fn


@estimator_export(
    'estimator.experimental.build_raw_supervised_input_receiver_fn')
def build_raw_supervised_input_receiver_fn(features,
                                           labels,
                                           default_batch_size=None):
  """Build a supervised_input_receiver_fn for raw features and labels.

  This function wraps tensor placeholders in a supervised_receiver_fn
  with the expectation that the features and labels appear precisely as
  the model_fn expects them. Features and labels can therefore be dicts of
  tensors, or raw tensors.

  Args:
    features: a dict of string to `Tensor` or `Tensor`.
    labels: a dict of string to `Tensor` or `Tensor`.
    default_batch_size: the number of query examples expected per batch. Leave
      unset for variable batch size (recommended).

  Returns:
    A supervised_input_receiver_fn.

  Raises:
    ValueError: if features and labels have overlapping keys.
  """
  # Check for overlapping keys before beginning.
  try:
    feat_keys = features.keys()
  except AttributeError:
    feat_keys = [SINGLE_RECEIVER_DEFAULT_NAME]
  try:
    label_keys = labels.keys()
  except AttributeError:
    label_keys = [SINGLE_LABEL_DEFAULT_NAME]

  overlap_keys = set(feat_keys) & set(label_keys)
  if overlap_keys:
    raise ValueError('Features and labels must have distinct keys. '
                     'Found overlapping keys: {}'.format(overlap_keys))

  def supervised_input_receiver_fn():
    """A receiver_fn that expects pass-through features and labels."""
    if not isinstance(features, dict):
      features_cp = _placeholder_from_tensor(features, default_batch_size)
      receiver_features = {SINGLE_RECEIVER_DEFAULT_NAME: features_cp}
    else:
      receiver_features = _placeholders_from_receiver_tensors_dict(
          features, default_batch_size)
      features_cp = receiver_features

    if not isinstance(labels, dict):
      labels_cp = _placeholder_from_tensor(labels, default_batch_size)
      receiver_labels = {SINGLE_LABEL_DEFAULT_NAME: labels_cp}
    else:
      receiver_labels = _placeholders_from_receiver_tensors_dict(
          labels, default_batch_size)
      labels_cp = receiver_labels

    receiver_tensors = dict(receiver_features)
    receiver_tensors.update(receiver_labels)
    return SupervisedInputReceiver(features_cp, labels_cp, receiver_tensors)

  return supervised_input_receiver_fn


def build_supervised_input_receiver_fn_from_input_fn(input_fn, **input_fn_args):
  """Get a function that returns a SupervisedInputReceiver matching an input_fn.

  Note that this function calls the input_fn in a local graph in order to
  extract features and labels. Placeholders are then created from those
  features and labels in the default graph.

  Args:
    input_fn: An Estimator input_fn, which is a function that returns one of:
      * A 'tf.data.Dataset' object: Outputs of `Dataset` object must be a tuple
        (features, labels) with same constraints as below.
      * A tuple (features, labels): Where `features` is a `Tensor` or a
        dictionary of string feature name to `Tensor` and `labels` is a `Tensor`
        or a dictionary of string label name to `Tensor`. Both `features` and
        `labels` are consumed by `model_fn`. They should satisfy the expectation
        of `model_fn` from inputs.
    **input_fn_args: set of kwargs to be passed to the input_fn. Note that these
      will not be checked or validated here, and any errors raised by the
      input_fn will be thrown to the top.

  Returns:
    A function taking no arguments that, when called, returns a
    SupervisedInputReceiver. This function can be passed in as part of the
    input_receiver_map when exporting SavedModels from Estimator with multiple
    modes.
  """
  # Wrap the input_fn call in a graph to prevent sullying the default namespace
  with tf.Graph().as_default():
    result = input_fn(**input_fn_args)
    features, labels, _ = util.parse_input_fn_result(result)
  # Placeholders are created back in the default graph.
  return build_raw_supervised_input_receiver_fn(features, labels)


### Below utilities are specific to SavedModel exports.
# TODO(kathywu): Rename all references to use the original definition in
# model_utils, or estimator/export/export_lib.py if other estimator export
# functions are used.
build_all_signature_defs = export_utils.build_all_signature_defs
get_temp_export_dir = export_utils.get_temp_export_dir
get_timestamped_export_dir = export_utils.get_timestamped_export_dir
