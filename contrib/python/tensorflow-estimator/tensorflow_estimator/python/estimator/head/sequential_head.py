# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
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
"""Defines a head for sequential models."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc

import six
import tensorflow as tf

if six.PY3:
  from collections.abc import Iterable
else:
  from collections import Iterable

from tensorflow.python.framework import ops
from tensorflow_estimator.python.estimator.head import base_head
from tensorflow_estimator.python.estimator.head import multi_head
from tensorflow_estimator.python.estimator.mode_keys import ModeKeys


class _SequentialHead(base_head.Head):
  """Interface for the head of a sequential model.

  A sequential head handles input sequences of different lengths to compute the
  output of a model. It requires a sequence mask tensor, to indicate which steps
  of the sequences are padded and ensure proper aggregation for loss and metrics
  computation. It has a `input_sequence_mask_key` property that specifies which
  tensor of the feature dictionary to use as the sequence mask tensor.

  Such a head can for instance be used with `RNNEstimator` for sequential
  predictions.

  Example of usage:
    ```python
    def _my_model_fn(features, labels, mode, params, config=None):
      feature_layer = tf.feature_column.SequenceFeatureLayer(columns)
      input_layer, sequence_length = feature_layer(features)
      sequence_length_mask = tf.sequence_mask(sequence_length)
      rnn_layer = tf.keras.layers.RNN(cell=tf.keras.layers.SimpleRNNCell(units),
                                      return_sequences=True)
      logits = rnn_layer(input_layer, mask=sequence_length_mask)
      features[sequential_head.input_sequence_mask_key] = sequence_length_mask
      return sequential_head.create_estimator_spec(
          features=features,
          labels=labels,
          mode=mode,
          logits=logits,
          optimizer=optimizer)
    ```
  """
  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
  def input_sequence_mask_key(self):
    """Key of the sequence mask tensor in the feature dictionary.

    Returns:
      A string.
    """
    raise NotImplementedError('Calling an abstract method.')


class SequentialHeadWrapper(_SequentialHead):
  """Sequential head wrapping a Head object.

  Wraps a `Head` object and applies a sequential mask to:
    - Loss aggregation: To only account for masked steps. Used for
      `create_estimator_spec` and `loss` methods.
    - Metrics: The sequence mask is used to only account for mask steps in
      metrics computation with the `update_metrics` method.
    - Predictions: To add a sequence length mask tensor to the predictions
      dictionary.
  """

  def __init__(self,
               static_head,
               sequence_length_mask='sequence_length_mask',
               feature_columns=None):
    """Initializes a `SequentialHeadWrapper` instance.

    Example of usage:
      ```python
      # Define a sequential head.
      static_head = tf.estimator.BinaryClassHead(weight_column='weights')
      sequential_head = head_lib.SequentialHeadWrapper(
          static_head=static_head, sequence_length_mask='mask',
          feature_columns='weights')

      # Define feature columns and parsing spec.
      feature_columns = [
        tf.feature_column.sequence_numeric_column('sequential-feature')
      ]
      label_column = tf.feature_column.sequence_numeric_column(
          'label', dtype=tf.int32),
      weight_column = tf.feature_column.sequence_numeric_column('weights')
      parsing_spec = tf.feature_column.make_parse_example_spec(
          feature_columns + [label_column, weight_column])

      # Use the head in a model function.
      def _my_model_fn(features, labels, mode, params, config=None):
        feature_layer = tf.feature_column.SequenceFeatureLayer(feature_columns)
        input_layer, sequence_length = feature_layer(features)
        sequence_length_mask = tf.sequence_mask(sequence_length)
        rnn_layer = tf.keras.layers.RNN(
            cell=tf.keras.layers.SimpleRNNCell(units),
            return_sequences=True)
        logits = rnn_layer(input_layer, mask=sequence_length_mask)
        features['mask'] = sequence_length_mask
        return sequential_head.create_estimator_spec(
            features=features,
            labels=labels,
            mode=mode,
            logits=logits,
            optimizer=optimizer)
      ```

    Args:
      static_head: `Head` object, static head to wrap.
      sequence_length_mask: `str`, name of sequence length mask tensor in
        features dictionary. Tensor must be a dense tensor of shape [batch_size,
        seq_length].
      feature_columns: `str` or list of the former. Specifies the features of
        the features dictionary to which the sequence length mask must be
        applied, and which are passed to the static head's methods when calling
        `create_estimator_spec`, `loss` or `update_metrics`. This is typically a
        weight tensor.

    Raises:
      TypeError: If `sequence_length_mask` is not of string type.
      TypeError: If provided features columns are not of string type.
    """
    # Verify and set sequence mask column.
    # TODO(aarg): Add support for `NumericColumn`.
    if not isinstance(sequence_length_mask, six.string_types):
      raise TypeError('`sequence_mask` column must be a string. '
                      'Given type: {}.'.format(type(sequence_length_mask)))
    self._sequence_length_mask = sequence_length_mask

    # Verify and set feature columns (to be flattened).
    feature_columns = feature_columns or []
    if not isinstance(feature_columns, Iterable):
      raise TypeError('`feature_columns` must be either a string or an '
                      'iterable of strings got {} instead.'.format(
                          type(feature_columns)))
    if isinstance(feature_columns, six.string_types):
      self._feature_columns = [feature_columns]
    else:
      self._feature_columns = feature_columns

    for column in self._feature_columns:
      # TODO(aarg): Add support for `NumericColumn` and `SequenceNumericColumn`.
      if not isinstance(column, six.string_types):
        raise TypeError('Column must a string. Given type: {}.'.format(
            type(column)))

    # Set other variables.
    if isinstance(static_head, multi_head.MultiHead):
      # TODO(aarg): Add support for MultiHead.
      raise ValueError(
          '`MultiHead` is not supported with `SequentialHeadWrapper`.')
    self._static_head = static_head

    super(SequentialHeadWrapper, self).__init__()

  def _flatten(self, labels, logits, features):
    """Flattens labels, logits, and features tensors.

    Provided tensors need to have at least two dimensions. The two first
    dimensions of the provided tensors are flattened to one single dimension.
    If a tensor is dense, the sequence mask in the features dictionary is used
    to flatten it.

    Note: If indices of a sparse tensor are not sorted, they will be reordered.

    Args:
      labels: `Tensor` or `SparseTensor` to flatten.
      logits: `Tensor` or `SparseTensor` to flatten.
      features: Dictionary of `Tensor` or `SparseTensor` objects to flatten.

    Returns:
      - Dense `Tensor` with flattened labels.
      - Dense `Tensor` with flattened logits.
      - Dictionary of flattened dense `Tensor` objects.

    Raises:
      ValueError: If the sequence mask is not found in `features`.
      ValueError: If one of the provided tensors to flatten has not at least two
        dimensions.
    """
    # Retrieve sequence_mask from features dictionary.
    if self.input_sequence_mask_key not in features:
      raise ValueError('The provided sequence_length_mask key `{}` should be '
                       'included in the features dictionary, but was not '
                       'found. Found keys: {}.'.format(
                           self.input_sequence_mask_key, list(features.keys())))
    sequence_mask = features[self.input_sequence_mask_key]
    if sequence_mask.get_shape().ndims != 2:
      raise ValueError('Mask is expected to have two dimensions, got '
                       '{} instead.'.format(sequence_mask.get_shape().ndims))

    with ops.name_scope('flatten'):
      expected_length = tf.math.reduce_sum(
          tf.cast(sequence_mask, tf.dtypes.int32))
      # Flatten logits and labels.
      flat_logits = _flatten_tensor(logits, sequence_mask, expected_length)
      flat_labels = _flatten_tensor(labels, sequence_mask, expected_length)

      # Flatten features.
      flat_features = {}
      for column in self._feature_columns:
        if column not in features:
          raise ValueError('`{}` column expected in features '
                           'dictionary.'.format(column))
        flat_features[column] = _flatten_tensor(features[column], sequence_mask,
                                                expected_length)

      return flat_labels, flat_logits, flat_features

  def loss(self,
           logits,
           labels,
           features=None,
           mode=None,
           regularization_losses=None):
    """Flattens input and returns regularized training loss.

    Flattens `logits`, `labels`, and `features` tensors that are specified by
    the head's `feature_columns` before calling the static head's `loss` method.

    Args:
      logits: Logits `Tensor` of rank >= 2 and shape [batch_size, seq_length,
        D2, ... DN].
      labels: Labels `Tensor` or `SparseTensor` or rank >= 2 and shape
        [batch_size, seq_length, D2, ... DN].
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Must contain the sequence length mask tensor. Features
        corresponding to the sequential's head `feature_columns` are flattened
        and passed to the static head's `loss` method.
      mode: Estimator's `ModeKeys`. To be used in case loss calculation is
        different in Train and Eval mode.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
      A scalar `Tensor` representing regularized training loss used in train and
      eval.
    """
    flat_labels, flat_logits, flat_features = self._flatten(
        labels, logits, features)
    return self._static_head.loss(
        logits=flat_logits,
        labels=flat_labels,
        features=flat_features,
        mode=mode,
        regularization_losses=regularization_losses)

  def create_estimator_spec(self,
                            features,
                            mode,
                            logits,
                            labels=None,
                            optimizer=None,
                            trainable_variables=None,
                            train_op_fn=None,
                            update_ops=None,
                            regularization_losses=None):
    """Returns `EstimatorSpec` that a model_fn can return.

    If in TRAIN or EVAL mode, `logits`, `labels`, and `features` tensors
    corresponding to the head's `feature_columns` are flattened before calling
    the static head's `create_estimator_spec` method.
    If in PREDICT mode, no flattening is done. The `EstimatatorSpec` is computed
    using the static head's `create_estimator_spec` method. The sequence length
    mask tensor is added to the predictions dictionary.

    Args:
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. If in TRAIN or EVAL mode, only specified features are
        flattened and passed to the static head's method.
      mode: Estimator's `ModeKeys`.
      logits: Logits `Tensor` of rank >= 2 and shape [batch_size, seq_length,
        D2, ... DN].
      labels: Labels `Tensor` or `SparseTensor` or rank >= 2 and shape
        [batch_size, seq_length, D2, ... DN].
      optimizer: An `tf.keras.optimizers.Optimizer` instance to optimize the
        loss in TRAIN mode. Namely, sets
        `train_op = optimizer.get_updates(loss, trainable_variables)`, which
        updates variables to minimize `loss`.
      trainable_variables: A list or tuple of `Variable` objects to update to
        minimize `loss`. In Tensorflow 1.x, by default these are the list of
        variables collected in the graph under the key
        `GraphKeys.TRAINABLE_VARIABLES`. As Tensorflow 2.x doesn't have
        collections and GraphKeys, trainable_variables need to be passed
        explicitly here.
      train_op_fn: Function that takes a scalar loss `Tensor` and returns an op
        to optimize the model with the loss in TRAIN mode. Used if `optimizer`
        is `None`. Exactly one of `train_op_fn` and `optimizer` must be set in
        TRAIN mode. By default, it is `None` in other modes. If you want to
        optimize loss yourself, you can pass `lambda _: tf.no_op()` and then use
          `EstimatorSpec.loss` to compute and apply gradients.
      update_ops: A list or tuple of update ops to be run at training time. For
        example, layers such as BatchNormalization create mean and variance
        update ops that need to be run at training time. In Tensorflow 1.x,
        these are thrown into an UPDATE_OPS collection. As Tensorflow 2.x
        doesn't have collections, update_ops need to be passed explicitly here.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
      `EstimatorSpec`.
    """
    if mode == ModeKeys.PREDICT:
      spec = self._static_head.create_estimator_spec(
          features=features, mode=mode, logits=logits)
      spec.predictions[self.input_sequence_mask_key] = features[
          self.input_sequence_mask_key]
      return spec._replace(predictions=spec.predictions)

    flat_labels, flat_logits, flat_features = self._flatten(
        labels, logits, features)

    return self._static_head.create_estimator_spec(
        features=flat_features,
        mode=mode,
        logits=flat_logits,
        trainable_variables=trainable_variables,
        labels=flat_labels,
        optimizer=optimizer,
        train_op_fn=train_op_fn,
        regularization_losses=regularization_losses,
        update_ops=update_ops)

  def update_metrics(self,
                     eval_metrics,
                     features,
                     logits,
                     labels,
                     regularization_losses=None):
    """Updates metric objects and returns a `dict` of the updated metrics.

    Flattens `logits`, `labels`, and `features` tensors that are specified by
    the head's feature_columns` before calling the static head's
    `update_metrics` method.

    Args:
      eval_metrics: A `dict` of metrics to be updated.
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Only specified features are flattened and passed to the
        static head's method.
      logits: Logits `Tensor` of rank >= 2 and shape [batch_size, seq_length,
        D2, ... DN].
      labels: Labels `Tensor` or `SparseTensor` or rank >= 2 and shape
        [batch_size, seq_length, D2, ... DN].
      regularization_losses: A list of additional scalar losses to be added to
        the training and evaluation loss, such as regularization losses.

    Returns:
       A `dict` of updated metrics keyed by name. The value is an instance of
       `Metric` class.
    """
    flat_labels, flat_logits, flat_features = self._flatten(
        labels, logits, features)
    return self._static_head.update_metrics(
        eval_metrics=eval_metrics,
        features=flat_features,
        logits=flat_logits,
        labels=flat_labels,
        regularization_losses=regularization_losses)

  def _create_tpu_estimator_spec(self,
                                 features,
                                 mode,
                                 logits,
                                 labels=None,
                                 optimizer=None,
                                 trainable_variables=None,
                                 train_op_fn=None,
                                 update_ops=None,
                                 regularization_losses=None):
    raise NotImplementedError

  def predictions(self, logits, keys=None):
    """Calls the static head's `predictions` method."""
    return self._static_head.predictions(logits, keys=keys)

  def metrics(self, regularization_losses=None):
    """Calls the static head's `metrics` method."""
    return self._static_head.metrics(regularization_losses)

  @property
  def input_sequence_mask_key(self):
    """Returns the key for the sequence mask feature."""
    return self._sequence_length_mask

  @property
  def logits_dimension(self):
    """Returns the logits dimension of the static head."""
    return self._static_head.logits_dimension

  @property
  def loss_reduction(self):
    """Returns the loss reduction of the static head."""
    return self._static_head.loss_reduction

  @property
  def name(self):
    """Returns the name of the static head."""
    if self._static_head.name:
      return '{}_sequential'.format(self._static_head.name)
    return None

  @property
  def static_head(self):
    """Returns the wrapped static head."""
    return self._static_head


def _flatten_tensor(tensor, sequence_mask, expected_length):
  """Flattens the two first dimensions and reshapes a tensor or sparse tensor.

  If `tensor` is a dense tensor, the sequence_mask is used to infer valid
  inputs.

  Note: If `tensor` is a `SparseTensor` and the indices are not sorted, they
  will be reordered.

  Args:
    tensor: A `Tensor` or `SparseTensor` of dimension at least 2, of shape
      [batch_size, seq_length, D0, D1, ..., DN].
    sequence_mask: A boolean `Tensor` of shape [batch_size, seq_length].
    expected_length: A integer scalar `Tensor` with the expected length of the
      resulting flattenned Tensor.

  Returns:
    A `Tensor` object of shape [expected_length, D0, D1, ..., DN].

  Raises:
    ValueError: If `tensor` has not at least 2 dimensions.
    ValueError: If `tensor` is not a `Tensor` or `SparseTensor` object.
    InvalidArgumentError: If the resulting `Tensor` doesn't have the expected
      length.
  """
  shape = tensor.get_shape()
  if shape.ndims < 2:
    raise ValueError('Input tensor expected to have at least 2 dimensions, '
                     'got {} instead.'.format(shape.ndims))
  if isinstance(tensor, tf.sparse.SparseTensor):
    # What follows depends on the indices ordering. Hence we reorder the indices
    # to ensure correctness.
    flat_tensor = tf.sparse.reorder(tensor).values
    if shape.ndims > 2:
      new_shape = tf.concat([[-1], shape[2:]], axis=0)
      flat_tensor = tf.reshape(tensor.values, new_shape)
  elif isinstance(tensor, tf.Tensor):
    flat_tensor = tf.boolean_mask(tensor, sequence_mask)
  else:
    raise ValueError('`tensor` expected to be a `Tensor` or  `SparseTensor` '
                     'got `{}` instead.'.format(tensor))
  if shape.ndims == 2:
    flat_tensor = tf.compat.v1.expand_dims(flat_tensor, -1)
    expected_shape = tf.concat([[expected_length], [1]], axis=0)
  else:
    expected_shape = tf.concat([[expected_length], shape[2:]], axis=0)

  # TODO(b/119617064): Unify eager and graph implementations.
  err_message = 'Tensor shape is incompatible with provided mask.'
  if tf.executing_eagerly():
    if flat_tensor._shape_tuple() != tuple(expected_shape.numpy()):  # pylint: disable=protected-access
      raise ValueError(err_message)
    return flat_tensor
  with tf.control_dependencies([
      tf.compat.v1.debugging.assert_equal(
          tf.compat.v1.shape(flat_tensor), expected_shape, message=err_message)
  ]):
    return tf.identity(flat_tensor)
