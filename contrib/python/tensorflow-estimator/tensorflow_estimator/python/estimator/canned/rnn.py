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
"""Recurrent Neural Network model and estimators."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import tensorflow as tf

from tensorflow.python.feature_column import feature_column_lib as fc
from tensorflow.python.framework import ops
from tensorflow.python.keras import activations
from tensorflow.python.keras import layers as keras_layers
from tensorflow.python.keras import models
from tensorflow.python.keras.layers import recurrent_v2
from tensorflow.python.keras.utils import losses_utils
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator import estimator
from tensorflow_estimator.python.estimator import model_fn
from tensorflow_estimator.python.estimator.canned import optimizers
from tensorflow_estimator.python.estimator.head import binary_class_head as binary_head_lib
from tensorflow_estimator.python.estimator.head import multi_class_head as multi_head_lib
from tensorflow_estimator.python.estimator.head import sequential_head as seq_head_lib

# The defaults are historical artifacts of the initial implementation, but seem
# reasonable choices.
# TODO(aarg): Also apply default learning rate and clipping to Keras model so
# they apply when the optimizer is set via `compile` and the model trained via
# the `fit` method.
_DEFAULT_LEARNING_RATE = 0.05
_DEFAULT_CLIP_NORM = 5.0

_SIMPLE_RNN_KEY = 'simple_rnn'
_LSTM_KEY = 'lstm'
_GRU_KEY = 'gru'

_CELL_TYPE_TO_LAYER_MAPPING = {
    _LSTM_KEY: recurrent_v2.LSTM,
    _GRU_KEY: recurrent_v2.GRU,
    _SIMPLE_RNN_KEY: keras_layers.SimpleRNN
}

_CELL_TYPES = {
    _LSTM_KEY: recurrent_v2.LSTMCell,
    _GRU_KEY: recurrent_v2.GRUCell,
    _SIMPLE_RNN_KEY: keras_layers.SimpleRNNCell
}

# Indicates no value was provided by the user to a kwarg.
USE_DEFAULT = object()


def _single_rnn_cell(units, cell_type):
  """Initializes a RNN cell."""
  cell_type = _CELL_TYPES.get(cell_type, cell_type)
  if not callable(cell_type):
    raise ValueError(
        '`cell_type` should be a class producing a RNN cell, or a string '
        'specifying the cell type. Supported strings are: {}.'.format(
            [_SIMPLE_RNN_KEY, _LSTM_KEY, _GRU_KEY]))
  cell = cell_type(units=units)
  if hasattr(cell, '_enable_caching_device'):
    # Enable the caching_device to speed up the repeative varaible read in
    # tf.while. This should work only with tf.session.
    cell._enable_caching_device = True  # pylint: disable=protected-access
  if not hasattr(cell, 'call') or not hasattr(cell, 'state_size'):
    raise ValueError('RNN cell should have a `call` and `state_size` method.')
  return cell


def _make_rnn_cell_fn(units, cell_type=_SIMPLE_RNN_KEY):
  """Convenience function to create `rnn_cell_fn` for canned RNN Estimators.

  Args:
    units: Iterable of integer number of hidden units per RNN layer.
    cell_type: A class producing a RNN cell or a string specifying the cell
      type. Supported strings are: `'simple_rnn'`, `'lstm'`, and `'gru'`.

  Returns:
    A function that returns a RNN cell.

  Raises:
    ValueError: If cell_type is not supported.
  """

  def rnn_cell_fn():
    cells = [_single_rnn_cell(n, cell_type) for n in units]
    if len(cells) == 1:
      return cells[0]
    return cells

  return rnn_cell_fn


class RNNModel(models.Model):
  """A Keras RNN model.

  Composition of layers to compute logits from RNN model, along with training
  and inference features. See `tf.keras.models.Model` for more details on Keras
  models.

  Example of usage:

  ```python
  rating = tf.feature_column.embedding_column(
      tf.feature_column.sequence_categorical_column_with_identity('rating', 5),
      10)
  rnn_layer = tf.keras.layers.SimpleRNN(20)
  rnn_model = RNNModel(rnn_layer, units=1, sequence_feature_columns=[rating])

  rnn_model.compile(
      tf.keras.optimizers.Adam(), loss=tf.keras.losses.MeanSquaredError())
  rnn_model.fit(generator(), epochs=10, steps_per_epoch=100)
  rnn_model.predict({'rating': np.array([[0, 1], [2, 3]])}, steps=1)
  ```
  """

  # TODO(aarg): Update arguments to support multiple rnn layers.
  def __init__(self,
               rnn_layer,
               units,
               sequence_feature_columns,
               context_feature_columns=None,
               activation=None,
               return_sequences=False,
               **kwargs):
    """Initializes a RNNModel instance.

    Args:
      rnn_layer: A Keras RNN layer.
      units: An int indicating the dimension of the logit layer, and of the
        model output.
      sequence_feature_columns: An iterable containing the `FeatureColumn`s that
        represent sequential input. All items in the set should either be
        sequence columns (e.g. `sequence_numeric_column`) or constructed from
        one (e.g. `embedding_column` with `sequence_categorical_column_*` as
        input).
      context_feature_columns: An iterable containing the `FeatureColumn`s for
        contextual input. The data represented by these columns will be
        replicated and given to the RNN at each timestep. These columns must be
        instances of classes derived from `DenseColumn` such as
        `numeric_column`, not the sequential variants.
      activation: Activation function to apply to the logit layer (for instance
        `tf.keras.activations.sigmoid`). If you don't specify anything, no
        activation is applied.
      return_sequences: A boolean indicating whether to return the last output
        in the output sequence, or the full sequence.
      **kwargs: Additional arguments.

    Raises:
      ValueError: If `units` is not an int.
    """
    super(RNNModel, self).__init__(**kwargs)
    if not isinstance(units, int):
      raise ValueError('units must be an int.  Given type: {}'.format(
          type(units)))
    self._return_sequences = return_sequences
    self._sequence_feature_columns = sequence_feature_columns
    self._context_feature_columns = context_feature_columns
    self._sequence_features_layer = fc.SequenceFeatures(
        sequence_feature_columns)
    self._dense_features_layer = None
    if context_feature_columns:
      self._dense_features_layer = tf.compat.v1.keras.layers.DenseFeatures(
          context_feature_columns)
    self._rnn_layer = rnn_layer
    self._logits_layer = keras_layers.Dense(
        units=units, activation=activation, name='logits')

  def call(self, inputs, training=None):
    """Computes the RNN output.

    By default no activation is applied and the logits are returned. To output
    probabilites an activation needs to be specified such as sigmoid or softmax.

    Args:
      inputs: A dict mapping keys to input tensors.
      training: Python boolean indicating whether the layers should behave in
        training mode or in inference mode. This argument is passed to the
        model's layers. This is for instance used with cells that use dropout.

    Returns:
      A `Tensor` with logits from RNN model. It has shape
      (batch_size, time_step, logits_size) if `return_sequence` is `True`,
      (batch_size, logits_size) otherwise.
    """
    if not isinstance(inputs, dict):
      raise ValueError('inputs should be a dictionary of `Tensor`s. '
                       'Given type: {}'.format(type(inputs)))
    with ops.name_scope('sequence_input_layer'):
      try:
        sequence_input, sequence_length = self._sequence_features_layer(
            inputs, training=training)
      except TypeError:
        sequence_input, sequence_length = self._sequence_features_layer(inputs)
      tf.compat.v1.summary.histogram('sequence_length', sequence_length)

      if self._context_feature_columns:
        try:
          context_input = self._dense_features_layer(inputs, training=training)
        except TypeError:
          context_input = self._dense_features_layer(inputs)
        sequence_input = fc.concatenate_context_input(
            context_input, sequence_input=sequence_input)

    sequence_length_mask = tf.sequence_mask(sequence_length)
    rnn_outputs = self._rnn_layer(
        sequence_input, mask=sequence_length_mask, training=training)

    logits = self._logits_layer(rnn_outputs)
    if self._return_sequences:
      # Passes sequence mask as `_keras_mask` to be used in Keras model for
      # loss and metrics aggregation to exclude padding in the sequential case.
      logits._keras_mask = sequence_length_mask  # pylint: disable=protected-access
    return logits

  def get_config(self):
    """Returns a dictionary with the config of the model."""
    config = {'name': self.name}
    config['rnn_layer'] = {
        'class_name': self._rnn_layer.__class__.__name__,
        'config': self._rnn_layer.get_config()
    }
    config['units'] = self._logits_layer.units
    config['return_sequences'] = self._return_sequences
    config['activation'] = activations.serialize(self._logits_layer.activation)
    config['sequence_feature_columns'] = fc.serialize_feature_columns(
        self._sequence_feature_columns)
    config['context_feature_columns'] = (
        fc.serialize_feature_columns(self._context_feature_columns)
        if self._context_feature_columns else None)
    return config

  @classmethod
  def from_config(cls, config, custom_objects=None):
    """Creates a RNNModel from its config.

    Args:
      config: A Python dictionary, typically the output of `get_config`.
      custom_objects: Optional dictionary mapping names (strings) to custom
        classes or functions to be considered during deserialization.

    Returns:
      A RNNModel.
    """
    rnn_layer = keras_layers.deserialize(
        config.pop('rnn_layer'), custom_objects=custom_objects)
    sequence_feature_columns = fc.deserialize_feature_columns(
        config.pop('sequence_feature_columns'), custom_objects=custom_objects)
    context_feature_columns = config.pop('context_feature_columns', None)
    if context_feature_columns:
      context_feature_columns = fc.deserialize_feature_columns(
          context_feature_columns, custom_objects=custom_objects)
    activation = activations.deserialize(
        config.pop('activation', None), custom_objects=custom_objects)
    return cls(
        rnn_layer=rnn_layer,
        sequence_feature_columns=sequence_feature_columns,
        context_feature_columns=context_feature_columns,
        activation=activation,
        **config)


def _get_rnn_estimator_spec(features, labels, mode, head, rnn_model, optimizer,
                            return_sequences):
  """Computes `EstimatorSpec` from logits to use in estimator model function.

  Args:
    features: dict of `Tensor` and `SparseTensor` objects returned from
      `input_fn`.
    labels: `Tensor` of shape [batch_size, 1] or [batch_size] with labels.
    mode: Defines whether this is training, evaluation or prediction. See
      `ModeKeys`.
    head: A `Head` instance.
    rnn_model: A Keras model that computes RNN logits from features.
    optimizer: String, `tf.keras.optimizers.Optimizer` object, or callable that
      creates the optimizer to use for training. If not specified, will use the
      Adagrad optimizer with a default learning rate of 0.05 and gradient clip
      norm of 5.0.
    return_sequences: A boolean indicating whether to return the last output in
      the output sequence, or the full sequence.

  Returns:
    An `EstimatorSpec` instance.

  Raises:
    ValueError: If mode or optimizer is invalid, or features has the wrong type.
  """
  training = (mode == model_fn.ModeKeys.TRAIN)
  # In TRAIN mode, create optimizer and assign global_step variable to
  # optimizer.iterations to make global_step increased correctly, as Hooks
  # relies on global step as step counter - otherwise skip optimizer
  # initialization and set it to None.
  if training:
    # If user does not provide an optimizer instance, use the optimizer
    # specified by the string with default learning rate and gradient clipping.
    if isinstance(optimizer, six.string_types):
      optimizer = optimizers.get_optimizer_instance_v2(
          optimizer, learning_rate=_DEFAULT_LEARNING_RATE)
      optimizer.clipnorm = _DEFAULT_CLIP_NORM
    else:
      optimizer = optimizers.get_optimizer_instance_v2(optimizer)
    optimizer.iterations = tf.compat.v1.train.get_or_create_global_step()
  else:
    optimizer = None

  logits = rnn_model(features, training)

  if return_sequences and head.input_sequence_mask_key not in features:
    features[head.input_sequence_mask_key] = logits._keras_mask  # pylint: disable=protected-access

  return head.create_estimator_spec(
      features=features,
      mode=mode,
      labels=labels,
      optimizer=optimizer,
      logits=logits,
      update_ops=rnn_model.updates,
      trainable_variables=rnn_model.trainable_variables)


def _verify_rnn_cell_input(rnn_cell_fn, units, cell_type):
  if rnn_cell_fn and (units or cell_type != USE_DEFAULT):
    raise ValueError(
        'units and cell_type must not be specified when using rnn_cell_fn')


def _make_rnn_layer(rnn_cell_fn, units, cell_type, return_sequences):
  """Assert arguments are valid and return rnn_layer_fn.

  Args:
    rnn_cell_fn: A function that returns a RNN cell instance that will be used
      to construct the RNN.
    units: Iterable of integer number of hidden units per RNN layer.
    cell_type: A class producing a RNN cell or a string specifying the cell
      type.
    return_sequences: A boolean indicating whether to return the last output
      in the output sequence, or the full sequence.:

  Returns:
    A tf.keras.layers.RNN layer.
  """
  _verify_rnn_cell_input(rnn_cell_fn, units, cell_type)
  if cell_type in _CELL_TYPE_TO_LAYER_MAPPING and isinstance(units, int):
    return _CELL_TYPE_TO_LAYER_MAPPING[cell_type](
        units=units, return_sequences=return_sequences)
  if not rnn_cell_fn:
    if cell_type == USE_DEFAULT:
      cell_type = _SIMPLE_RNN_KEY
    rnn_cell_fn = _make_rnn_cell_fn(units, cell_type)

  return keras_layers.RNN(cell=rnn_cell_fn(), return_sequences=return_sequences)


@estimator_export('estimator.experimental.RNNEstimator', v1=[])
class RNNEstimator(estimator.Estimator):
  """An Estimator for TensorFlow RNN models with user-specified head.

  Example:

  ```python
  token_sequence = sequence_categorical_column_with_hash_bucket(...)
  token_emb = embedding_column(categorical_column=token_sequence, ...)

  estimator = RNNEstimator(
      head=tf.estimator.RegressionHead(),
      sequence_feature_columns=[token_emb],
      units=[32, 16], cell_type='lstm')

  # Or with custom RNN cell:
  def rnn_cell_fn(_):
    cells = [ tf.keras.layers.LSTMCell(size) for size in [32, 16] ]
    return tf.keras.layers.StackedRNNCells(cells)

  estimator = RNNEstimator(
      head=tf.estimator.RegressionHead(),
      sequence_feature_columns=[token_emb],
      rnn_cell_fn=rnn_cell_fn)

  # Input builders
  def input_fn_train: # returns x, y
    pass
  estimator.train(input_fn=input_fn_train, steps=100)

  def input_fn_eval: # returns x, y
    pass
  metrics = estimator.evaluate(input_fn=input_fn_eval, steps=10)
  def input_fn_predict: # returns x, None
    pass
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
  otherwise there will be a `KeyError`:

  * if the head's `weight_column` is not `None`, a feature with
    `key=weight_column` whose value is a `Tensor`.
  * for each `column` in `sequence_feature_columns`:
    - a feature with `key=column.name` whose `value` is a `SparseTensor`.
  * for each `column` in `context_feature_columns`:
    - if `column` is a `CategoricalColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `WeightedCategoricalColumn`, two features: the first
      with `key` the id column name, the second with `key` the weight column
      name. Both features' `value` must be a `SparseTensor`.
    - if `column` is a `DenseColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss and predicted output are determined by the specified head.

  @compatibility(eager)
  Estimators are not compatible with eager execution.
  @end_compatibility
  """

  def __init__(self,
               head,
               sequence_feature_columns,
               context_feature_columns=None,
               units=None,
               cell_type=USE_DEFAULT,
               rnn_cell_fn=None,
               return_sequences=False,
               model_dir=None,
               optimizer='Adagrad',
               config=None):
    """Initializes a `RNNEstimator` instance.

    Args:
      head: A `Head` instance. This specifies the model's output and loss
        function to be optimized.
      sequence_feature_columns: An iterable containing the `FeatureColumn`s that
        represent sequential input. All items in the set should either be
        sequence columns (e.g. `sequence_numeric_column`) or constructed from
        one (e.g. `embedding_column` with `sequence_categorical_column_*` as
        input).
      context_feature_columns: An iterable containing the `FeatureColumn`s for
        contextual input. The data represented by these columns will be
        replicated and given to the RNN at each timestep. These columns must be
        instances of classes derived from `DenseColumn` such as
        `numeric_column`, not the sequential variants.
      units: Iterable of integer number of hidden units per RNN layer. If set,
        `cell_type` must also be specified and `rnn_cell_fn` must be `None`.
      cell_type: A class producing a RNN cell or a string specifying the cell
        type. Supported strings are: `'simple_rnn'`, `'lstm'`, and `'gru'`. If
          set, `units` must also be specified and `rnn_cell_fn` must be `None`.
      rnn_cell_fn: A function that returns a RNN cell instance that will be used
        to construct the RNN. If set, `units` and `cell_type` cannot be set.
        This is for advanced users who need additional customization beyond
        `units` and `cell_type`. Note that `tf.keras.layers.StackedRNNCells` is
        needed for stacked RNNs.
      return_sequences: A boolean indicating whether to return the last output
        in the output sequence, or the full sequence.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      optimizer: An instance of `tf.Optimizer` or string specifying optimizer
        type. Defaults to Adagrad optimizer.
      config: `RunConfig` object to configure the runtime settings.

    Note that a RNN cell has:
      - a `call` method.
      - a `state_size` attribute.
      - a `output_size` attribute.
      - a `get_initial_state` method.

    See the documentation on `tf.keras.layers.RNN` for more details.

    Raises:
      ValueError: If `units`, `cell_type`, and `rnn_cell_fn` are not
        compatible.
    """

    # TODO(aarg): Instead of raising an error convert head to sequential head.
    if return_sequences and not isinstance(head, seq_head_lib._SequentialHead):  # pylint: disable=protected-access
      raise ValueError('Provided head must be a `_SequentialHead` object when '
                       '`return_sequences` is set to True.')
    _verify_rnn_cell_input(rnn_cell_fn, units, cell_type)

    def _model_fn(features, labels, mode, config):
      """RNNEstimator model function."""
      del config  # Unused.
      rnn_layer = _make_rnn_layer(
          rnn_cell_fn=rnn_cell_fn,
          units=units,
          cell_type=cell_type,
          return_sequences=return_sequences)
      rnn_model = RNNModel(
          rnn_layer=rnn_layer,
          units=head.logits_dimension,
          sequence_feature_columns=sequence_feature_columns,
          context_feature_columns=context_feature_columns,
          return_sequences=return_sequences,
          name='rnn_model')
      return _get_rnn_estimator_spec(
          features,
          labels,
          mode,
          head=head,
          rnn_model=rnn_model,
          optimizer=optimizer,
          return_sequences=return_sequences)

    super(RNNEstimator, self).__init__(
        model_fn=_model_fn, model_dir=model_dir, config=config)


@estimator_export('estimator.experimental.RNNClassifier', v1=[])
class RNNClassifier(RNNEstimator):
  """A classifier for TensorFlow RNN models.

  Trains a recurrent neural network model to classify instances into one of
  multiple classes.

  Example:

  ```python
  token_sequence = sequence_categorical_column_with_hash_bucket(...)
  token_emb = embedding_column(categorical_column=token_sequence, ...)

  estimator = RNNClassifier(
      sequence_feature_columns=[token_emb],
      units=[32, 16], cell_type='lstm')

  # Input builders
  def input_fn_train: # returns x, y
    pass
  estimator.train(input_fn=input_fn_train, steps=100)

  def input_fn_eval: # returns x, y
    pass
  metrics = estimator.evaluate(input_fn=input_fn_eval, steps=10)
  def input_fn_predict: # returns x, None
    pass
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
  otherwise there will be a `KeyError`:

  * if `weight_column` is not `None`, a feature with
    `key=weight_column` whose value is a `Tensor`.
  * for each `column` in `sequence_feature_columns`:
    - a feature with `key=column.name` whose `value` is a `SparseTensor`.
  * for each `column` in `context_feature_columns`:
    - if `column` is a `CategoricalColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `WeightedCategoricalColumn`, two features: the first
      with `key` the id column name, the second with `key` the weight column
      name. Both features' `value` must be a `SparseTensor`.
    - if `column` is a `DenseColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss is calculated by using softmax cross entropy.

  @compatibility(eager)
  Estimators are not compatible with eager execution.
  @end_compatibility
  """

  def __init__(self,
               sequence_feature_columns,
               context_feature_columns=None,
               units=None,
               cell_type=USE_DEFAULT,
               rnn_cell_fn=None,
               return_sequences=False,
               model_dir=None,
               n_classes=2,
               weight_column=None,
               label_vocabulary=None,
               optimizer='Adagrad',
               loss_reduction=losses_utils.ReductionV2.SUM_OVER_BATCH_SIZE,
               sequence_mask='sequence_mask',
               config=None):
    """Initializes a `RNNClassifier` instance.

    Args:
      sequence_feature_columns: An iterable containing the `FeatureColumn`s that
        represent sequential input. All items in the set should either be
        sequence columns (e.g. `sequence_numeric_column`) or constructed from
        one (e.g. `embedding_column` with `sequence_categorical_column_*` as
        input).
      context_feature_columns: An iterable containing the `FeatureColumn`s for
        contextual input. The data represented by these columns will be
        replicated and given to the RNN at each timestep. These columns must be
        instances of classes derived from `DenseColumn` such as
        `numeric_column`, not the sequential variants.
      units: Iterable of integer number of hidden units per RNN layer. If set,
        `cell_type` must also be specified and `rnn_cell_fn` must be `None`.
      cell_type: A class producing a RNN cell or a string specifying the cell
        type. Supported strings are: `'simple_rnn'`, `'lstm'`, and `'gru'`. If
          set, `units` must also be specified and `rnn_cell_fn` must be `None`.
      rnn_cell_fn: A function that returns a RNN cell instance that will be used
        to construct the RNN. If set, `units` and `cell_type` cannot be set.
        This is for advanced users who need additional customization beyond
        `units` and `cell_type`. Note that `tf.keras.layers.StackedRNNCells` is
        needed for stacked RNNs.
      return_sequences: A boolean indicating whether to return the last output
        in the output sequence, or the full sequence. Note that if True,
        `weight_column` must be None or a string.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      n_classes: Number of label classes. Defaults to 2, namely binary
        classification. Must be > 1.
      weight_column: A string or a `NumericColumn` created by
        `tf.feature_column.numeric_column` defining feature column representing
        weights. It is used to down weight or boost examples during training. It
        will be multiplied by the loss of the example. If it is a string, it is
        used as a key to fetch weight tensor from the `features`. If it is a
        `NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
        weight_column.normalizer_fn is applied on it to get weight tensor.
      label_vocabulary: A list of strings represents possible label values. If
        given, labels must be string type and have any value in
        `label_vocabulary`. If it is not given, that means labels are already
        encoded as integer or float within [0, 1] for `n_classes=2` and encoded
        as integer values in {0, 1,..., n_classes-1} for `n_classes`>2 . Also
        there will be errors if vocabulary is not provided and labels are
        string.
      optimizer: An instance of `tf.Optimizer` or string specifying optimizer
        type. Defaults to Adagrad optimizer.
      loss_reduction: One of `tf.losses.Reduction` except `NONE`. Describes how
        to reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`.
      sequence_mask: A string with the name of the sequence mask tensor. If
        `sequence_mask` is in the features dictionary, the provided tensor is
        used, otherwise the sequence mask is computed from the length of
        sequential features. The sequence mask is used in evaluation and
        training mode to aggregate loss and metrics computation while excluding
        padding steps. It is also added to the predictions dictionary in
        prediction mode to indicate which steps are padding.
      config: `RunConfig` object to configure the runtime settings.

    Note that a RNN cell has:
      - a `call` method.
      - a `state_size` attribute.
      - a `output_size` attribute.
      - a `get_initial_state` method.

    See the documentation on `tf.keras.layers.RNN` for more details.

    Raises:
      ValueError: If `units`, `cell_type`, and `rnn_cell_fn` are not
        compatible.
    """
    if n_classes == 2:
      head = binary_head_lib.BinaryClassHead(
          weight_column=weight_column,
          label_vocabulary=label_vocabulary,
          loss_reduction=loss_reduction)
    else:
      head = multi_head_lib.MultiClassHead(
          n_classes=n_classes,
          weight_column=weight_column,
          label_vocabulary=label_vocabulary,
          loss_reduction=loss_reduction)

    if return_sequences:
      tf.compat.v1.logging.info(
          'Converting head to sequential head with '
          '`SequentialHeadWrapper` to allow sequential predictions.')
      head = seq_head_lib.SequentialHeadWrapper(
          head,
          sequence_length_mask=sequence_mask,
          feature_columns=weight_column)

    super(RNNClassifier, self).__init__(
        head=head,
        sequence_feature_columns=sequence_feature_columns,
        context_feature_columns=context_feature_columns,
        units=units,
        cell_type=cell_type,
        rnn_cell_fn=rnn_cell_fn,
        return_sequences=return_sequences,
        model_dir=model_dir,
        optimizer=optimizer,
        config=config)
