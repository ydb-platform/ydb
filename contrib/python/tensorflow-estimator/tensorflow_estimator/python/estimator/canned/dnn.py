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
"""Deep Neural Network estimators."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import tensorflow as tf
from tensorflow.python.feature_column import feature_column
from tensorflow.python.feature_column import feature_column_lib
from tensorflow.python.framework import ops
from tensorflow.python.keras.engine import training
from tensorflow.python.keras.layers import core as keras_core
from tensorflow.python.keras.layers import normalization as keras_norm
from tensorflow.python.keras.utils import losses_utils
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator import estimator
from tensorflow_estimator.python.estimator.canned import head as head_lib
from tensorflow_estimator.python.estimator.canned import optimizers
from tensorflow_estimator.python.estimator.head import head_utils
from tensorflow_estimator.python.estimator.head import regression_head
from tensorflow_estimator.python.estimator.mode_keys import ModeKeys

# The default learning rate of 0.05 is a historical artifact of the initial
# implementation, but seems a reasonable choice.
_LEARNING_RATE = 0.05


def _add_hidden_layer_summary(value, tag):
  tf.compat.v1.summary.scalar('%s/fraction_of_zero_values' % tag,
                              tf.math.zero_fraction(value))
  tf.compat.v1.summary.histogram('%s/activation' % tag, value)


@estimator_export(v1=['estimator.experimental.dnn_logit_fn_builder'])
def dnn_logit_fn_builder(units, hidden_units, feature_columns, activation_fn,
                         dropout, input_layer_partitioner, batch_norm):
  """Function builder for a dnn logit_fn.

  Args:
    units: An int indicating the dimension of the logit layer.  In the MultiHead
      case, this should be the sum of all component Heads' logit dimensions.
    hidden_units: Iterable of integer number of hidden units per layer.
    feature_columns: Iterable of `feature_column._FeatureColumn` model inputs.
    activation_fn: Activation function applied to each layer.
    dropout: When not `None`, the probability we will drop out a given
      coordinate.
    input_layer_partitioner: Partitioner for input layer.
    batch_norm: Whether to use batch normalization after each hidden layer.

  Returns:
    A logit_fn (see below).

  Raises:
    ValueError: If units is not an int.
  """
  if not isinstance(units, six.integer_types):
    raise ValueError('units must be an int.  Given type: {}'.format(
        type(units)))

  def dnn_logit_fn(features, mode):
    """Deep Neural Network logit_fn.

    Args:
      features: This is the first item returned from the `input_fn` passed to
        `train`, `evaluate`, and `predict`. This should be a single `Tensor` or
        `dict` of same.
      mode: Optional. Specifies if this training, evaluation or prediction. See
        `ModeKeys`.

    Returns:
      A `Tensor` representing the logits, or a list of `Tensor`'s representing
      multiple logits in the MultiHead case.
    """
    dnn_model = _DNNModel(
        units,
        hidden_units,
        feature_columns,
        activation_fn,
        dropout,
        input_layer_partitioner,
        batch_norm,
        name='dnn')
    return dnn_model(features, mode)

  return dnn_logit_fn


def dnn_logit_fn_builder_v2(units, hidden_units, feature_columns, activation_fn,
                            dropout, batch_norm):
  """Function builder for a dnn logit_fn.

  Args:
    units: An int indicating the dimension of the logit layer.  In the MultiHead
      case, this should be the sum of all component Heads' logit dimensions.
    hidden_units: Iterable of integer number of hidden units per layer.
    feature_columns: Iterable of `feature_column._FeatureColumn` model inputs.
    activation_fn: Activation function applied to each layer.
    dropout: When not `None`, the probability we will drop out a given
      coordinate.
    batch_norm: Whether to use batch normalization after each hidden layer.

  Returns:
    A logit_fn (see below).

  Raises:
    ValueError: If units is not an int.
  """
  if not isinstance(units, six.integer_types):
    raise ValueError('units must be an int.  Given type: {}'.format(
        type(units)))

  def dnn_logit_fn(features, mode):
    """Deep Neural Network logit_fn.

    Args:
      features: This is the first item returned from the `input_fn` passed to
        `train`, `evaluate`, and `predict`. This should be a single `Tensor` or
        `dict` of same.
      mode: Optional. Specifies if this training, evaluation or prediction. See
        `ModeKeys`.

    Returns:
      A `Tensor` representing the logits, or a list of `Tensor`'s representing
      multiple logits in the MultiHead case.
    """
    dnn_model = _DNNModelV2(
        units,
        hidden_units,
        feature_columns,
        activation_fn,
        dropout,
        batch_norm,
        name='dnn')
    return dnn_model(features, mode)

  return dnn_logit_fn


def _get_previous_name_scope():
  current_name_scope = ops.get_name_scope()
  return current_name_scope.rsplit('/', 1)[0] + '/'


class _DNNModel(training.Model):
  """A DNN Model."""

  def __init__(self,
               units,
               hidden_units,
               feature_columns,
               activation_fn,
               dropout,
               input_layer_partitioner,
               batch_norm,
               name=None,
               **kwargs):
    super(_DNNModel, self).__init__(name=name, **kwargs)
    if feature_column_lib.is_feature_column_v2(feature_columns):
      self._input_layer = tf.compat.v1.keras.layers.DenseFeatures(
          feature_columns=feature_columns, name='input_layer')
    else:
      self._input_layer = feature_column.InputLayer(
          feature_columns=feature_columns,
          name='input_layer',
          create_scope_now=False)

    self._add_layer(self._input_layer, 'input_layer')

    self._dropout = dropout
    self._batch_norm = batch_norm

    self._hidden_layers = []
    self._dropout_layers = []
    self._batch_norm_layers = []
    self._hidden_layer_scope_names = []
    for layer_id, num_hidden_units in enumerate(hidden_units):
      with tf.compat.v1.variable_scope('hiddenlayer_%d' %
                                       layer_id) as hidden_layer_scope:
        hidden_layer = tf.compat.v1.layers.Dense(
            units=num_hidden_units,
            activation=activation_fn,
            kernel_initializer=tf.compat.v1.glorot_uniform_initializer(),
            name=hidden_layer_scope,
            _scope=hidden_layer_scope)
        self._add_layer(hidden_layer, hidden_layer_scope.name)
        self._hidden_layer_scope_names.append(hidden_layer_scope.name)
        self._hidden_layers.append(hidden_layer)
        if self._dropout is not None:
          dropout_layer = tf.compat.v1.layers.Dropout(rate=self._dropout)
          self._add_layer(dropout_layer, dropout_layer.name)
          self._dropout_layers.append(dropout_layer)
        if self._batch_norm:
          batch_norm_layer = tf.compat.v1.layers.BatchNormalization(
              # The default momentum 0.99 actually crashes on certain
              # problem, so here we use 0.999, which is the default of
              # tf.contrib.layers.batch_norm.
              momentum=0.999,
              trainable=True,
              name='batchnorm_%d' % layer_id,
              _scope='batchnorm_%d' % layer_id)
          self._add_layer(batch_norm_layer, batch_norm_layer.name)
          self._batch_norm_layers.append(batch_norm_layer)

    with tf.compat.v1.variable_scope('logits') as logits_scope:
      self._logits_layer = tf.compat.v1.layers.Dense(
          units=units,
          activation=None,
          kernel_initializer=tf.compat.v1.glorot_uniform_initializer(),
          name=logits_scope,
          _scope=logits_scope)
      self._add_layer(self._logits_layer, logits_scope.name)
      self._logits_scope_name = logits_scope.name
    self._input_layer_partitioner = input_layer_partitioner

  def call(self, features, mode):
    is_training = mode == ModeKeys.TRAIN
    # The Keras training.Model adds a name_scope with the name of the model
    # which modifies the constructed graph. Hence we add another name_scope
    # here which is the one before the training.Model one was applied.
    # TODO(rohanj): Remove this in TF 2.0 (b/116728605)
    with ops.name_scope(name=_get_previous_name_scope()):
      # TODO(rohanj): Remove dependence on variable scope for partitioning.
      with tf.compat.v1.variable_scope(
          'input_from_feature_columns',
          partitioner=self._input_layer_partitioner):
        try:
          net = self._input_layer(features, training=is_training)
        except TypeError:
          net = self._input_layer(features)
      for i in range(len(self._hidden_layers)):
        net = self._hidden_layers[i](net)
        if self._dropout is not None and is_training:
          net = self._dropout_layers[i](net, training=True)
        if self._batch_norm:
          net = self._batch_norm_layers[i](net, training=is_training)
        _add_hidden_layer_summary(net, self._hidden_layer_scope_names[i])

      logits = self._logits_layer(net)
      _add_hidden_layer_summary(logits, self._logits_scope_name)
      return logits

  def _add_layer(self, layer, layer_name):
    # "Magic" required for keras.Model classes to track all the variables in
    # a list of layers.Layer objects.
    # TODO(ashankar): Figure out API so user code doesn't have to do this.
    setattr(self, layer_name, layer)


def _name_from_scope_name(name):
  """Returns the name of an op given the name of its scope.

  Args:
    name: the name of the scope.

  Returns:
    the name of the op (equal to scope name minus any trailing slash).
  """
  return name[:-1] if (name and name[-1] == '/') else name


class _DNNModelV2(training.Model):
  """A DNN Model."""

  def __init__(self,
               units,
               hidden_units,
               feature_columns,
               activation_fn,
               dropout,
               batch_norm,
               name=None,
               **kwargs):
    super(_DNNModelV2, self).__init__(name=name, **kwargs)
    with ops.name_scope(
        'input_from_feature_columns') as input_feature_column_scope:
      layer_name = input_feature_column_scope + 'input_layer'
      if feature_column_lib.is_feature_column_v2(feature_columns):
        self._input_layer = tf.compat.v2.keras.layers.DenseFeatures(
            feature_columns=feature_columns, name=layer_name)
      else:
        raise ValueError(
            'Received a feature column from TensorFlow v1, but this is a '
            'TensorFlow v2 Estimator. Please either use v2 feature columns '
            '(accessible via tf.feature_column.* in TF 2.x) with this '
            'Estimator, or switch to a v1 Estimator for use with v1 feature '
            'columns (accessible via tf.compat.v1.estimator.* and '
            'tf.compat.v1.feature_column.*, respectively.')

    self._dropout = dropout
    self._batch_norm = batch_norm

    self._hidden_layers = []
    self._dropout_layers = []
    self._batch_norm_layers = []
    self._hidden_layer_scope_names = []
    for layer_id, num_hidden_units in enumerate(hidden_units):
      with ops.name_scope('hiddenlayer_%d' % layer_id) as hidden_layer_scope:
        # Get scope name without the trailing slash.
        hidden_shared_name = _name_from_scope_name(hidden_layer_scope)
        hidden_layer = keras_core.Dense(
            units=num_hidden_units,
            activation=activation_fn,
            kernel_initializer=tf.compat.v1.glorot_uniform_initializer(),
            name=hidden_shared_name)
        self._hidden_layer_scope_names.append(hidden_shared_name)
        self._hidden_layers.append(hidden_layer)
        if self._dropout is not None:
          dropout_layer = keras_core.Dropout(rate=self._dropout)
          self._dropout_layers.append(dropout_layer)
        if self._batch_norm:
          batch_norm_name = hidden_shared_name + '/batchnorm_%d' % layer_id
          # TODO(scottzhu): Change back to use BatchNormalization when the
          # cleanup is done.
          batch_norm_layer = keras_norm.BatchNormalizationBase(
              # The default momentum 0.99 actually crashes on certain
              # problem, so here we use 0.999, which is the default of
              # tf.contrib.layers.batch_norm.
              momentum=0.999,
              trainable=True,
              name=batch_norm_name)
          self._batch_norm_layers.append(batch_norm_layer)

    with ops.name_scope('logits') as logits_scope:
      logits_shared_name = _name_from_scope_name(logits_scope)
      self._logits_layer = keras_core.Dense(
          units=units,
          activation=None,
          kernel_initializer=tf.compat.v1.glorot_uniform_initializer(),
          name=logits_shared_name)
      self._logits_scope_name = logits_shared_name

  def call(self, features, mode):
    is_training = mode == ModeKeys.TRAIN
    try:
      net = self._input_layer(features, training=is_training)
    except TypeError:
      net = self._input_layer(features)
    for i in range(len(self._hidden_layers)):
      net = self._hidden_layers[i](net)
      if self._dropout is not None and is_training:
        net = self._dropout_layers[i](net, training=True)
      if self._batch_norm:
        net = self._batch_norm_layers[i](net, training=is_training)
      _add_hidden_layer_summary(net, self._hidden_layer_scope_names[i])

    logits = self._logits_layer(net)
    _add_hidden_layer_summary(logits, self._logits_scope_name)
    return logits


def _validate_features(features):
  if not isinstance(features, dict):
    raise ValueError('features should be a dictionary of `Tensor`s. '
                     'Given type: {}'.format(type(features)))


def _get_dnn_estimator_spec(use_tpu, head, features, labels, mode, logits,
                            optimizer):
  """Get EstimatorSpec for DNN Model."""
  if use_tpu:
    return head._create_tpu_estimator_spec(  # pylint: disable=protected-access
        features=features,
        mode=mode,
        labels=labels,
        optimizer=optimizer,
        logits=logits)
  else:
    return head.create_estimator_spec(
        features=features,
        mode=mode,
        labels=labels,
        optimizer=optimizer,
        logits=logits)


def _dnn_model_fn(features,
                  labels,
                  mode,
                  head,
                  hidden_units,
                  feature_columns,
                  optimizer='Adagrad',
                  activation_fn=tf.nn.relu,
                  dropout=None,
                  input_layer_partitioner=None,
                  config=None,
                  use_tpu=False,
                  batch_norm=False):
  """Deep Neural Net model_fn v1.

  Args:
    features: dict of `Tensor`.
    labels: `Tensor` of shape [batch_size, 1] or [batch_size] labels of dtype
      `int32` or `int64` in the range `[0, n_classes)`.
    mode: Defines whether this is training, evaluation or prediction. See
      `ModeKeys`.
    head: A `head_lib._Head` instance.
    hidden_units: Iterable of integer number of hidden units per layer.
    feature_columns: Iterable of `feature_column._FeatureColumn` model inputs.
    optimizer: String, `tf.Optimizer` object, or callable that creates the
      optimizer to use for training. If not specified, will use the Adagrad
      optimizer with a default learning rate of 0.05.
    activation_fn: Activation function applied to each layer.
    dropout: When not `None`, the probability we will drop out a given
      coordinate.
    input_layer_partitioner: Partitioner for input layer. Defaults to
      `min_max_variable_partitioner` with `min_slice_size` 64 << 20.
    config: `RunConfig` object to configure the runtime settings.
    use_tpu: Whether to make a DNN model able to run on TPU. Will make function
      return a `_TPUEstimatorSpec` instance and disable variable partitioning.
    batch_norm: Whether to use batch normalization after each hidden layer.

  Returns:
    An `EstimatorSpec` instance.

  Raises:
    ValueError: If features has the wrong type.
  """

  optimizer = optimizers.get_optimizer_instance(
      optimizer, learning_rate=_LEARNING_RATE)

  _validate_features(features)

  num_ps_replicas = config.num_ps_replicas if config else 0

  partitioner = (None if use_tpu else tf.compat.v1.min_max_variable_partitioner(
      max_partitions=num_ps_replicas))
  with tf.compat.v1.variable_scope(
      'dnn', values=tuple(six.itervalues(features)), partitioner=partitioner):
    input_layer_partitioner = input_layer_partitioner or (
        None if use_tpu else tf.compat.v1.min_max_variable_partitioner(
            max_partitions=num_ps_replicas, min_slice_size=64 << 20))

    logit_fn = dnn_logit_fn_builder(
        units=head.logits_dimension,
        hidden_units=hidden_units,
        feature_columns=feature_columns,
        activation_fn=activation_fn,
        dropout=dropout,
        input_layer_partitioner=input_layer_partitioner,
        batch_norm=batch_norm)
    logits = logit_fn(features=features, mode=mode)

    return _get_dnn_estimator_spec(use_tpu, head, features, labels, mode,
                                   logits, optimizer)


def _dnn_model_fn_builder_v2(units, hidden_units, feature_columns,
                             activation_fn, dropout, batch_norm, features,
                             mode):
  """Function builder for dnn logits, trainable variables and update ops.

  Args:
    units: An int indicating the dimension of the logit layer.  In the MultiHead
      case, this should be the sum of all component Heads' logit dimensions.
    hidden_units: Iterable of integer number of hidden units per layer.
    feature_columns: Iterable of `feature_column._FeatureColumn` model inputs.
    activation_fn: Activation function applied to each layer.
    dropout: When not `None`, the probability we will drop out a given
      coordinate.
    batch_norm: Whether to use batch normalization after each hidden layer.
    features: This is the first item returned from the `input_fn` passed to
      `train`, `evaluate`, and `predict`. This should be a single `Tensor` or
      `dict` of same.
    mode: Optional. Specifies if this training, evaluation or prediction. See
      `ModeKeys`.

  Returns:
    A `Tensor` representing the logits, or a list of `Tensor`'s representing
      multiple logits in the MultiHead case.
    A list of trainable variables.
    A list of update ops.

  Raises:
    ValueError: If units is not an int.
  """
  if not isinstance(units, six.integer_types):
    raise ValueError('units must be an int.  Given type: {}'.format(
        type(units)))
  dnn_model = _DNNModelV2(
      units,
      hidden_units,
      feature_columns,
      activation_fn,
      dropout,
      batch_norm,
      name='dnn')
  logits = dnn_model(features, mode)
  trainable_variables = dnn_model.trainable_variables
  update_ops = dnn_model.updates

  return logits, trainable_variables, update_ops


def dnn_model_fn_v2(features,
                    labels,
                    mode,
                    head,
                    hidden_units,
                    feature_columns,
                    optimizer='Adagrad',
                    activation_fn=tf.nn.relu,
                    dropout=None,
                    config=None,
                    use_tpu=False,
                    batch_norm=False):
  """Deep Neural Net model_fn v2.

  This function is different than _dnn_model_fn_v1 in the way it handles the
  optimizer when a String optimizer name is passed.

  Args:
    features: dict of `Tensor`.
    labels: `Tensor` of shape [batch_size, 1] or [batch_size] labels of dtype
      `int32` or `int64` in the range `[0, n_classes)`.
    mode: Defines whether this is training, evaluation or prediction. See
      `ModeKeys`.
    head: A `base_head.Head` instance.
    hidden_units: Iterable of integer number of hidden units per layer.
    feature_columns: Iterable of `feature_column._FeatureColumn` model inputs.
    optimizer: String, `tf.keras.optimizers.Optimizer` object, or callable that
      creates the optimizer to use for training. If not specified, will use the
      Adagrad optimizer. If it is String, the default learning rate of the
      optimizer will be used. If it is String, and optimizer does not have a
      default learning rate, then, a fixed learning rate of 0.05 is used.
    activation_fn: Activation function applied to each layer.
    dropout: When not `None`, the probability we will drop out a given
      coordinate.
    config: `RunConfig` object to configure the runtime settings.
    use_tpu: Whether to make a DNN model able to run on TPU. Will make function
      return a `_TPUEstimatorSpec` instance and disable variable partitioning.
    batch_norm: Whether to use batch normalization after each hidden layer.

  Returns:
    An `EstimatorSpec` instance.

  Raises:
    ValueError: If features has the wrong type.
  """
  _validate_features(features)

  del config

  logits, trainable_variables, update_ops = _dnn_model_fn_builder_v2(
      units=head.logits_dimension,
      hidden_units=hidden_units,
      feature_columns=feature_columns,
      activation_fn=activation_fn,
      dropout=dropout,
      batch_norm=batch_norm,
      features=features,
      mode=mode)

  # In TRAIN mode, create optimizer and assign global_step variable to
  # optimizer.iterations to make global_step increased correctly, as Hooks
  # relies on global step as step counter.
  if mode == ModeKeys.TRAIN:
    optimizer = optimizers.get_optimizer_instance_v2(optimizer)
    optimizer.iterations = tf.compat.v1.train.get_or_create_global_step()

  # Create EstimatorSpec.
  if use_tpu:
    estimator_spec_fn = head._create_tpu_estimator_spec  # pylint: disable=protected-access
  else:
    estimator_spec_fn = head.create_estimator_spec  # pylint: disable=protected-access

  return estimator_spec_fn(
      features=features,
      mode=mode,
      labels=labels,
      optimizer=optimizer,
      logits=logits,
      trainable_variables=trainable_variables,
      update_ops=update_ops)


@estimator_export('estimator.DNNClassifier', v1=[])
class DNNClassifierV2(estimator.EstimatorV2):
  """A classifier for TensorFlow DNN models.

  Example:

  ```python
  categorical_feature_a = categorical_column_with_hash_bucket(...)
  categorical_feature_b = categorical_column_with_hash_bucket(...)

  categorical_feature_a_emb = embedding_column(
      categorical_column=categorical_feature_a, ...)
  categorical_feature_b_emb = embedding_column(
      categorical_column=categorical_feature_b, ...)

  estimator = tf.estimator.DNNClassifier(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256])

  # Or estimator using the ProximalAdagradOptimizer optimizer with
  # regularization.
  estimator = tf.estimator.DNNClassifier(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256],
      optimizer=tf.compat.v1.train.ProximalAdagradOptimizer(
        learning_rate=0.1,
        l1_regularization_strength=0.001
      ))

  # Or estimator using an optimizer with a learning rate decay.
  estimator = tf.estimator.DNNClassifier(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256],
      optimizer=lambda: tf.keras.optimizers.Adam(
          learning_rate=tf.compat.v1.train.exponential_decay(
              learning_rate=0.1,
              global_step=tf.compat.v1.train.get_global_step(),
              decay_steps=10000,
              decay_rate=0.96))

  # Or estimator with warm-starting from a previous checkpoint.
  estimator = tf.estimator.DNNClassifier(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256],
      warm_start_from="/path/to/checkpoint/dir")

  # Input builders
  def input_fn_train:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_eval:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_predict:
    # Returns tf.data.Dataset of (x, None) tuple.
    pass
  estimator.train(input_fn=input_fn_train)
  metrics = estimator.evaluate(input_fn=input_fn_eval)
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
  otherwise there will be a `KeyError`:

  * if `weight_column` is not `None`, a feature with `key=weight_column` whose
    value is a `Tensor`.
  * for each `column` in `feature_columns`:
    - if `column` is a `CategoricalColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `WeightedCategoricalColumn`, two features: the first
      with `key` the id column name, the second with `key` the weight column
      name. Both features' `value` must be a `SparseTensor`.
    - if `column` is a `DenseColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss is calculated by using softmax cross entropy.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(
      self,
      hidden_units,
      feature_columns,
      model_dir=None,
      n_classes=2,
      weight_column=None,
      label_vocabulary=None,
      optimizer='Adagrad',
      activation_fn=tf.nn.relu,
      dropout=None,
      config=None,
      warm_start_from=None,
      loss_reduction=losses_utils.ReductionV2.SUM_OVER_BATCH_SIZE,
      batch_norm=False,
  ):
    """Initializes a `DNNClassifier` instance.

    Args:
      hidden_units: Iterable of number hidden units per layer. All layers are
        fully connected. Ex. `[64, 32]` means first layer has 64 nodes and
        second one has 32.
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `_FeatureColumn`.
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
        `_NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
        weight_column.normalizer_fn is applied on it to get weight tensor.
      label_vocabulary: A list of strings represents possible label values. If
        given, labels must be string type and have any value in
        `label_vocabulary`. If it is not given, that means labels are already
        encoded as integer or float within [0, 1] for `n_classes=2` and encoded
        as integer values in {0, 1,..., n_classes-1} for `n_classes`>2 . Also
        there will be errors if vocabulary is not provided and labels are
        string.
      optimizer: An instance of `tf.keras.optimizers.*` used to train the model.
        Can also be a string (one of 'Adagrad', 'Adam', 'Ftrl', 'RMSProp',
        SGD'), or callable. Defaults to Adagrad optimizer.
      activation_fn: Activation function applied to each layer. If `None`, will
        use `tf.nn.relu`.
      dropout: When not `None`, the probability we will drop out a given
        coordinate.
      config: `RunConfig` object to configure the runtime settings.
      warm_start_from: A string filepath to a checkpoint to warm-start from, or
        a `WarmStartSettings` object to fully configure warm-starting.  If the
        string filepath is provided instead of a `WarmStartSettings`, then all
        weights are warm-started, and it is assumed that vocabularies and Tensor
        names are unchanged.
      loss_reduction: One of `tf.losses.Reduction` except `NONE`. Describes how
        to reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`.
      batch_norm: Whether to use batch normalization after each hidden layer.
    """
    head = head_utils.binary_or_multi_class_head(
        n_classes,
        weight_column=weight_column,
        label_vocabulary=label_vocabulary,
        loss_reduction=loss_reduction)
    estimator._canned_estimator_api_gauge.get_cell('Classifier').set('DNN')

    def _model_fn(features, labels, mode, config):
      """Call the defined shared dnn_model_fn_v2."""
      return dnn_model_fn_v2(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          hidden_units=hidden_units,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          activation_fn=activation_fn,
          dropout=dropout,
          config=config,
          batch_norm=batch_norm)

    super(DNNClassifierV2, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


@estimator_export(v1=['estimator.DNNClassifier'])  # pylint: disable=missing-docstring
class DNNClassifier(estimator.Estimator):
  __doc__ = DNNClassifierV2.__doc__.replace('SUM_OVER_BATCH_SIZE', 'SUM')

  def __init__(
      self,
      hidden_units,
      feature_columns,
      model_dir=None,
      n_classes=2,
      weight_column=None,
      label_vocabulary=None,
      optimizer='Adagrad',
      activation_fn=tf.nn.relu,
      dropout=None,
      input_layer_partitioner=None,
      config=None,
      warm_start_from=None,
      loss_reduction=tf.compat.v1.losses.Reduction.SUM,
      batch_norm=False,
  ):
    head = head_lib._binary_logistic_or_multi_class_head(  # pylint: disable=protected-access
        n_classes, weight_column, label_vocabulary, loss_reduction)
    estimator._canned_estimator_api_gauge.get_cell('Classifier').set('DNN')

    def _model_fn(features, labels, mode, config):
      """Call the defined shared dnn_model_fn."""
      return _dnn_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          hidden_units=hidden_units,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          activation_fn=activation_fn,
          dropout=dropout,
          input_layer_partitioner=input_layer_partitioner,
          config=config,
          batch_norm=batch_norm)

    super(DNNClassifier, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


@estimator_export('estimator.DNNEstimator', v1=[])
class DNNEstimatorV2(estimator.EstimatorV2):
  """An estimator for TensorFlow DNN models with user-specified head.

  Example:

  ```python
  sparse_feature_a = sparse_column_with_hash_bucket(...)
  sparse_feature_b = sparse_column_with_hash_bucket(...)

  sparse_feature_a_emb = embedding_column(sparse_id_column=sparse_feature_a,
                                          ...)
  sparse_feature_b_emb = embedding_column(sparse_id_column=sparse_feature_b,
                                          ...)

  estimator = tf.estimator.DNNEstimator(
      head=tf.estimator.MultiLabelHead(n_classes=3),
      feature_columns=[sparse_feature_a_emb, sparse_feature_b_emb],
      hidden_units=[1024, 512, 256])

  # Or estimator using the ProximalAdagradOptimizer optimizer with
  # regularization.
  estimator = tf.estimator.DNNEstimator(
      head=tf.estimator.MultiLabelHead(n_classes=3),
      feature_columns=[sparse_feature_a_emb, sparse_feature_b_emb],
      hidden_units=[1024, 512, 256],
      optimizer=tf.compat.v1.train.ProximalAdagradOptimizer(
        learning_rate=0.1,
        l1_regularization_strength=0.001
      ))

  # Or estimator using an optimizer with a learning rate decay.
  estimator = tf.estimator.DNNEstimator(
      head=tf.estimator.MultiLabelHead(n_classes=3),
      feature_columns=[sparse_feature_a_emb, sparse_feature_b_emb],
      hidden_units=[1024, 512, 256],
      optimizer=lambda: tf.keras.optimizers.Adam(
          learning_rate=tf.compat.v1.train.exponential_decay(
              learning_rate=0.1,
              global_step=tf.compat.v1.train.get_global_step(),
              decay_steps=10000,
              decay_rate=0.96))

  # Or estimator with warm-starting from a previous checkpoint.
  estimator = tf.estimator.DNNEstimator(
      head=tf.estimator.MultiLabelHead(n_classes=3),
      feature_columns=[sparse_feature_a_emb, sparse_feature_b_emb],
      hidden_units=[1024, 512, 256],
      warm_start_from="/path/to/checkpoint/dir")

  # Input builders
  def input_fn_train:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_eval:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_predict:
    # Returns tf.data.Dataset of (x, None) tuple.
    pass
  estimator.train(input_fn=input_fn_train)
  metrics = estimator.evaluate(input_fn=input_fn_eval)
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
  otherwise there will be a `KeyError`:

  * if `weight_column` is not `None`, a feature with `key=weight_column` whose
    value is a `Tensor`.
  * for each `column` in `feature_columns`:
    - if `column` is a `CategoricalColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `WeightedCategoricalColumn`, two features: the first
      with `key` the id column name, the second with `key` the weight column
      name. Both features' `value` must be a `SparseTensor`.
    - if `column` is a `DenseColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss and predicted output are determined by the specified head.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(self,
               head,
               hidden_units,
               feature_columns,
               model_dir=None,
               optimizer='Adagrad',
               activation_fn=tf.nn.relu,
               dropout=None,
               config=None,
               warm_start_from=None,
               batch_norm=False):
    """Initializes a `DNNEstimator` instance.

    Args:
      head: A `_Head` instance constructed with a method such as
        `tf.contrib.estimator.multi_label_head`.
      hidden_units: Iterable of number hidden units per layer. All layers are
        fully connected. Ex. `[64, 32]` means first layer has 64 nodes and
        second one has 32.
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `_FeatureColumn`.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      optimizer: An instance of `tf.keras.optimizers.*` used to train the model.
        Can also be a string (one of 'Adagrad', 'Adam', 'Ftrl', 'RMSProp',
        SGD'), or callable. Defaults to Adagrad optimizer.
      activation_fn: Activation function applied to each layer. If `None`, will
        use `tf.nn.relu`.
      dropout: When not `None`, the probability we will drop out a given
        coordinate.
      config: `RunConfig` object to configure the runtime settings.
      warm_start_from: A string filepath to a checkpoint to warm-start from, or
        a `WarmStartSettings` object to fully configure warm-starting.  If the
        string filepath is provided instead of a `WarmStartSettings`, then all
        weights are warm-started, and it is assumed that vocabularies and Tensor
        names are unchanged.
      batch_norm: Whether to use batch normalization after each hidden layer.
    """

    def _model_fn(features, labels, mode, config):
      """Call the defined shared dnn_model_fn_v2."""
      return dnn_model_fn_v2(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          hidden_units=hidden_units,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          activation_fn=activation_fn,
          dropout=dropout,
          config=config,
          batch_norm=batch_norm)

    estimator._canned_estimator_api_gauge.get_cell('Estimator').set('DNN')  # pylint: disable=protected-access
    super(DNNEstimatorV2, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


@estimator_export(v1=['estimator.DNNEstimator'])  # pylint: disable=missing-docstring
class DNNEstimator(estimator.Estimator):
  __doc__ = DNNEstimatorV2.__doc__

  def __init__(self,
               head,
               hidden_units,
               feature_columns,
               model_dir=None,
               optimizer='Adagrad',
               activation_fn=tf.nn.relu,
               dropout=None,
               input_layer_partitioner=None,
               config=None,
               warm_start_from=None,
               batch_norm=False):

    def _model_fn(features, labels, mode, config):
      """Call the defined shared _dnn_model_fn."""
      return _dnn_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          hidden_units=hidden_units,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          activation_fn=activation_fn,
          dropout=dropout,
          input_layer_partitioner=input_layer_partitioner,
          config=config,
          batch_norm=batch_norm)

    estimator._canned_estimator_api_gauge.get_cell('Estimator').set('DNN')  # pylint: disable=protected-access
    super(DNNEstimator, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


@estimator_export('estimator.DNNRegressor', v1=[])
class DNNRegressorV2(estimator.EstimatorV2):
  """A regressor for TensorFlow DNN models.

  Example:

  ```python
  categorical_feature_a = categorical_column_with_hash_bucket(...)
  categorical_feature_b = categorical_column_with_hash_bucket(...)

  categorical_feature_a_emb = embedding_column(
      categorical_column=categorical_feature_a, ...)
  categorical_feature_b_emb = embedding_column(
      categorical_column=categorical_feature_b, ...)

  estimator = tf.estimator.DNNRegressor(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256])

  # Or estimator using the ProximalAdagradOptimizer optimizer with
  # regularization.
  estimator = tf.estimator.DNNRegressor(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256],
      optimizer=tf.compat.v1.train.ProximalAdagradOptimizer(
        learning_rate=0.1,
        l1_regularization_strength=0.001
      ))

  # Or estimator using an optimizer with a learning rate decay.
  estimator = tf.estimator.DNNRegressor(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256],
      optimizer=lambda: tf.keras.optimizers.Adam(
          learning_rate=tf.compat.v1.train.exponential_decay(
              learning_rate=0.1,
              global_step=tf.compat.v1.train.get_global_step(),
              decay_steps=10000,
              decay_rate=0.96))

  # Or estimator with warm-starting from a previous checkpoint.
  estimator = tf.estimator.DNNRegressor(
      feature_columns=[categorical_feature_a_emb, categorical_feature_b_emb],
      hidden_units=[1024, 512, 256],
      warm_start_from="/path/to/checkpoint/dir")

  # Input builders
  def input_fn_train:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_eval:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_predict:
    # Returns tf.data.Dataset of (x, None) tuple.
    pass
  estimator.train(input_fn=input_fn_train)
  metrics = estimator.evaluate(input_fn=input_fn_eval)
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
  otherwise there will be a `KeyError`:

  * if `weight_column` is not `None`, a feature with `key=weight_column` whose
    value is a `Tensor`.
  * for each `column` in `feature_columns`:
    - if `column` is a `CategoricalColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `WeightedCategoricalColumn`, two features: the first
      with `key` the id column name, the second with `key` the weight column
      name. Both features' `value` must be a `SparseTensor`.
    - if `column` is a `DenseColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss is calculated by using mean squared error.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(
      self,
      hidden_units,
      feature_columns,
      model_dir=None,
      label_dimension=1,
      weight_column=None,
      optimizer='Adagrad',
      activation_fn=tf.nn.relu,
      dropout=None,
      config=None,
      warm_start_from=None,
      loss_reduction=losses_utils.ReductionV2.SUM_OVER_BATCH_SIZE,
      batch_norm=False,
  ):
    """Initializes a `DNNRegressor` instance.

    Args:
      hidden_units: Iterable of number hidden units per layer. All layers are
        fully connected. Ex. `[64, 32]` means first layer has 64 nodes and
        second one has 32.
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      label_dimension: Number of regression targets per example. This is the
        size of the last dimension of the labels and logits `Tensor` objects
        (typically, these have shape `[batch_size, label_dimension]`).
      weight_column: A string or a `NumericColumn` created by
        `tf.feature_column.numeric_column` defining feature column representing
        weights. It is used to down weight or boost examples during training. It
        will be multiplied by the loss of the example. If it is a string, it is
        used as a key to fetch weight tensor from the `features`. If it is a
        `NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
        weight_column.normalizer_fn is applied on it to get weight tensor.
      optimizer: An instance of `tf.keras.optimizers.*` used to train the model.
        Can also be a string (one of 'Adagrad', 'Adam', 'Ftrl', 'RMSProp',
        SGD'), or callable. Defaults to Adagrad optimizer.
      activation_fn: Activation function applied to each layer. If `None`, will
        use `tf.nn.relu`.
      dropout: When not `None`, the probability we will drop out a given
        coordinate.
      config: `RunConfig` object to configure the runtime settings.
      warm_start_from: A string filepath to a checkpoint to warm-start from, or
        a `WarmStartSettings` object to fully configure warm-starting.  If the
        string filepath is provided instead of a `WarmStartSettings`, then all
        weights are warm-started, and it is assumed that vocabularies and Tensor
        names are unchanged.
      loss_reduction: One of `tf.losses.Reduction` except `NONE`. Describes how
        to reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`.
      batch_norm: Whether to use batch normalization after each hidden layer.
    """
    head = regression_head.RegressionHead(
        label_dimension=label_dimension,
        weight_column=weight_column,
        loss_reduction=loss_reduction)
    estimator._canned_estimator_api_gauge.get_cell('Regressor').set('DNN')  # pylint: disable=protected-access

    def _model_fn(features, labels, mode, config):
      """Call the defined shared dnn_model_fn_v2."""
      return dnn_model_fn_v2(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          hidden_units=hidden_units,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          activation_fn=activation_fn,
          dropout=dropout,
          config=config,
          batch_norm=batch_norm)

    super(DNNRegressorV2, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


@estimator_export(v1=['estimator.DNNRegressor'])  # pylint: disable=missing-docstring
class DNNRegressor(estimator.Estimator):
  __doc__ = DNNRegressorV2.__doc__.replace('SUM_OVER_BATCH_SIZE', 'SUM')

  def __init__(
      self,
      hidden_units,
      feature_columns,
      model_dir=None,
      label_dimension=1,
      weight_column=None,
      optimizer='Adagrad',
      activation_fn=tf.nn.relu,
      dropout=None,
      input_layer_partitioner=None,
      config=None,
      warm_start_from=None,
      loss_reduction=tf.compat.v1.losses.Reduction.SUM,
      batch_norm=False,
  ):
    head = head_lib._regression_head(  # pylint: disable=protected-access
        label_dimension=label_dimension,
        weight_column=weight_column,
        loss_reduction=loss_reduction)
    estimator._canned_estimator_api_gauge.get_cell('Regressor').set('DNN')  # pylint: disable=protected-access

    def _model_fn(features, labels, mode, config):
      """Call the defined shared _dnn_model_fn."""
      return _dnn_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          hidden_units=hidden_units,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          activation_fn=activation_fn,
          dropout=dropout,
          input_layer_partitioner=input_layer_partitioner,
          config=config,
          batch_norm=batch_norm)

    super(DNNRegressor, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)
