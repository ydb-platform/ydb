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
"""Abstractions for the base head class."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc

import six
import tensorflow as tf
from tensorflow.python.feature_column import feature_column_lib
from tensorflow.python.feature_column.feature_column import _LazyBuilder
from tensorflow.python.feature_column.feature_column import _NumericColumn
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import ops
from tensorflow.python.keras.optimizer_v2 import optimizer_v2
from tensorflow.python.keras.utils import losses_utils
from tensorflow.python.ops import weights_broadcast_ops
from tensorflow.python.util import function_utils
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator.canned import metric_keys
from tensorflow_estimator.python.estimator.export import export_output

DEFAULT_SERVING_KEY = tf.saved_model.DEFAULT_SERVING_SIGNATURE_DEF_KEY

# The above default is defined by TF Serving, but these next three are just
# a local convention without any special meaning.
CLASSIFY_SERVING_KEY = 'classification'
REGRESS_SERVING_KEY = 'regression'
PREDICT_SERVING_KEY = 'predict'


@estimator_export('estimator.Head')
@six.add_metaclass(abc.ABCMeta)
class Head(object):
  """Interface for the head/top of a model.

  Head sits on top of the model network and handles computing the outputs of
  the network. Given logits (or output of a hidden layer), a Head knows how to
  compute predictions, loss, train_op, metrics and export outputs. It is meant
  to:

  1. Simplify writing model_fn and to make model_fn more configurable for
     Estimator.
  2. Simpilfy creating loss and metrics for the train and test loop in Eager
     execution.
  3. Support wide range of machine learning models. Since most heads can work
     with logits, they can support DNN, RNN, Wide, Wide&Deep,
     Global objectives, Gradient boosted trees and many other types
     of machine learning models.

  Common usage:
  Here is simplified model_fn to build a DNN regression model.
    ```python
    def _my_dnn_model_fn(features, labels, mode, params, config=None):
      # Optionally your callers can pass head to model_fn as a param.
      head = tf.estimator.RegressionHead(...)

      feature_columns = tf.feature_column.numeric_column(...)
      feature_layer = tf.keras.layers.DenseFeatures(feature_columns)
      inputs = feature_layer(features)

      # Compute logits with tf.keras.layers API
      hidden_layer0 = tf.keras.layers.Dense(
          units=1000, activation="relu")(inputs)
      hidden_layer1 = tf.keras.layers.Dense(
          units=500, activation="relu")(hidden_layer0)
      logits = tf.keras.layers.Dense(
          units=head.logits_dimension, activation=None)(hidden_layer1)

      # Or use Keras model for logits computation
      model = tf.keras.Sequential()
      model.add(tf.keras.layers.Dense(units=1000, activation="relu"))
      model.add(tf.keras.layers.Dense(units=500, activation="relu"))
      model.add(tf.keras.layers.Dense(
         units=head.logits_dimension, activation=None))
      logits = model(inputs)

      return head.create_estimator_spec(
          features=features,
          labels=labels,
          mode=mode,
          logits=logits,
          optimizer=optimizer)
    ```
  """

  @abc.abstractproperty
  def name(self):
    """The name of this head.

    Returns:
      A string.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractproperty
  def logits_dimension(self):
    """Size of the last dimension of the logits `Tensor`.

    Often is the number of classes, labels, or real values to be predicted.
    Typically, logits is of shape `[batch_size, logits_dimension]`.

    Returns:
      The expected size of the `logits` tensor.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractproperty
  def loss_reduction(self):
    """One of `tf.losses.Reduction`.

    Describes how to reduce training loss over batch, such as mean or sum.

    Returns:
      The type of loss reduction used in the head.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def loss(self,
           labels,
           logits,
           features=None,
           mode=None,
           regularization_losses=None):
    """Returns a loss `Tensor` from provided arguments.

    Note that, the args of `features` and `mode` are most likely not used, but
    some Head implementations may require them.

    Args:
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      logits: Logits `Tensor` to be used for loss construction.
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      mode: Estimator's `ModeKeys`. To be used in case loss calculation is
        different in Train and Eval mode.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
      A scalar `Tensor` representing regularized training loss used in train and
      eval.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def predictions(self, logits, keys=None):
    """Returns a `dict` of predictions from provided logits.

    Args:
      logits: Logits `Tensor` to be used for prediction construction.
      keys: A list of `string` for prediction keys. Defaults to `None`, meaning
        if not specified, predictions will be created for all the pre-defined
        valid keys in the head.

    Returns:
      A `dict` of predicted `Tensor` keyed by prediction name.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def metrics(self, regularization_losses=None):
    """Returns a `dict` of metric objects.

    Args:
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
       A `dict` of metrics keyed by string name. The value is an instance of
       `Metric` class.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def update_metrics(self,
                     eval_metrics,
                     features,
                     logits,
                     labels,
                     mode=None,
                     regularization_losses=None):
    """Updates metric objects and returns a `dict` of the updated metrics.

    Args:
      eval_metrics: A `dict` of metrics to be updated.
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      logits: logits `Tensor` to be used for metrics update.
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      mode: Estimator's `ModeKeys`. In most cases, this arg is not used and can
        be removed in the method implementation.
      regularization_losses: A list of additional scalar losses to be added to
        the training and evaluation loss, such as regularization losses.  Note
        that, the `mode` arg is not used in the `tf.estimator.*Head`. If the
        update of the metrics doesn't rely on `mode`, it can be safely ignored
        in the method signature.

    Returns:
       A `dict` of updated metrics keyed by name. The value is an instance of
       `Metric` class.
    """
    raise NotImplementedError('Calling an abstract method.')

  def _summary_key(self, key):
    return '{}/{}'.format(key, self.name) if self.name else key

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

    It is recommended to pass all args via name.

    Args:
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      mode: Estimator's `ModeKeys`.
      logits: Logits `Tensor` to be used by the head.
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      optimizer: An `tf.keras.optimizers.Optimizer` instance to optimize the
        loss in TRAIN mode. Namely, sets `train_op = optimizer.get_updates(loss,
        trainable_variables)`, which updates variables to minimize `loss`.
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
    # Not all subclasses of Head will have implemented
    # _create_tpu_estimator_spec. If it is implemented, we can convert it to
    # the normal `EstimatorSpec` by calling the method of
    # `_TPUEstimatorSpec.as_estimator_spec()`.
    try:
      tpu_estimator_spec = (
          self._create_tpu_estimator_spec(
              features=features,
              mode=mode,
              logits=logits,
              labels=labels,
              optimizer=optimizer,
              trainable_variables=trainable_variables,
              train_op_fn=train_op_fn,
              update_ops=update_ops,
              regularization_losses=regularization_losses))
      return tpu_estimator_spec.as_estimator_spec()
    except NotImplementedError:
      raise NotImplementedError(
          'Subclasses of Head must implement `create_estimator_spec()` or '
          '_create_tpu_estimator_spec().')

  def _create_tpu_estimator_spec(
      self,
      features,
      mode,
      logits,
      labels=None,
      optimizer=None,
      trainable_variables=None,
      train_op_fn=None,
      update_ops=None,
      regularization_losses=None,
  ):
    """Returns `model_fn._TPUEstimatorSpec` that a model_fn can return.

    Args:
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      mode: Estimator's `ModeKeys`.
      logits: Logits `Tensor` to be used by the head.
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      optimizer: An `tf.keras.optimizers.Optimizer` instance to optimize the
        loss in TRAIN mode. Namely, sets `train_op = optimizer.get_updates(loss,
        trainable_variables)`, which updates variables to minimize `loss`.
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
      A `model_fn._TPUEstimatorSpec' instance.
    """
    raise NotImplementedError(
        'TPUEstimatorSpec not available for this model head.')


# TODO(b/119617064): unify eager and graph implementations
# Note that, tensor shape checking is slow in Eager mode. To amend it, the
# tensor static shape is used for checking. The duplication of shape checking
# for eager mode in the following helper functions can be safely removed
# if there's some way to get around it in the future.

# Label shape error messages.
_LABEL_NONE_ERR_MSG = (
    'You must provide a labels Tensor. Given: None. '
    'Suggested troubleshooting steps: Check that your data contains your label '
    'feature. Check that your input_fn properly parses and returns labels.')

_SPARSE_LABEL_ERR_MSG = (
    'SparseTensor labels are not supported. Labels must be a Tensor of shape '
    '[D0, D1, ..., DN, {}], e.g. [batch_size, {}].Suggested Fix (1): Check the'
    ' label feature in your data. Each example must contain {} value(s). If '
    'not, your choice of label was probably incorrect. Suggested Fix (2): In '
    'your input_fn, use tf.sparse_tensor_to_dense() to turn labels into a '
    'Tensor.')

_MISMATCHED_LABEL_DIM_ERR_MSG = (
    'Mismatched label shape. Expected labels dimension={}.  Received {}. '
    'Suggested Fix: If your classifier expects one-hot encoding label, check '
    'your n_classes argument to the estimator and/or the shape of your label. '
    'Otherwise, check the shape of your label.')

_LABEL_SHAPE_ERR_MSG = (
    'labels shape must be [D0, D1, ... DN, {}]. Suggested Fix: check your '
    'n_classes argument to the head and/or the shape of your label.')

_VALIDATION_ERROR_MSG = '{} should be a list or a tuple. Given type: {}.'


def check_dense_labels_match_logits_and_reshape(labels, logits,
                                                expected_labels_dimension):
  """Checks labels shape matches logits, and reshapes if needed.

  Consider logits of shape [D0, D1, ... DN, logits_dimension]. Then labels
  shape must be [D0, D1, ... DN, expected_labels_dimension].
  If expected_labels_dimension=1, labels could be [D0, D1, ... DN] and this
  method reshapes them to [D0, D1, ... DN, 1].

  Args:
    labels: labels Tensor.
    logits: logits Tensor.
    expected_labels_dimension: Integer.

  Returns:
    Validated and reshaped labels Tensor.

  Raises:
    ValueError: If labels is a SparseTensor.
    ValueError: If labels shape is statically defined and fails validation.
    OpError: If labels shape is not statically defined and fails validation.
  """
  if labels is None:
    raise ValueError(_LABEL_NONE_ERR_MSG)
  with ops.name_scope('labels', values=(labels, logits)) as scope:
    labels = tf.compat.v1.convert_to_tensor_or_sparse_tensor(labels)
    if isinstance(labels, tf.sparse.SparseTensor):
      raise ValueError(
          _SPARSE_LABEL_ERR_MSG.format(expected_labels_dimension,
                                       expected_labels_dimension,
                                       expected_labels_dimension))
    # Eager mode.
    if tf.executing_eagerly():
      labels_rank = labels._rank()  # pylint: disable=protected-access
      logits_rank = logits._rank()  # pylint: disable=protected-access
      if (labels_rank is not None and logits_rank is not None and
          labels_rank == logits_rank - 1):
        labels = tf.compat.v1.expand_dims(labels, -1)
        labels_rank += 1
      labels_shape = labels._shape_tuple()  # pylint: disable=protected-access
      if labels_rank < 2:
        raise ValueError('labels must have rank at least 2.  Received rank {}, '
                         'shape {}'.format(labels_rank, labels_shape))
      if labels_shape[-1] != expected_labels_dimension:
        raise ValueError(
            _MISMATCHED_LABEL_DIM_ERR_MSG.format(expected_labels_dimension,
                                                 labels_shape[-1]))
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      expected_labels_shape = logits_shape[:-1] + (expected_labels_dimension,)
      if expected_labels_shape != labels_shape:
        raise ValueError(
            '{}, expected_labels_shape: {}. labels_shape: {}.'.format(
                _LABEL_SHAPE_ERR_MSG.format(expected_labels_dimension),
                expected_labels_shape, labels_shape))
      return labels

    # Graph mode.
    if (labels.shape.ndims is not None and logits.shape.ndims is not None and
        labels.shape.ndims == logits.shape.ndims - 1):
      labels = tf.compat.v1.expand_dims(labels, -1)
    assert_rank = tf.compat.v1.debugging.assert_rank_at_least(
        labels,
        2,
        message=_LABEL_SHAPE_ERR_MSG.format(expected_labels_dimension))
    with tf.control_dependencies([assert_rank]):
      static_shape = labels.shape
      if static_shape.ndims is not None:
        final_dim = static_shape[-1]
        if (final_dim is not None) and (final_dim != expected_labels_dimension):
          raise ValueError(
              _MISMATCHED_LABEL_DIM_ERR_MSG.format(expected_labels_dimension,
                                                   final_dim))
      logits_shape = tf.compat.v1.shape(logits)
      expected_labels_shape = tf.concat(
          [logits_shape[:-1], [expected_labels_dimension]], axis=0)
      labels_shape = tf.compat.v1.shape(labels)
      assert_dimension = tf.compat.v1.debugging.assert_equal(
          expected_labels_shape,
          labels_shape,
          message=_LABEL_SHAPE_ERR_MSG.format(expected_labels_dimension),
          data=[
              'expected_labels_shape: ', expected_labels_shape,
              'labels_shape: ', labels_shape
          ])
      with tf.control_dependencies([assert_dimension]):
        return tf.identity(labels, name=scope)


def get_weights_and_check_match_logits(features,
                                       weight_column,
                                       logits,
                                       allow_per_logit_weights=False):
  """Fetches weights from features and checks that the shape matches logits.

  Consider logits of shape [D0, D1, ... DN, logits_dimension]. Weights shape
  can be either:
  * [D0, D1, ... DN, logits_dimension] if `allow_per_logit_weights=True`.
  * [D0, D1, ... DN, 1]
  * [D0, D1, ... DN]: In this case, weights is reshaped into
    [D0, D1, ... DN, 1] to work with weight broadcasting rules.

  Args:
    features: The features dict that contains weights.
    weight_column: The weight column. If not given, this method returns 1.
    logits: logits Tensor.
    allow_per_logit_weights: Boolean. Whether we allow weights along the logits
      dimension, namely shape `[D0, D1, ... DN, logits_dimension]`.

  Returns:
    Validated and reshaped weights Tensor.

  Raises:
    ValueError: If the weights `Tensor` cannot be cast into float.
  """
  if allow_per_logit_weights:
    err_msg = ('weights shape must be [D0, D1, ... DN], [D0, D1, ... DN, 1] or '
               '[D0, D1, ... DN, logits_dimension]')
  else:
    err_msg = ('weights shape must be [D0, D1, ... DN] or [D0, D1, ... DN, 1]')
  with ops.name_scope(
      'weights', values=tuple(six.itervalues(features)) + (logits,)) as scope:
    # Fetch the weights.
    if weight_column is None:
      return 1.
    # TODO(b/117839674): update feature_column
    if isinstance(weight_column, six.string_types):
      weight_column = tf.feature_column.numeric_column(
          key=weight_column, shape=(1,))
    if not isinstance(weight_column,
                      (feature_column_lib.NumericColumn, _NumericColumn)):
      raise TypeError('Weight column must be either a string or NumericColumn.'
                      ' Given type: {}.'.format(type(weight_column)))
    weights = weight_column._get_dense_tensor(  # pylint: disable=protected-access
        _LazyBuilder(features))
    if not (weights.dtype.is_floating or weights.dtype.is_integer):
      raise ValueError('Weight column should be castable to float. '
                       'Given dtype: {}'.format(weights.dtype))
    weights = tf.cast(weights, name='weights', dtype=tf.dtypes.float32)
    # Validate the weights shape.
    # Eager mode.
    if tf.executing_eagerly():
      weights_shape = weights._shape_tuple()  # pylint: disable=protected-access
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      weights_rank = weights._rank()  # pylint: disable=protected-access
      logits_rank = logits._rank()  # pylint: disable=protected-access
      if (weights_rank is not None and logits_rank is not None and
          weights_rank == logits_rank - 1):
        if logits_shape[:-1] != weights_shape:
          raise ValueError('{}, logits_shape: {}. weights_shape: {}.'.format(
              err_msg, logits_shape, weights_shape))
        return tf.compat.v1.expand_dims(weights, -1, name=scope)
      supported_weights_shape = logits_shape[:-1] + (1,)
      if allow_per_logit_weights:
        if (logits_shape != weights_shape and
            supported_weights_shape != weights_shape):
          raise ValueError('{}, logits_shape: {}. weights_shape: {}.'.format(
              err_msg, logits_shape, weights_shape))
      else:
        if supported_weights_shape != weights_shape:
          raise ValueError('{}, logits_shape: {}. weights_shape: {}.'.format(
              err_msg, logits_shape, weights_shape))
      return weights

    # Graph mode.
    weights_shape = tf.compat.v1.shape(weights, name='weights_shape')
    logits_shape = tf.compat.v1.shape(logits, name='logits_shape')
    if (weights.shape.ndims is not None and logits.shape.ndims is not None and
        weights.shape.ndims == logits.shape.ndims - 1):
      assert_dimension = tf.compat.v1.debugging.assert_equal(
          logits_shape[:-1],
          weights_shape,
          message=err_msg,
          data=[
              'logits_shape: ', logits_shape, 'weights_shape: ', weights_shape
          ])
      with tf.control_dependencies([assert_dimension]):
        return tf.compat.v1.expand_dims(weights, -1, name=scope)
    supported_weights_shape = tf.concat([logits_shape[:-1], [1]], axis=0)
    if allow_per_logit_weights:
      condition = tf.math.reduce_any([
          tf.reduce_all(tf.math.equal(logits_shape, weights_shape)),
          tf.reduce_all(tf.math.equal(supported_weights_shape, weights_shape))
      ])
      assert_dimension = tf.debugging.Assert(
          condition=condition,
          data=[
              err_msg, 'logits_shape: ', logits_shape, 'weights_shape: ',
              weights_shape
          ])
    else:
      assert_dimension = tf.compat.v1.debugging.assert_equal(
          supported_weights_shape,
          weights_shape,
          message=err_msg,
          data=[
              'logits_shape: ', logits_shape, 'weights_shape: ', weights_shape
          ])
    with tf.control_dependencies([assert_dimension]):
      return tf.identity(weights, name=scope)


def check_logits_final_dim(logits, expected_logits_dimension):
  """Checks that logits shape is [D0, D1, ... DN, logits_dimension]."""
  with ops.name_scope('logits', values=(logits,)) as scope:
    logits = tf.cast(logits, tf.dtypes.float32)
    # Eager mode
    if tf.executing_eagerly():
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      logits_rank = logits._rank()  # pylint: disable=protected-access
      if logits_rank < 2:
        raise ValueError('logits must have rank at least 2.  Received rank {}, '
                         'shape {}'.format(logits_rank, logits_shape))
      if (isinstance(expected_logits_dimension, int) and
          logits_shape[-1] != expected_logits_dimension):
        raise ValueError(
            'logits shape must be [D0, D1, ... DN, logits_dimension], '
            'got {}.'.format(logits_shape))
      return logits
    # Graph mode
    logits_shape = tf.compat.v1.shape(logits)
    assert_rank = tf.compat.v1.debugging.assert_rank_at_least(
        logits,
        2,
        data=[logits_shape],
        message='logits shape must be [D0, D1, ... DN, logits_dimension]')
    with tf.control_dependencies([assert_rank]):
      static_shape = logits.shape
      if static_shape.ndims is not None and static_shape[-1] is not None:
        if (isinstance(expected_logits_dimension, int) and
            static_shape[-1] != expected_logits_dimension):
          raise ValueError(
              'logits shape must be [D0, D1, ... DN, logits_dimension], '
              'got {}.'.format(static_shape))
        return logits
      assert_dimension = tf.compat.v1.debugging.assert_equal(
          expected_logits_dimension,
          logits_shape[-1],
          data=[logits_shape],
          message='logits shape must be [D0, D1, ... DN, logits_dimension]')
      with tf.control_dependencies([assert_dimension]):
        return tf.identity(logits, name=scope)


def validate_loss_fn_args(loss_fn):
  """Validates loss_fn arguments.

  Required arguments: labels, logits.
  Optional arguments: features, loss_reduction.

  Args:
    loss_fn: The loss function.

  Raises:
    ValueError: If the signature is unexpected.
  """
  loss_fn_args = function_utils.fn_args(loss_fn)
  for required_arg in ['labels', 'logits']:
    if required_arg not in loss_fn_args:
      raise ValueError('loss_fn must contain argument: {}. '
                       'Given arguments: {}'.format(required_arg, loss_fn_args))
  invalid_args = list(
      set(loss_fn_args) -
      set(['labels', 'logits', 'features', 'loss_reduction']))
  if invalid_args:
    raise ValueError('loss_fn has unexpected args: {}'.format(invalid_args))


def validate_loss_reduction(loss_reduction):
  if (loss_reduction not in losses_utils.ReductionV2.all() or
      loss_reduction == losses_utils.ReductionV2.NONE):
    raise ValueError(
        'Invalid loss_reduction: {}. See `tf.losses.Reduction` for valid '
        'options.'.format(loss_reduction))


def validate_update_ops(update_ops=None):
  if update_ops is not None and not isinstance(update_ops, (list, tuple)):
    raise ValueError(
        _VALIDATION_ERROR_MSG.format('update_ops', type(update_ops)))


def validate_v2_optimizer(optimzier):
  if not isinstance(optimzier, optimizer_v2.OptimizerV2):
    raise ValueError(
        'The given optimizer is not a tf.keras.optimizers.Optimizer instance. '
        'Given: {}'.format(optimzier))


def validate_trainable_variables(trainable_variables=None):
  if trainable_variables is None:
    raise ValueError('trainable_variables cannot be None. Given {}'.format(
        trainable_variables))
  if not isinstance(trainable_variables, (list, tuple)):
    raise ValueError(
        _VALIDATION_ERROR_MSG.format('trainable_variables',
                                     type(trainable_variables)))


def validate_n_classes(n_classes):
  """Validates n_classes argument.

  Required arguments: n_classes.

  Args:
    n_classes: The number of classes.

  Raises:
    ValueError: If n_classes is <= 2 and n_classes is a Python integer.
  Returns:
    n_classes in its original type.
  """
  if isinstance(n_classes, int) and (n_classes <= 2):
    raise ValueError('n_classes must be > 2: %s.' % n_classes)

  n_classes_as_tensor = ops.convert_to_tensor(n_classes)
  assert_n_classes = tf.compat.v1.debugging.assert_greater(
      n_classes_as_tensor, 2, message='n_classes must be greater than 2')
  with tf.control_dependencies([assert_n_classes]):
    tf.no_op()
  # Return n_classes in its original type, so that any code
  # using the accessor logits_dimension() has the original type.
  return n_classes


def call_loss_fn(loss_fn, labels, logits, features, expected_loss_dim=1):
  """Calls loss_fn and checks the returned shape.

  For shape checking, eager uses the static dimension to improve performance.

  Args:
    loss_fn: The loss function.
    labels: Processed labels Tensor.
    logits: Logits Tensor of shape [D0, D1, ... DN, logits_dimension].
    features: Features dict.
    expected_loss_dim: The expected last dimension of loss Tensor.

  Returns:
    Loss Tensor with shape [D0, D1, ... DN, expected_loss_dim].

  Raises:
    ValueError: If the loss tensor shape is unexpected.
  """
  loss_fn_args = function_utils.fn_args(loss_fn)
  kwargs = {}
  if 'features' in loss_fn_args:
    kwargs['features'] = features
  with ops.name_scope(
      'call_loss_fn', values=[labels, logits] + list(six.itervalues(features))):
    unweighted_loss = loss_fn(labels=labels, logits=logits, **kwargs)
    # Eager mode.
    if tf.executing_eagerly():
      loss_shape = unweighted_loss._shape_tuple()  # pylint: disable=protected-access
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      expected_loss_shape = logits_shape[:-1] + (expected_loss_dim,)
      if loss_shape != expected_loss_shape:
        raise ValueError(
            'loss_fn must return Tensor of shape '
            '[D0, D1, ... DN, {}]. '.format(expected_loss_dim),
            'logits_shape: ', logits_shape, 'loss_shape: ', loss_shape)
      return unweighted_loss
    # Graph mode.
    logits_shape = tf.compat.v1.shape(logits, name='logits_shape')
    expected_loss_shape = tf.concat([logits_shape[:-1], [expected_loss_dim]],
                                    axis=0,
                                    name='expected_loss_shape')
    loss_shape = tf.compat.v1.shape(unweighted_loss, name='loss_shape')
    check_loss_shape_op = tf.debugging.Assert(
        tf.reduce_all(tf.math.equal(loss_shape, expected_loss_shape)),
        data=[
            'loss_fn must return Tensor of shape '
            '[D0, D1, ... DN, {}]. '.format(expected_loss_dim),
            'logits_shape: ', logits_shape, 'loss_shape: ', loss_shape
        ],
        name='check_loss_shape')
    with tf.control_dependencies([check_loss_shape_op]):
      return tf.identity(unweighted_loss)


def check_prediction_keys(pred_keys, valid_keys):
  for key in pred_keys:
    if key not in valid_keys:
      raise ValueError('Prediction key must be in PredictionKeys, given: {}.'
                       'Valid prediction keys include {}.'.format(
                           key, valid_keys))


def all_class_ids(logits, n_classes):
  batch_size = tf.compat.v1.shape(logits)[0]
  class_id_list = tf.range(n_classes)
  return tf.tile(
      input=tf.compat.v1.expand_dims(input=class_id_list, axis=0),
      multiples=[batch_size, 1])


def all_classes(logits, n_classes, label_vocabulary=None):
  batch_size = tf.compat.v1.shape(logits)[0]
  if label_vocabulary:
    classes_list = label_vocabulary
  else:
    classes_list = tf.strings.as_string(tf.range(n_classes))
  return tf.tile(
      input=tf.compat.v1.expand_dims(input=classes_list, axis=0),
      multiples=[batch_size, 1])


def classification_output(scores, n_classes, label_vocabulary=None):
  return export_output.ClassificationOutput(
      scores=scores,
      # `ClassificationOutput` requires string classes.
      classes=all_classes(scores, n_classes, label_vocabulary))


def check_label_range(labels, n_classes, message=None):
  """Check if labels are in the range of [0, n_classes)."""
  with ops.name_scope('check_label_range', values=(labels,)):
    # Eager mode
    if tf.executing_eagerly():
      assert_less = tf.reduce_all(tf.math.less_equal(labels, n_classes - 1))
      if not assert_less:
        raise ValueError(message or
                         'Labels must be <= {} - 1'.format(n_classes))
      assert_greater = tf.reduce_all(tf.math.greater_equal(labels, 0))
      if not assert_greater:
        raise ValueError(message or 'Labels must be >= 0')
      return labels
    # Graph mode
    assert_less = tf.compat.v1.debugging.assert_less_equal(
        labels,
        ops.convert_to_tensor(n_classes - 1, dtype=labels.dtype),
        message=message or 'Labels must be <= n_classes - 1')
    assert_greater = tf.compat.v1.debugging.assert_non_negative(
        labels, message=message or 'Labels must be >= 0')
    with tf.control_dependencies((assert_less, assert_greater)):
      return tf.identity(labels)


def update_metric_with_broadcast_weights(eval_metric, values, weights):
  values = tf.cast(values, dtype=tf.dtypes.float32)
  if weights is not None:
    weights = weights_broadcast_ops.broadcast_weights(weights, values)
  eval_metric.update_state(values=values, sample_weight=weights)


def create_eval_metrics_tuple(fn, kwargs):
  """Creates TPU eval metrics tuple.

  Helper function to make eval_metric tuple (eval_metric_fn, fn_kwargs) used
  by `TPUEstimator`. TPUEstimator requires that `eval_metric_fn` take
  exclusively Tensor arguments. This helper can help create such a function from
  a more generic function that can take both Tensor and non-Tensor arguments.

  Args:
    fn: A eval_metric_fn that takes both Tensor and non-Tensor arguments. This
      function must return a dict of form
        {'metric name': (metric_tensor, eval_op)}
    kwargs: Dict of arguments for `fn`.

  Returns:
    `eval_metric` tuple that can be passed to a `model_fn._TPUEstimatorSpec`.
  """
  tensor_kwargs = {}
  nontensor_kwargs = {}
  for k, v in six.iteritems(kwargs):
    if tf.is_tensor(v):
      tensor_kwargs[k] = v
    else:
      nontensor_kwargs[k] = v

  def _fn(**tensors):
    return fn(**dict(nontensor_kwargs, **tensors))

  return (_fn, tensor_kwargs)


def create_estimator_spec_train_op(
    head_name,
    optimizer=None,
    trainable_variables=None,
    train_op_fn=None,
    update_ops=None,
    regularized_training_loss=None,
    loss_reduction=losses_utils.ReductionV2.SUM_OVER_BATCH_SIZE):
  """Create train_op for estimator_spec.

  Args:
    head_name: The name of the head.
    optimizer: An `tf.keras.optimizers.Optimizer` instance to optimize the loss
      in TRAIN mode. Namely, sets `train_op = optimizer.get_updates(loss,
      trainable_variables)`, which updates variables to minimize `loss`.
    trainable_variables: A list or tuple of `Variable` objects to update to
      minimize `loss`. In Tensorflow 1.x, by default these are the list of
      variables collected in the graph under the key
      `GraphKeys.TRAINABLE_VARIABLES`. As Tensorflow 2.x doesn't have
      collections and GraphKeys, trainable_variables need to be passed
      explicitly here.
    train_op_fn: Function that takes a scalar loss `Tensor` and returns
      `train_op`. Used if `optimizer` is `None`.
    update_ops: A list or tuple of update ops to be run at training time. For
      example, layers such as BatchNormalization create mean and variance update
      ops that need to be run at training time. In Tensorflow 1.x, these are
      thrown into an UPDATE_OPS collection. As Tensorflow 2.x doesn't have
      collections, update_ops need to be passed explicitly here.
    regularized_training_loss: A scalar for total training loss that includes
      all regularization losses. If you're not using optimizer to generate train
      op, make sure to scale the loss correctly before passing it in. The loss
      typically needs to be scaled down by the number of workers.
    loss_reduction: One of `tf.keras.losses.Reduction` except `NONE`. Describes
      how to reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`.

  Returns:
    A train op for EstimatorSpec.
  """
  del head_name
  validate_update_ops(update_ops)
  with ops.name_scope(''):  # Reset all previous name_scope.
    # Add training as the name_scope to be compatible with Keras.
    with ops.name_scope('training'):
      if optimizer is not None:
        if train_op_fn is not None:
          raise ValueError('train_op_fn and optimizer cannot both be set.')
        validate_v2_optimizer(optimizer)
        validate_trainable_variables(trainable_variables)
        # Scale loss by number of replicas.
        if loss_reduction == losses_utils.ReductionV2.SUM_OVER_BATCH_SIZE:
          regularized_training_loss = losses_utils.scale_loss_for_distribution(
              regularized_training_loss)
        train_op = optimizer.get_updates(regularized_training_loss,
                                         trainable_variables)[0]
      elif train_op_fn is not None:
        train_op = train_op_fn(regularized_training_loss)
      else:
        raise ValueError('train_op_fn and optimizer cannot both be None.')
      if update_ops is not None:
        train_op = tf.group(train_op, *update_ops)
      return train_op


def create_estimator_spec_summary(regularized_training_loss,
                                  regularization_losses=None,
                                  summary_key_fn=None):
  """Create summary for estimator_spec."""
  with ops.name_scope(''):
    keys = metric_keys.MetricKeys
    loss_key = summary_key_fn(keys.LOSS) if summary_key_fn else keys.LOSS
    tf.compat.v1.summary.scalar(loss_key, regularized_training_loss)
    if regularization_losses is not None:
      regularization_loss = tf.math.add_n(regularization_losses)
      regularization_loss_key = (
          summary_key_fn(keys.LOSS_REGULARIZATION)
          if summary_key_fn else keys.LOSS_REGULARIZATION)
      tf.compat.v1.summary.scalar(regularization_loss_key, regularization_loss)
