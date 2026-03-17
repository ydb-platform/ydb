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
"""Base class for time series models."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import collections

import six
import tensorflow as tf
from tensorflow_estimator.python.estimator.canned.timeseries import math_utils
from tensorflow_estimator.python.estimator.canned.timeseries.feature_keys import TrainEvalFeatures

ModelOutputs = collections.namedtuple(  # pylint: disable=invalid-name
    typename="ModelOutputs",
    field_names=[
        "loss",  # The scalar value to be minimized during training.
        "end_state",  # A nested tuple specifying the model's state after
                      # running on the specified data
        "predictions",  # A dictionary of predictions, each with shape prefixed
                        # by the shape of `prediction_times`.
        "prediction_times"  # A [batch size x window size] integer Tensor
                            # indicating times for which values in `predictions`
                            # were computed.
    ])


@six.add_metaclass(abc.ABCMeta)
class TimeSeriesModel(object):
  """Base class for creating generative time series models."""

  def __init__(self,
               num_features,
               exogenous_feature_columns=None,
               dtype=tf.dtypes.float32):
    """Constructor for generative models.

    Args:
      num_features: Number of features for the time series
      exogenous_feature_columns: A list of `tf.feature_column`s (for example
        `tf.feature_column.embedding_column`) corresponding to exogenous
        features which provide extra information to the model but are not part
        of the series to be predicted. Passed to
        `tf.feature_column.input_layer`.
      dtype: The floating point datatype to use.
    """
    if exogenous_feature_columns:
      self._exogenous_feature_columns = exogenous_feature_columns
    else:
      self._exogenous_feature_columns = []
    self.num_features = num_features
    self.dtype = dtype
    self._input_statistics = None
    self._graph_initialized = False
    self._stats_means = None
    self._stats_sigmas = None

  @property
  def exogenous_feature_columns(self):
    """`tf.feature_colum`s for features which are not predicted."""
    return self._exogenous_feature_columns

  # TODO(allenl): Move more of the generic machinery for generating and
  # predicting into TimeSeriesModel, and possibly share it between generate()
  # and predict()
  def generate(self,
               number_of_series,
               series_length,
               model_parameters=None,
               seed=None):
    """Sample synthetic data from model parameters, with optional substitutions.

    Returns `number_of_series` possible sequences of future values, sampled from
    the generative model with each conditioned on the previous. Samples are
    based on trained parameters, except for those parameters explicitly
    overridden in `model_parameters`.

    For distributions over future observations, see predict().

    Args:
      number_of_series: Number of time series to create.
      series_length: Length of each time series.
      model_parameters: A dictionary mapping model parameters to values, which
        replace trained parameters when generating data.
      seed: If specified, return deterministic time series according to this
        value.

    Returns:
      A dictionary with keys TrainEvalFeatures.TIMES (mapping to an array with
      shape [number_of_series, series_length]) and TrainEvalFeatures.VALUES
      (mapping to an array with shape [number_of_series, series_length,
      num_features]).
    """
    raise NotImplementedError("This model does not support generation.")

  def initialize_graph(self, input_statistics=None):
    """Define ops for the model, not depending on any previously defined ops.

    Args:
      input_statistics: A math_utils.InputStatistics object containing input
        statistics. If None, data-independent defaults are used, which may
        result in longer or unstable training.
    """
    self._graph_initialized = True
    self._input_statistics = input_statistics
    if self._input_statistics:
      self._stats_means, variances = (
          self._input_statistics.overall_feature_moments)
      self._stats_sigmas = tf.math.sqrt(variances)

  def _scale_data(self, data):
    """Scale data according to stats (input scale -> model scale)."""
    if self._input_statistics is not None:
      return (data - self._stats_means) / self._stats_sigmas
    else:
      return data

  def _scale_variance(self, variance):
    """Scale variances according to stats (input scale -> model scale)."""
    if self._input_statistics is not None:
      return variance / self._input_statistics.overall_feature_moments.variance
    else:
      return variance

  def _scale_back_data(self, data):
    """Scale back data according to stats (model scale -> input scale)."""
    if self._input_statistics is not None:
      return (data * self._stats_sigmas) + self._stats_means
    else:
      return data

  def _scale_back_variance(self, variance):
    """Scale back variances according to stats (model scale -> input scale)."""
    if self._input_statistics is not None:
      return variance * self._input_statistics.overall_feature_moments.variance
    else:
      return variance

  def _check_graph_initialized(self):
    if not self._graph_initialized:
      raise ValueError(
          "TimeSeriesModels require initialize_graph() to be called before "
          "use. This defines variables and ops in the default graph, and "
          "allows Tensor-valued input statistics to be specified.")

  def define_loss(self, features, mode):
    """Default loss definition with state replicated across a batch.

    Time series passed to this model have a batch dimension, and each series in
    a batch can be operated on in parallel. This loss definition assumes that
    each element of the batch represents an independent sample conditioned on
    the same initial state (i.e. it is simply replicated across the batch). A
    batch size of one provides sequential operations on a single time series.

    More complex processing may operate instead on get_start_state() and
    get_batch_loss() directly.

    Args:
      features: A dictionary (such as is produced by a chunker) with at minimum
        the following key/value pairs (others corresponding to the
        `exogenous_feature_columns` argument to `__init__` may be included
        representing exogenous regressors):
        TrainEvalFeatures.TIMES: A [batch size x window size] integer Tensor
          with times for each observation. If there is no artificial chunking,
          the window size is simply the length of the time series.
        TrainEvalFeatures.VALUES: A [batch size x window size x num features]
          Tensor with values for each observation.
      mode: The tf.estimator.ModeKeys mode to use (TRAIN, EVAL). For INFER, see
        predict().

    Returns:
      A ModelOutputs object.
    """
    self._check_graph_initialized()
    start_state = math_utils.replicate_state(
        start_state=self.get_start_state(),
        batch_size=tf.compat.v1.shape(features[TrainEvalFeatures.TIMES])[0])
    return self.get_batch_loss(features=features, mode=mode, state=start_state)

  # TODO(vitalyk,allenl): Better documentation surrounding options for chunking,
  # references to papers, etc.
  @abc.abstractmethod
  def get_start_state(self):
    """Returns a tuple of state for the start of the time series.

    For example, a mean and covariance. State should not have a batch
    dimension, and will often be TensorFlow Variables to be learned along with
    the rest of the model parameters.
    """
    pass

  @abc.abstractmethod
  def get_batch_loss(self, features, mode, state):
    """Return predictions, losses, and end state for a time series.

    Args:
      features: A dictionary with times, values, and (optionally) exogenous
        regressors. See `define_loss`.
      mode: The tf.estimator.ModeKeys mode to use (TRAIN, EVAL, INFER).
      state: Model-dependent state, each with size [batch size x ...]. The
        number and type will typically be fixed by the model (for example a mean
        and variance).

    Returns:
      A ModelOutputs object.
    """
    pass

  @abc.abstractmethod
  def predict(self, features):
    """Returns predictions of future observations given an initial state.

    Computes distributions for future observations. For sampled draws from the
    model where each is conditioned on the previous, see generate().

    Args:
      features: A dictionary with at minimum the following key/value pairs
        (others corresponding to the `exogenous_feature_columns` argument to
        `__init__` may be included representing exogenous regressors):
        PredictionFeatures.TIMES: A [batch size x window size] Tensor with times
          to make predictions for. Times must be increasing within each part of
          the batch, and must be greater than the last time `state` was updated.
        PredictionFeatures.STATE_TUPLE: Model-dependent state, each with size
          [batch size x ...]. The number and type will typically be fixed by the
          model (for example a mean and variance). Typically these will be the
          end state returned by get_batch_loss, predicting beyond that data.

    Returns:
      A dictionary with model-dependent predictions corresponding to the
      requested times. Keys indicate the type of prediction, and values have
      shape [batch size x window size x ...]. For example state space models
      return a "predicted_mean" and "predicted_covariance".
    """
    pass

  def _get_exogenous_embedding_shape(self):
    """Computes the shape of the vector returned by _process_exogenous_features.

    Returns:
      The shape as a list. Does not include a batch dimension.
    """
    if not self._exogenous_feature_columns:
      return (0,)
    with tf.Graph().as_default():
      parsed_features = (
          tf.compat.v1.feature_column.make_parse_example_spec(
              self._exogenous_feature_columns))
      placeholder_features = tf.compat.v1.io.parse_example(
          serialized=tf.compat.v1.placeholder(
              shape=[None], dtype=tf.dtypes.string),
          features=parsed_features)
      embedded = tf.compat.v1.feature_column.input_layer(
          features=placeholder_features,
          feature_columns=self._exogenous_feature_columns)
      return embedded.get_shape().as_list()[1:]

  def _process_exogenous_features(self, times, features):
    """Create a single vector from exogenous features.

    Args:
      times: A [batch size, window size] vector of times for this batch,
        primarily used to check the shape information of exogenous features.
      features: A dictionary of exogenous features corresponding to the columns
        in self._exogenous_feature_columns. Each value should have a shape
        prefixed by [batch size, window size].

    Returns:
      A Tensor with shape [batch size, window size, exogenous dimension], where
      the size of the exogenous dimension depends on the exogenous feature
      columns passed to the model's constructor.
    Raises:
      ValueError: If an exogenous feature has an unknown rank.
    """
    if self._exogenous_feature_columns:
      exogenous_features_single_batch_dimension = {}
      for name, tensor in features.items():
        if tensor.get_shape().ndims is None:
          # input_from_feature_columns does not support completely unknown
          # feature shapes, so we save on a bit of logic and provide a better
          # error message by checking that here.
          raise ValueError(
              ("Features with unknown rank are not supported. Got shape {} for "
               "feature {}.").format(tensor.get_shape(), name))
        tensor_shape_dynamic = tf.compat.v1.shape(tensor)
        tensor = tf.reshape(
            tensor,
            tf.concat([[tensor_shape_dynamic[0] * tensor_shape_dynamic[1]],
                       tensor_shape_dynamic[2:]],
                      axis=0))
        # Avoid shape warnings when embedding "scalar" exogenous features (those
        # with only batch and window dimensions); input_from_feature_columns
        # expects input ranks to match the embedded rank.
        if tensor.get_shape().ndims == 1 and tensor.dtype != tf.dtypes.string:
          exogenous_features_single_batch_dimension[name] = tensor[:, None]
        else:
          exogenous_features_single_batch_dimension[name] = tensor
      embedded_exogenous_features_single_batch_dimension = (
          tf.compat.v1.feature_column.input_layer(
              features=exogenous_features_single_batch_dimension,
              feature_columns=self._exogenous_feature_columns,
              trainable=True))
      exogenous_regressors = tf.reshape(
          embedded_exogenous_features_single_batch_dimension,
          tf.concat([
              tf.compat.v1.shape(times),
              tf.compat.v1.shape(
                  embedded_exogenous_features_single_batch_dimension)[1:]
          ],
                    axis=0))
      exogenous_regressors.set_shape(times.get_shape().concatenate(
          embedded_exogenous_features_single_batch_dimension.get_shape()[1:]))
      exogenous_regressors = tf.cast(exogenous_regressors, dtype=self.dtype)
    else:
      # Not having any exogenous features is a special case so that models can
      # avoid superfluous updates, which may not be free of side effects due to
      # bias terms in transformations.
      exogenous_regressors = None
    return exogenous_regressors
