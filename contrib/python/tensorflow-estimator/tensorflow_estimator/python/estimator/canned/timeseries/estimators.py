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
"""Estimators for time series models."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import tensorflow as tf
from tensorflow_estimator.python.estimator import estimator_lib
from tensorflow_estimator.python.estimator.canned import optimizers
from tensorflow_estimator.python.estimator.canned.timeseries import ar_model
from tensorflow_estimator.python.estimator.canned.timeseries import feature_keys
from tensorflow_estimator.python.estimator.canned.timeseries import head as ts_head_lib
from tensorflow_estimator.python.estimator.canned.timeseries import math_utils
from tensorflow_estimator.python.estimator.canned.timeseries import state_management
from tensorflow_estimator.python.estimator.export import export_lib


class TimeSeriesRegressor(estimator_lib.Estimator):
  """An Estimator to fit and evaluate a time series model."""

  def __init__(self,
               model,
               state_manager=None,
               optimizer=None,
               model_dir=None,
               config=None,
               head_type=ts_head_lib.TimeSeriesRegressionHead):
    """Initialize the Estimator.

    Args:
      model: The time series model to wrap (inheriting from TimeSeriesModel).
      state_manager: The state manager to use, or (by default)
        PassthroughStateManager if none is needed.
      optimizer: The optimization algorithm to use when training, inheriting
        from tf.train.Optimizer. Defaults to Adam with step size 0.02.
      model_dir: See `Estimator`.
      config: See `Estimator`.
      head_type: The kind of head to use for the model (inheriting from
        `TimeSeriesRegressionHead`).
    """
    input_statistics_generator = math_utils.InputStatisticsFromMiniBatch(
        dtype=model.dtype, num_features=model.num_features)
    if state_manager is None:
      if isinstance(model, ar_model.ARModel):
        state_manager = state_management.FilteringOnlyStateManager()
      else:
        state_manager = state_management.PassthroughStateManager()
    if optimizer is None:
      optimizer = tf.compat.v1.train.AdamOptimizer(0.02)
    self._model = model
    ts_regression_head = head_type(
        model=model,
        state_manager=state_manager,
        optimizer=optimizer,
        input_statistics_generator=input_statistics_generator)
    model_fn = ts_regression_head.create_estimator_spec
    super(TimeSeriesRegressor, self).__init__(
        model_fn=model_fn, model_dir=model_dir, config=config)

  def _model_start_state_placeholders(self,
                                      batch_size_tensor,
                                      static_batch_size=None):
    """Creates placeholders with zeroed start state for the current model."""
    gathered_state = {}
    # Models may not know the shape of their state without creating some
    # variables/ops. Avoid polluting the default graph by making a new one. We
    # use only static metadata from the returned Tensors.
    with tf.Graph().as_default():
      self._model.initialize_graph()

      # Evaluate the initial state as same-dtype "zero" values. These zero
      # constants aren't used, but are necessary for feeding to
      # placeholder_with_default for the "cold start" case where state is not
      # fed to the model.
      def _zeros_like_constant(tensor):
        return tf.get_static_value(tf.compat.v1.zeros_like(tensor))

      start_state = tf.nest.map_structure(_zeros_like_constant,
                                          self._model.get_start_state())
    for prefixed_state_name, state in ts_head_lib.state_to_dictionary(
        start_state).items():
      state_shape_with_batch = tf.TensorShape(
          (static_batch_size,)).concatenate(state.shape)
      default_state_broadcast = tf.tile(
          state[None, ...],
          multiples=tf.concat(
              [batch_size_tensor[None],
               tf.ones(len(state.shape), dtype=tf.dtypes.int32)],
              axis=0))
      gathered_state[
          prefixed_state_name] = tf.compat.v1.placeholder_with_default(
              input=default_state_broadcast,
              name=prefixed_state_name,
              shape=state_shape_with_batch)
    return gathered_state

  def build_one_shot_parsing_serving_input_receiver_fn(self,
                                                       filtering_length,
                                                       prediction_length,
                                                       default_batch_size=None,
                                                       values_input_dtype=None,
                                                       truncate_values=False):
    """Build an input_receiver_fn for export_saved_model accepting tf.Examples.

    Only compatible with `OneShotPredictionHead` (see `head`).

    Args:
      filtering_length: The number of time steps used as input to the model, for
        which values are provided. If more than `filtering_length` values are
        provided (via `truncate_values`), only the first `filtering_length`
        values are used.
      prediction_length: The number of time steps requested as predictions from
        the model. Times and all exogenous features must be provided for these
        steps.
      default_batch_size: If specified, must be a scalar integer. Sets the batch
        size in the static shape information of all feature Tensors, which means
        only this batch size will be accepted by the exported model. If None
        (default), static shape information for batch sizes is omitted.
      values_input_dtype: An optional dtype specification for values in the
        tf.Example protos (either float32 or int64, since these are the numeric
        types supported by tf.Example). After parsing, values are cast to the
        model's dtype (float32 or float64).
      truncate_values: If True, expects `filtering_length + prediction_length`
        values to be provided, but only uses the first `filtering_length`. If
        False (default), exactly `filtering_length` values must be provided.

    Returns:
      An input_receiver_fn which may be passed to the Estimator's
      export_saved_model.

      Expects features contained in a vector of serialized tf.Examples with
      shape [batch size] (dtype `tf.string`), each tf.Example containing
      features with the following shapes:
        times: [filtering_length + prediction_length] integer
        values: [filtering_length, num features] floating point. If
          `truncate_values` is True, expects `filtering_length +
          prediction_length` values but only uses the first `filtering_length`.
        all exogenous features: [filtering_length + prediction_length, ...]
          (various dtypes)
    """
    if values_input_dtype is None:
      values_input_dtype = tf.dtypes.float32
    if truncate_values:
      values_proto_length = filtering_length + prediction_length
    else:
      values_proto_length = filtering_length

    def _serving_input_receiver_fn():
      """A receiver function to be passed to export_saved_model."""
      times_column = tf.feature_column.numeric_column(
          key=feature_keys.TrainEvalFeatures.TIMES, dtype=tf.dtypes.int64)
      values_column = tf.feature_column.numeric_column(
          key=feature_keys.TrainEvalFeatures.VALUES,
          dtype=values_input_dtype,
          shape=(self._model.num_features,))
      parsed_features_no_sequence = (
          tf.compat.v1.feature_column.make_parse_example_spec(
              list(self._model.exogenous_feature_columns) +
              [times_column, values_column]))
      parsed_features = {}
      for key, feature_spec in parsed_features_no_sequence.items():
        if isinstance(feature_spec, tf.io.FixedLenFeature):
          if key == feature_keys.TrainEvalFeatures.VALUES:
            parsed_features[key] = feature_spec._replace(
                shape=((values_proto_length,) + feature_spec.shape))
          else:
            parsed_features[key] = feature_spec._replace(
                shape=((filtering_length + prediction_length,) +
                       feature_spec.shape))
        elif feature_spec.dtype == tf.dtypes.string:
          parsed_features[key] = tf.io.FixedLenFeature(
              shape=(filtering_length + prediction_length,),
              dtype=tf.dtypes.string)
        else:  # VarLenFeature
          raise ValueError("VarLenFeatures not supported, got %s for key %s" %
                           (feature_spec, key))
      tfexamples = tf.compat.v1.placeholder(
          shape=[default_batch_size], dtype=tf.dtypes.string, name="input")
      features = tf.compat.v1.io.parse_example(
          serialized=tfexamples, features=parsed_features)
      features[feature_keys.TrainEvalFeatures.TIMES] = tf.compat.v1.squeeze(
          features[feature_keys.TrainEvalFeatures.TIMES], axis=-1)
      features[feature_keys.TrainEvalFeatures.VALUES] = tf.cast(
          features[feature_keys.TrainEvalFeatures.VALUES],
          dtype=self._model.dtype)[:, :filtering_length]
      features.update(
          self._model_start_state_placeholders(
              batch_size_tensor=tf.compat.v1.shape(
                  features[feature_keys.TrainEvalFeatures.TIMES])[0],
              static_batch_size=default_batch_size))
      return export_lib.ServingInputReceiver(features, {"examples": tfexamples})

    return _serving_input_receiver_fn

  def build_raw_serving_input_receiver_fn(self,
                                          default_batch_size=None,
                                          default_series_length=None):
    """Build an input_receiver_fn for export_saved_model which accepts arrays.

    Automatically creates placeholders for exogenous `FeatureColumn`s passed to
    the model.

    Args:
      default_batch_size: If specified, must be a scalar integer. Sets the batch
        size in the static shape information of all feature Tensors, which means
        only this batch size will be accepted by the exported model. If None
        (default), static shape information for batch sizes is omitted.
      default_series_length: If specified, must be a scalar integer. Sets the
        series length in the static shape information of all feature Tensors,
        which means only this series length will be accepted by the exported
        model. If None (default), static shape information for series length is
        omitted.

    Returns:
      An input_receiver_fn which may be passed to the Estimator's
      export_saved_model.
    """

    def _serving_input_receiver_fn():
      """A receiver function to be passed to export_saved_model."""
      placeholders = {}
      time_placeholder = tf.compat.v1.placeholder(
          name=feature_keys.TrainEvalFeatures.TIMES,
          dtype=tf.dtypes.int64,
          shape=[default_batch_size, default_series_length])
      placeholders[feature_keys.TrainEvalFeatures.TIMES] = time_placeholder
      # Values are only necessary when filtering. For prediction the default
      # value will be ignored.
      placeholders[feature_keys.TrainEvalFeatures.VALUES] = (
          tf.compat.v1.placeholder_with_default(
              name=feature_keys.TrainEvalFeatures.VALUES,
              input=tf.zeros(
                  shape=[
                      default_batch_size if default_batch_size else 0,
                      default_series_length if default_series_length else 0,
                      self._model.num_features
                  ],
                  dtype=self._model.dtype),
              shape=(default_batch_size, default_series_length,
                     self._model.num_features)))
      if self._model.exogenous_feature_columns:
        with tf.Graph().as_default():
          # Default placeholders have only an unknown batch dimension. Make them
          # in a separate graph, then splice in the series length to the shapes
          # and re-create them in the outer graph.
          parsed_features = (
              tf.compat.v1.feature_column.make_parse_example_spec(
                  self._model.exogenous_feature_columns))
          placeholder_features = tf.compat.v1.io.parse_example(
              serialized=tf.compat.v1.placeholder(
                  shape=[None], dtype=tf.dtypes.string),
              features=parsed_features)
          exogenous_feature_shapes = {
              key: (value.get_shape(), value.dtype)
              for key, value in placeholder_features.items()
          }
        for feature_key, (batch_only_feature_shape,
                          value_dtype) in (exogenous_feature_shapes.items()):
          batch_only_feature_shape = (
              batch_only_feature_shape.with_rank_at_least(1).as_list())
          feature_shape = ([default_batch_size, default_series_length] +
                           batch_only_feature_shape[1:])
          placeholders[feature_key] = tf.compat.v1.placeholder(
              dtype=value_dtype, name=feature_key, shape=feature_shape)
      batch_size_tensor = tf.compat.v1.shape(time_placeholder)[0]
      placeholders.update(
          self._model_start_state_placeholders(
              batch_size_tensor, static_batch_size=default_batch_size))
      return export_lib.ServingInputReceiver(placeholders, placeholders)

    return _serving_input_receiver_fn


# TODO(b/113684821): Add detailed documentation on what the input_fn should do.
# Add an example of making and returning a Dataset object. Determine if
# endogenous features can be passed in as FeatureColumns. Move ARModel's loss
# functions into a more general location.
class LSTMAutoRegressor(TimeSeriesRegressor):
  """An Estimator for an LSTM autoregressive model.

  LSTMAutoRegressor is a window-based model, inputting fixed windows of length
  `input_window_size` and outputting fixed windows of length
  `output_window_size`. These two parameters must add up to the window_size
  of data returned by the `input_fn`.

  Each periodicity in the `periodicities` arg is divided by the `num_timesteps`
  into timesteps that are represented as time features added to the model.

  A good heuristic for picking an appropriate periodicity for a given data set
  would be the length of cycles in the data. For example, energy usage in a
  home is typically cyclic each day. If the time feature in a home energy
  usage dataset is in the unit of hours, then 24 would be an appropriate
  periodicity. Similarly, a good heuristic for `num_timesteps` is how often the
  data is expected to change within the cycle. For the aforementioned home
  energy usage dataset and periodicity of 24, then 48 would be a reasonable
  value if usage is expected to change every half hour.

  Each feature's value for a given example with time t is the difference
  between t and the start of the timestep it falls under. If it doesn't fall
  under a feature's associated timestep, then that feature's value is zero.

  For example: if `periodicities` = (9, 12) and `num_timesteps` = 3, then 6
  features would be added to the model, 3 for periodicity 9 and 3 for
  periodicity 12.

  For an example data point where t = 17:
  - It's in the 3rd timestep for periodicity 9 (2nd period is 9-18 and 3rd
    timestep is 15-18)
  - It's in the 2nd timestep for periodicity 12 (2nd period is 12-24 and
    2nd timestep is between 16-20).

  Therefore the 6 added features for this row with t = 17 would be:

  # Feature name (periodicity#_timestep#), feature value
  P9_T1, 0 # not in first timestep
  P9_T2, 0 # not in second timestep
  P9_T3, 2 # 17 - 15 since 15 is the start of the 3rd timestep
  P12_T1, 0 # not in first timestep
  P12_T2, 1 # 17 - 16 since 16 is the start of the 2nd timestep
  P12_T3, 0 # not in third timestep

  Example Code:

  ```python
  extra_feature_columns = (
      feature_column.numeric_column("exogenous_variable"),
  )

  estimator = LSTMAutoRegressor(
      periodicities=10,
      input_window_size=10,
      output_window_size=5,
      model_dir="/path/to/model/dir",
      num_features=1,
      extra_feature_columns=extra_feature_columns,
      num_timesteps=50,
      num_units=10,
      optimizer=tf.train.ProximalAdagradOptimizer(...))

  # Input builders
  def input_fn_train():
    return {
      "times": tf.range(15)[None, :],
      "values": tf.random_normal(shape=[1, 15, 1])
    }
  estimator.train(input_fn=input_fn_train, steps=100)

  def input_fn_eval():
    pass
  metrics = estimator.evaluate(input_fn=input_fn_eval, steps=10)

  def input_fn_predict():
    pass
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```
  """

  def __init__(self,
               periodicities,
               input_window_size,
               output_window_size,
               model_dir=None,
               num_features=1,
               extra_feature_columns=None,
               num_timesteps=10,
               loss=ar_model.ARModel.NORMAL_LIKELIHOOD_LOSS,
               num_units=128,
               optimizer="Adam",
               config=None):
    """Initialize the Estimator.

    Args:
      periodicities: periodicities of the input data, in the same units as the
        time feature (for example 24 if feeding hourly data with a daily
        periodicity, or 60 * 24 if feeding minute-level data with daily
        periodicity). Note this can be a single value or a list of values for
        multiple periodicities.
      input_window_size: Number of past time steps of data to look at when doing
        the regression.
      output_window_size: Number of future time steps to predict. Note that
        setting this value to > 1 empirically seems to give a better fit.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      num_features: The dimensionality of the time series (default value is one
        for univariate, more than one for multivariate).
      extra_feature_columns: A list of `tf.feature_column`s (for example
        `tf.feature_column.embedding_column`) corresponding to features which
        provide extra information to the model but are not part of the series to
        be predicted.
      num_timesteps: Number of buckets into which to divide (time %
        periodicity). This value multiplied by the number of periodicities is
        the number of time features added to the model.
      loss: Loss function to use for training. Currently supported values are
        SQUARED_LOSS and NORMAL_LIKELIHOOD_LOSS. Note that for
        NORMAL_LIKELIHOOD_LOSS, we train the covariance term as well. For
        SQUARED_LOSS, the evaluation loss is reported based on un-scaled
        observations and predictions, while the training loss is computed on
        normalized data.
      num_units: The size of the hidden state in the encoder and decoder LSTM
        cells.
      optimizer: string, `tf.train.Optimizer` object, or callable that defines
        the optimizer algorithm to use for training. Defaults to the Adam
        optimizer with a learning rate of 0.01.
      config: Optional `estimator.RunConfig` object to configure the runtime
        settings.
    """
    optimizer = optimizers.get_optimizer_instance(optimizer, learning_rate=0.01)
    model = ar_model.ARModel(
        periodicities=periodicities,
        input_window_size=input_window_size,
        output_window_size=output_window_size,
        num_features=num_features,
        exogenous_feature_columns=extra_feature_columns,
        num_time_buckets=num_timesteps,
        loss=loss,
        prediction_model_factory=functools.partial(
            ar_model.LSTMPredictionModel, num_units=num_units))
    state_manager = state_management.FilteringOnlyStateManager()
    super(LSTMAutoRegressor, self).__init__(
        model=model,
        state_manager=state_manager,
        optimizer=optimizer,
        model_dir=model_dir,
        config=config,
        head_type=ts_head_lib.OneShotPredictionHead)
