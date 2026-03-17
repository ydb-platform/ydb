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
"""Auto-Regressive models for time series data."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from tensorflow.python.framework import ops
from tensorflow.python.keras.engine import training
from tensorflow.python.keras.layers import core
from tensorflow.python.keras.layers import recurrent_v2
from tensorflow.python.ops import distributions
from tensorflow.python.ops import gen_math_ops
from tensorflow_estimator.python.estimator import estimator_lib
from tensorflow_estimator.python.estimator.canned.timeseries import model
from tensorflow_estimator.python.estimator.canned.timeseries import model_utils
from tensorflow_estimator.python.estimator.canned.timeseries.feature_keys import PredictionFeatures
from tensorflow_estimator.python.estimator.canned.timeseries.feature_keys import TrainEvalFeatures


class LSTMPredictionModel(training.Model):
  """A simple encoder/decoder model using an LSTM.

  This model does not operate on its own, but rather is a plugin to
  `ARModel`. See `ARModel`'s constructor documentation
  (`prediction_model_factory`) for a usage example.
  """

  def __init__(self,
               num_features,
               input_window_size,
               output_window_size,
               num_units=128):
    """Construct the LSTM prediction model.

    Args:
      num_features: number of input features per time step.
      input_window_size: Number of past time steps of data to look at when doing
        the regression.
      output_window_size: Number of future time steps to predict. Note that
        setting it to > 1 empirically seems to give a better fit.
      num_units: The number of units in the encoder and decoder LSTM cells.
    """
    super(LSTMPredictionModel, self).__init__()
    self._encoder = recurrent_v2.LSTM(
        num_units, name="encoder", dtype=self.dtype, return_state=True)
    self._decoder = recurrent_v2.LSTM(
        num_units, name="decoder", dtype=self.dtype, return_sequences=True)
    self._mean_transform = core.Dense(num_features, name="mean_transform")
    self._covariance_transform = core.Dense(
        num_features, name="covariance_transform")

  def call(self, input_window_features, output_window_features):
    """Compute predictions from input and output windows."""
    _, state_h, state_c = self._encoder(input_window_features)
    encoder_states = [state_h, state_c]
    decoder_output = self._decoder(
        output_window_features, initial_state=encoder_states)
    predicted_mean = self._mean_transform(decoder_output)
    predicted_covariance = gen_math_ops.exp(
        self._covariance_transform(decoder_output))
    return {"mean": predicted_mean, "covariance": predicted_covariance}


class ARModel(model.TimeSeriesModel):
  """Auto-regressive model, both linear and non-linear.

  Features to the model include time and values of input_window_size timesteps,
  and times for output_window_size timesteps. These are passed through a
  configurable prediction model, and then fed to a loss function (e.g. squared
  loss).

  Note that this class can also be used to regress against time only by setting
  the input_window_size to zero.

  Each periodicity in the `periodicities` arg is divided by the
  `num_time_buckets` into time buckets that are represented as features added
  to the model.

  A good heuristic for picking an appropriate periodicity for a given data set
  would be the length of cycles in the data. For example, energy usage in a
  home is typically cyclic each day. If the time feature in a home energy
  usage dataset is in the unit of hours, then 24 would be an appropriate
  periodicity. Similarly, a good heuristic for `num_time_buckets` is how often
  the data is expected to change within the cycle. For the aforementioned home
  energy usage dataset and periodicity of 24, then 48 would be a reasonable
  value if usage is expected to change every half hour.

  Each feature's value for a given example with time t is the difference
  between t and the start of the time bucket it falls under. If it doesn't fall
  under a feature's associated time bucket, then that feature's value is zero.

  For example: if `periodicities` = (9, 12) and `num_time_buckets` = 3, then 6
  features would be added to the model, 3 for periodicity 9 and 3 for
  periodicity 12.

  For an example data point where t = 17:
  - It's in the 3rd time bucket for periodicity 9 (2nd period is 9-18 and 3rd
    time bucket is 15-18)
  - It's in the 2nd time bucket for periodicity 12 (2nd period is 12-24 and
    2nd time bucket is between 16-20).

  Therefore the 6 added features for this row with t = 17 would be:

  # Feature name (periodicity#_timebucket#), feature value
  P9_T1, 0 # not in first time bucket
  P9_T2, 0 # not in second time bucket
  P9_T3, 2 # 17 - 15 since 15 is the start of the 3rd time bucket
  P12_T1, 0 # not in first time bucket
  P12_T2, 1 # 17 - 16 since 16 is the start of the 2nd time bucket
  P12_T3, 0 # not in third time bucket
  """
  SQUARED_LOSS = "squared_loss"
  NORMAL_LIKELIHOOD_LOSS = "normal_likelihood_loss"

  def __init__(self,
               periodicities,
               input_window_size,
               output_window_size,
               num_features,
               prediction_model_factory=LSTMPredictionModel,
               num_time_buckets=10,
               loss=NORMAL_LIKELIHOOD_LOSS,
               exogenous_feature_columns=None):
    """Constructs an auto-regressive model.

    Args:
      periodicities: periodicities of the input data, in the same units as the
        time feature (for example 24 if feeding hourly data with a daily
        periodicity, or 60 * 24 if feeding minute-level data with daily
        periodicity). Note this can be a single value or a list of values for
        multiple periodicities.
      input_window_size: Number of past time steps of data to look at when doing
        the regression.
      output_window_size: Number of future time steps to predict. Note that
        setting it to > 1 empirically seems to give a better fit.
      num_features: number of input features per time step.
      prediction_model_factory: A callable taking arguments `num_features`,
        `input_window_size`, and `output_window_size` and returning a
        `tf.keras.Model`. The `Model`'s `call()` takes two arguments: an input
        window and an output window, and returns a dictionary of predictions.
        See `LSTMPredictionModel` for an example. The default model computes
        predictions as a linear function of flattened input and output windows.
      num_time_buckets: Number of buckets into which to divide (time %
        periodicity). This value multiplied by the number of periodicities is
        the number of time features added to the model.
      loss: Loss function to use for training. Currently supported values are
        SQUARED_LOSS and NORMAL_LIKELIHOOD_LOSS. Note that for
        NORMAL_LIKELIHOOD_LOSS, we train the covariance term as well. For
        SQUARED_LOSS, the evaluation loss is reported based on un-scaled
        observations and predictions, while the training loss is computed on
        normalized data (if input statistics are available).
      exogenous_feature_columns: A list of `tf.feature_column`s (for example
        `tf.feature_column.embedding_column`) corresponding to features which
        provide extra information to the model but are not part of the series to
        be predicted.

    Example usage:

    >>> model = ar_model.ARModel(
    ...    periodicities=2, num_features=3,
    ...    prediction_model_factory=functools.partial(
    ...       LSTMPredictionModel, hidden_layer_sizes=[10, 10]))
    """
    self._model_factory = prediction_model_factory
    self.input_window_size = input_window_size
    self.output_window_size = output_window_size
    self.window_size = self.input_window_size + self.output_window_size
    self.loss = loss
    super(ARModel, self).__init__(
        num_features=num_features,
        exogenous_feature_columns=exogenous_feature_columns)
    if exogenous_feature_columns is not None:
      self.exogenous_size = self._get_exogenous_embedding_shape()[-1]
    else:
      self.exogenous_size = 0
    assert num_time_buckets > 0
    self._buckets = int(num_time_buckets)
    if periodicities is None or not periodicities:
      periodicities = []
    elif (not isinstance(periodicities, list) and
          not isinstance(periodicities, tuple)):
      periodicities = [periodicities]
    self._periodicities = [int(p) for p in periodicities]
    for p in self._periodicities:
      assert p > 0
    assert len(self._periodicities) or self.input_window_size
    assert output_window_size > 0

  def initialize_graph(self, input_statistics=None):
    super(ARModel, self).initialize_graph(input_statistics=input_statistics)
    self._model_scope = tf.compat.v1.variable_scope(
        # The trailing slash means we strip all enclosing variable_scopes, which
        # unfortunately is necessary because the model gets called inside and
        # outside a "while" scope (for prediction and training respectively),
        # and the variables names need to match.
        "model/",
        use_resource=True)
    self._model_instance = self._model_factory(
        num_features=self.num_features,
        input_window_size=self.input_window_size,
        output_window_size=self.output_window_size)

  def get_start_state(self):
    # State which matches the format we'll return later. Typically this will not
    # be used by the model directly, but the shapes and dtypes should match so
    # that the serving input_receiver_fn gets placeholder shapes correct.
    return (tf.zeros([self.input_window_size], dtype=tf.dtypes.int64),
            tf.zeros([self.input_window_size, self.num_features],
                     dtype=self.dtype),
            tf.zeros([self.input_window_size, self.exogenous_size],
                     dtype=self.dtype))

  # TODO(allenl,agarwal): Support sampling for AR.
  def random_model_parameters(self, seed=None):
    pass

  def generate(self,
               number_of_series,
               series_length,
               model_parameters=None,
               seed=None):
    pass

  def _predicted_covariance_op(self, activations, num_values):
    activation, activation_size = activations[-1]
    if self.loss == ARModel.NORMAL_LIKELIHOOD_LOSS:
      log_sigma_square = model_utils.fully_connected(
          activation,
          activation_size,
          self.output_window_size * num_values,
          name="log_sigma_square",
          activation=None)
      predicted_covariance = gen_math_ops.exp(log_sigma_square)
      predicted_covariance = tf.reshape(
          predicted_covariance, [-1, self.output_window_size, num_values])
    else:
      shape = tf.stack([
          tf.compat.v1.shape(activation)[0],
          tf.constant(self.output_window_size),
          tf.constant(num_values)
      ])
      predicted_covariance = tf.ones(shape=shape, dtype=activation.dtype)
    return predicted_covariance

  def _predicted_mean_op(self, activations):
    activation, activation_size = activations[-1]
    predicted_mean = model_utils.fully_connected(
        activation,
        activation_size,
        self.output_window_size * self.num_features,
        name="predicted_mean",
        activation=None)
    return tf.reshape(predicted_mean,
                      [-1, self.output_window_size, self.num_features])

  def prediction_ops(self, times, values, exogenous_regressors):
    """Compute model predictions given input data.

    Args:
      times: A [batch size, self.window_size] integer Tensor, the first
        self.input_window_size times in each part of the batch indicating input
        features, and the last self.output_window_size times indicating
        prediction times.
      values: A [batch size, self.input_window_size, self.num_features] Tensor
        with input features.
      exogenous_regressors: A [batch size, self.window_size,
        self.exogenous_size] Tensor with exogenous features.

    Returns:
      Tuple (predicted_mean, predicted_covariance), where each element is a
      Tensor with shape [batch size, self.output_window_size,
      self.num_features].
    """
    times.get_shape().assert_is_compatible_with([None, self.window_size])
    batch_size = tf.compat.v1.shape(times)[0]
    if self.input_window_size:
      values.get_shape().assert_is_compatible_with(
          [None, self.input_window_size, self.num_features])
    if exogenous_regressors is not None:
      exogenous_regressors.get_shape().assert_is_compatible_with(
          [None, self.window_size, self.exogenous_size])
    # Create input features.
    input_window_features = []
    input_feature_size = 0
    output_window_features = []
    output_feature_size = 0
    if self._periodicities:
      _, time_features = self._compute_time_features(times)
      num_time_features = self._buckets * len(self._periodicities)
      time_features = tf.reshape(
          time_features, [batch_size, self.window_size, num_time_features])
      input_time_features, output_time_features = tf.split(
          time_features, (self.input_window_size, self.output_window_size),
          axis=1)
      input_feature_size += num_time_features
      output_feature_size += num_time_features
      input_window_features.append(input_time_features)
      output_window_features.append(output_time_features)
    if self.input_window_size:
      inp = tf.slice(values, [0, 0, 0], [-1, self.input_window_size, -1])
      input_window_features.append(
          tf.reshape(inp,
                     [batch_size, self.input_window_size, self.num_features]))
      input_feature_size += self.num_features
    if self.exogenous_size:
      input_exogenous_features, output_exogenous_features = tf.split(
          exogenous_regressors,
          (self.input_window_size, self.output_window_size),
          axis=1)
      input_feature_size += self.exogenous_size
      output_feature_size += self.exogenous_size
      input_window_features.append(input_exogenous_features)
      output_window_features.append(output_exogenous_features)
    assert input_window_features
    input_window_features = tf.concat(input_window_features, axis=2)
    if output_window_features:
      output_window_features = tf.concat(output_window_features, axis=2)
    else:
      output_window_features = tf.zeros(
          [batch_size, self.output_window_size, 0], dtype=self.dtype)
    static_batch_size = times.get_shape().dims[0].value
    input_window_features.set_shape(
        [static_batch_size, self.input_window_size, input_feature_size])
    output_window_features.set_shape(
        [static_batch_size, self.output_window_size, output_feature_size])
    return self._output_window_predictions(input_window_features,
                                           output_window_features)

  def _output_window_predictions(self, input_window_features,
                                 output_window_features):
    with self._model_scope:
      predictions = self._model_instance(input_window_features,
                                         output_window_features)
      result_shape = [None, self.output_window_size, self.num_features]
      for v in predictions.values():
        v.set_shape(result_shape)
      return predictions

  def loss_op(self, targets, prediction_ops):
    """Create loss_op."""
    prediction = prediction_ops["mean"]
    if self.loss == ARModel.NORMAL_LIKELIHOOD_LOSS:
      covariance = prediction_ops["covariance"]
      sigma = tf.math.sqrt(tf.math.maximum(covariance, 1e-5))
      normal = distributions.normal.Normal(loc=targets, scale=sigma)
      loss_op = -tf.math.reduce_sum(normal.log_prob(prediction))
    else:
      assert self.loss == ARModel.SQUARED_LOSS, self.loss
      loss_op = tf.math.reduce_sum(tf.math.square(prediction - targets))
    loss_op /= tf.cast(
        tf.math.reduce_prod(tf.compat.v1.shape(targets)), loss_op.dtype)
    return loss_op

  def _process_exogenous_features(self, times, features):
    embedded = super(ARModel, self)._process_exogenous_features(
        times=times, features=features)
    if embedded is None:
      assert self.exogenous_size == 0
      # No embeddings. Return a zero-size [batch, times, 0] array so we don't
      # have to special case it downstream.
      return tf.zeros(
          tf.concat([tf.compat.v1.shape(times),
                     tf.constant([0])], axis=0))
    else:
      return embedded

  # TODO(allenl, agarwal): Consider better ways of warm-starting predictions.
  def predict(self, features):
    """Computes predictions multiple steps into the future.

    Args:
      features: A dictionary with the following key/value pairs:
        PredictionFeatures.TIMES: A [batch size, predict window size] integer
          Tensor of times, after the window of data indicated by `STATE_TUPLE`,
          to make predictions for.
        PredictionFeatures.STATE_TUPLE: A tuple of (times, values), times with
          shape [batch size, self.input_window_size], values with shape [batch
          size, self.input_window_size, self.num_features] representing a
          segment of the time series before `TIMES`. This data is used to start
          of the autoregressive computation. This should have data for at least
          self.input_window_size timesteps. And any exogenous features, with
          shapes prefixed by shape of `TIMES`.

    Returns:
      A dictionary with keys, "mean", "covariance". The
      values are Tensors of shape [batch_size, predict window size,
      num_features] and correspond to the values passed in `TIMES`.
    """
    if not self._graph_initialized:
      self.initialize_graph()
    predict_times = tf.cast(
        ops.convert_to_tensor(features[PredictionFeatures.TIMES]),
        tf.dtypes.int32)
    exogenous_regressors = self._process_exogenous_features(
        times=predict_times,
        features={
            key: value for key, value in features.items() if key not in [
                TrainEvalFeatures.TIMES, TrainEvalFeatures.VALUES,
                PredictionFeatures.STATE_TUPLE
            ]
        })
    with tf.control_dependencies([
        tf.compat.v1.debugging.assert_equal(
            tf.compat.v1.shape(predict_times)[1],
            tf.compat.v1.shape(exogenous_regressors)[1])
    ]):
      exogenous_regressors = tf.identity(exogenous_regressors)
    batch_size = tf.compat.v1.shape(predict_times)[0]
    num_predict_values = tf.compat.v1.shape(predict_times)[1]
    prediction_iterations = (
        (num_predict_values + self.output_window_size - 1) //
        self.output_window_size)
    # Pad predict_times and exogenous regressors so as to have exact multiple of
    # self.output_window_size values per example.
    padding_size = (
        prediction_iterations * self.output_window_size - num_predict_values)
    predict_times = tf.compat.v1.pad(predict_times, [[0, 0], [0, padding_size]])
    exogenous_regressors = tf.compat.v1.pad(exogenous_regressors,
                                            [[0, 0], [0, padding_size], [0, 0]])
    state = features[PredictionFeatures.STATE_TUPLE]
    (state_times, state_values, state_exogenous_regressors) = state
    state_times = tf.cast(ops.convert_to_tensor(state_times), tf.dtypes.int32)
    state_values = ops.convert_to_tensor(state_values, dtype=self.dtype)
    state_exogenous_regressors = ops.convert_to_tensor(
        state_exogenous_regressors, dtype=self.dtype)

    initial_input_times = predict_times[:, :self.output_window_size]
    initial_input_exogenous_regressors = (
        exogenous_regressors[:, :self.output_window_size, :])
    if self.input_window_size > 0:
      initial_input_times = tf.concat(
          [state_times[:, -self.input_window_size:], initial_input_times], 1)
      values_size = tf.compat.v1.shape(state_values)[1]
      times_size = tf.compat.v1.shape(state_times)[1]
      with tf.control_dependencies([
          tf.compat.v1.debugging.assert_greater_equal(values_size,
                                                      self.input_window_size),
          tf.compat.v1.debugging.assert_equal(values_size, times_size)
      ]):
        initial_input_values = state_values[:, -self.input_window_size:, :]
        initial_input_exogenous_regressors = tf.concat([
            state_exogenous_regressors[:, -self.input_window_size:, :],
            initial_input_exogenous_regressors[:, :self.output_window_size, :]
        ],
                                                       axis=1)
    else:
      initial_input_values = 0

    # Iterate over the predict_times, predicting self.output_window_size values
    # in each iteration.
    def _while_condition(iteration_number, *unused_args):
      return tf.math.less(iteration_number, prediction_iterations)

    def _while_body(iteration_number, input_times, input_values,
                    input_exogenous_regressors, mean_ta, covariance_ta):
      """Predict self.output_window_size values."""
      prediction_ops = self.prediction_ops(input_times, input_values,
                                           input_exogenous_regressors)
      predicted_mean = prediction_ops["mean"]
      predicted_covariance = prediction_ops["covariance"]
      offset = self.output_window_size * tf.math.minimum(
          iteration_number + 1, prediction_iterations - 1)
      if self.input_window_size > 0:
        if self.output_window_size < self.input_window_size:
          new_input_values = tf.concat(
              [input_values[:, self.output_window_size:, :], predicted_mean], 1)
          new_input_exogenous_regressors = tf.concat([
              input_exogenous_regressors[:, -self.input_window_size:, :],
              exogenous_regressors[
                  :, offset:offset + self.output_window_size, :]
          ], axis=1)
          new_input_times = tf.concat([
              input_times[:, -self.input_window_size:],
              predict_times[:, offset:offset + self.output_window_size]
          ], 1)
        else:
          new_input_values = predicted_mean[:, -self.input_window_size:, :]
          new_input_exogenous_regressors = exogenous_regressors[
              :,
              offset - self.input_window_size:offset + self.output_window_size,
              :]
          new_input_times = predict_times[
              :,
              offset - self.input_window_size:offset + self.output_window_size]
      else:
        new_input_values = input_values
        new_input_exogenous_regressors = exogenous_regressors[
            :, offset:offset + self.output_window_size, :]
        new_input_times = predict_times[:,
                                        offset:offset + self.output_window_size]
      new_input_times.set_shape(initial_input_times.get_shape())
      new_input_exogenous_regressors.set_shape(
          initial_input_exogenous_regressors.get_shape())
      new_mean_ta = mean_ta.write(iteration_number, predicted_mean)
      if isinstance(covariance_ta, tf.TensorArray):
        new_covariance_ta = covariance_ta.write(iteration_number,
                                                predicted_covariance)
      else:
        new_covariance_ta = covariance_ta
      return (iteration_number + 1, new_input_times, new_input_values,
              new_input_exogenous_regressors, new_mean_ta, new_covariance_ta)

    # Note that control_flow_ops.while_loop doesn't seem happy with None. Hence
    # using 0 for cases where we don't want to predict covariance.
    covariance_ta_init = (
        tf.TensorArray(dtype=self.dtype, size=prediction_iterations)
        if self.loss != ARModel.SQUARED_LOSS else 0.)
    mean_ta_init = tf.TensorArray(dtype=self.dtype, size=prediction_iterations)
    _, _, _, _, mean_ta, covariance_ta = tf.compat.v1.while_loop(
        _while_condition, _while_body, [
            0, initial_input_times, initial_input_values,
            initial_input_exogenous_regressors, mean_ta_init, covariance_ta_init
        ])

    def _parse_ta(values_ta):
      """Helper function to parse the returned TensorArrays."""

      if not isinstance(values_ta, tf.TensorArray):
        return None
      predictions_length = prediction_iterations * self.output_window_size
      # Shape [prediction_iterations, batch_size, self.output_window_size,
      #        self.num_features]
      values_packed = values_ta.stack()
      # Transpose to move batch dimension outside.
      output_values = tf.reshape(
          tf.compat.v1.transpose(values_packed, [1, 0, 2, 3]),
          tf.stack([batch_size, predictions_length, -1]))
      # Clip to desired size
      return output_values[:, :num_predict_values, :]

    predicted_mean = _parse_ta(mean_ta)
    predicted_covariance = _parse_ta(covariance_ta)
    if predicted_covariance is None:
      predicted_covariance = tf.compat.v1.ones_like(predicted_mean)

    # Transform and scale the mean and covariance appropriately.
    predicted_mean = self._scale_back_data(predicted_mean)
    predicted_covariance = self._scale_back_variance(predicted_covariance)

    return {"mean": predicted_mean, "covariance": predicted_covariance}

  def _process_window(self, features, mode, exogenous_regressors):
    """Compute model outputs on a single window of data."""
    times = tf.cast(features[TrainEvalFeatures.TIMES], tf.dtypes.int64)
    values = tf.cast(features[TrainEvalFeatures.VALUES], dtype=self.dtype)
    exogenous_regressors = tf.cast(exogenous_regressors, dtype=self.dtype)
    original_values = values

    # Extra shape checking for the window size (above that in
    # `head.create_estimator_spec`).
    expected_times_shape = [None, self.window_size]
    if not times.get_shape().is_compatible_with(expected_times_shape):
      raise ValueError(
          ("ARModel with input_window_size={input_window_size} "
           "and output_window_size={output_window_size} expects "
           "feature '{times_feature}' to have shape (batch_size, "
           "{window_size}) (for any batch_size), but got shape {times_shape}. "
           "If you are using RandomWindowInputFn, set "
           "window_size={window_size} or adjust the input_window_size and "
           "output_window_size arguments to ARModel.").format(
               input_window_size=self.input_window_size,
               output_window_size=self.output_window_size,
               times_feature=TrainEvalFeatures.TIMES,
               window_size=self.window_size,
               times_shape=times.get_shape()))
    values = self._scale_data(values)
    if self.input_window_size > 0:
      input_values = values[:, :self.input_window_size, :]
    else:
      input_values = None
    prediction_ops = self.prediction_ops(times, input_values,
                                         exogenous_regressors)
    prediction = prediction_ops["mean"]
    covariance = prediction_ops["covariance"]
    targets = tf.slice(values, [0, self.input_window_size, 0], [-1, -1, -1])
    targets.get_shape().assert_is_compatible_with(prediction.get_shape())
    if (mode == estimator_lib.ModeKeys.EVAL and
        self.loss == ARModel.SQUARED_LOSS):
      # Report an evaluation loss which matches the expected
      #  (observed - predicted) ** 2.
      # Note that this affects only evaluation; the training loss is unaffected.
      loss = self.loss_op(
          self._scale_back_data(targets),
          {"mean": self._scale_back_data(prediction_ops["mean"])})
    else:
      loss = self.loss_op(targets, prediction_ops)

    # Scale back the prediction.
    prediction = self._scale_back_data(prediction)
    covariance = self._scale_back_variance(covariance)

    return model.ModelOutputs(
        loss=loss,
        end_state=(times[:, -self.input_window_size:],
                   values[:, -self.input_window_size:, :],
                   exogenous_regressors[:, -self.input_window_size:, :]),
        predictions={
            "mean": prediction,
            "covariance": covariance,
            "observed": original_values[:, -self.output_window_size:]
        },
        prediction_times=times[:, -self.output_window_size:])

  def get_batch_loss(self, features, mode, state):
    """Computes predictions and a loss.

    Args:
      features: A dictionary (such as is produced by a chunker) with the
        following key/value pairs (shapes are given as required for training):
          TrainEvalFeatures.TIMES: A [batch size, self.window_size] integer
            Tensor with times for each observation. To train on longer
            sequences, the data should first be chunked.
          TrainEvalFeatures.VALUES: A [batch size, self.window_size,
            self.num_features] Tensor with values for each observation. When
            evaluating, `TIMES` and `VALUES` must have a window size of at least
            self.window_size, but it may be longer, in which case the last
            window_size - self.input_window_size times (or fewer if this is not
            divisible by self.output_window_size) will be evaluated on with
            non-overlapping output windows (and will have associated
            predictions). This is primarily to support qualitative
            evaluation/plotting, and is not a recommended way to compute
            evaluation losses (since there is no overlap in the output windows,
            which for window-based models is an undesirable bias).
      mode: The tf.estimator.ModeKeys mode to use (TRAIN or EVAL).
      state: Unused

    Returns:
      A model.ModelOutputs object.
    Raises:
      ValueError: If `mode` is not TRAIN or EVAL, or if static shape information
      is incorrect.
    """
    features = {
        feature_name: ops.convert_to_tensor(feature_value)
        for feature_name, feature_value in features.items()
    }
    times = features[TrainEvalFeatures.TIMES]
    exogenous_regressors = self._process_exogenous_features(
        times=times,
        features={
            key: value for key, value in features.items() if key not in [
                TrainEvalFeatures.TIMES, TrainEvalFeatures.VALUES,
                PredictionFeatures.STATE_TUPLE
            ]
        })
    if mode == estimator_lib.ModeKeys.TRAIN:
      # For training, we require the window size to be self.window_size as
      # iterating sequentially on larger windows could introduce a bias.
      return self._process_window(
          features, mode=mode, exogenous_regressors=exogenous_regressors)
    elif mode == estimator_lib.ModeKeys.EVAL:
      # For evaluation, we allow the user to pass in a larger window, in which
      # case we try to cover as much of the window as possible without
      # overlap. Quantitative evaluation is more efficient/correct with fixed
      # windows matching self.window_size (as with training), but this looping
      # allows easy plotting of "in-sample" predictions.
      times.get_shape().assert_has_rank(2)
      static_window_size = times.get_shape().dims[1].value
      if (static_window_size is not None and
          static_window_size < self.window_size):
        raise ValueError(
            ("ARModel requires a window of at least input_window_size + "
             "output_window_size to evaluate on (input_window_size={}, "
             "output_window_size={}, and got shape {} for feature '{}' (batch "
             "size, window size)).").format(self.input_window_size,
                                            self.output_window_size,
                                            times.get_shape(),
                                            TrainEvalFeatures.TIMES))
      num_iterations = (
          (tf.compat.v1.shape(times)[1] - self.input_window_size) //
          self.output_window_size)
      output_size = num_iterations * self.output_window_size
      # Rather than dealing with overlapping windows of output, discard a bit at
      # the beginning if output windows don't cover evenly.
      crop_length = output_size + self.input_window_size
      features = {
          feature_name: feature_value[:, -crop_length:]
          for feature_name, feature_value in features.items()
      }

      # Note that, unlike the ARModel's predict() while_loop, each iteration
      # here can run in parallel, since we are not feeding predictions or state
      # from previous iterations.
      def _while_condition(iteration_number, loss_ta, mean_ta, covariance_ta):
        del loss_ta, mean_ta, covariance_ta  # unused
        return iteration_number < num_iterations

      def _while_body(iteration_number, loss_ta, mean_ta, covariance_ta):
        """Perform a processing step on a single window of data."""
        base_offset = iteration_number * self.output_window_size
        model_outputs = self._process_window(
            features={
                feature_name:
                feature_value[:, base_offset:base_offset + self.window_size]
                for feature_name, feature_value in features.items()
            },
            mode=mode,
            exogenous_regressors=exogenous_regressors[:,
                                                      base_offset:base_offset +
                                                      self.window_size])
        # This code needs to be updated if new predictions are added in
        # self._process_window
        assert len(model_outputs.predictions) == 3
        assert "mean" in model_outputs.predictions
        assert "covariance" in model_outputs.predictions
        assert "observed" in model_outputs.predictions
        return (iteration_number + 1,
                loss_ta.write(iteration_number, model_outputs.loss),
                mean_ta.write(iteration_number,
                              model_outputs.predictions["mean"]),
                covariance_ta.write(iteration_number,
                                    model_outputs.predictions["covariance"]))

      _, loss_ta, mean_ta, covariance_ta = tf.compat.v1.while_loop(
          _while_condition, _while_body, [
              0,
              tf.TensorArray(dtype=self.dtype, size=num_iterations),
              tf.TensorArray(dtype=self.dtype, size=num_iterations),
              tf.TensorArray(dtype=self.dtype, size=num_iterations)
          ])
      values = tf.cast(features[TrainEvalFeatures.VALUES], dtype=self.dtype)
      batch_size = tf.compat.v1.shape(times)[0]
      prediction_shape = [
          batch_size, self.output_window_size * num_iterations,
          self.num_features
      ]
      (previous_state_times, previous_state_values,
       previous_state_exogenous_regressors) = state
      # Make sure returned state always has windows of self.input_window_size,
      # even if we were passed fewer than self.input_window_size points this
      # time.
      if self.input_window_size > 0:
        new_state_times = tf.concat(
            [previous_state_times,
             tf.cast(times, dtype=tf.dtypes.int64)],
            axis=1)[:, -self.input_window_size:]
        new_state_times.set_shape((None, self.input_window_size))
        new_state_values = tf.concat(
            [previous_state_values,
             self._scale_data(values)], axis=1)[:, -self.input_window_size:, :]
        new_state_values.set_shape(
            (None, self.input_window_size, self.num_features))
        new_exogenous_regressors = tf.concat(
            [previous_state_exogenous_regressors, exogenous_regressors],
            axis=1)[:, -self.input_window_size:, :]
        new_exogenous_regressors.set_shape(
            (None, self.input_window_size, self.exogenous_size))
      else:
        # There is no state to keep, and the strided slices above do not handle
        # input_window_size=0.
        new_state_times = previous_state_times
        new_state_values = previous_state_values
        new_exogenous_regressors = previous_state_exogenous_regressors
      return model.ModelOutputs(
          loss=tf.math.reduce_mean(loss_ta.stack(), axis=0),
          end_state=(new_state_times, new_state_values,
                     new_exogenous_regressors),
          predictions={
              "mean":
                  tf.reshape(
                      tf.compat.v1.transpose(mean_ta.stack(), [1, 0, 2, 3]),
                      prediction_shape),
              "covariance":
                  tf.reshape(
                      tf.compat.v1.transpose(covariance_ta.stack(),
                                             [1, 0, 2, 3]), prediction_shape),
              "observed":
                  values[:, -output_size:]
          },
          prediction_times=times[:, -output_size:])
    else:
      raise ValueError(
          "Unknown mode '{}' passed to get_batch_loss.".format(mode))

  def _compute_time_features(self, time):
    """Compute some features on the time value."""
    batch_size = tf.compat.v1.shape(time)[0]
    num_periods = len(self._periodicities)
    # Reshape to 3D.
    periods = tf.constant(
        self._periodicities, shape=[1, 1, num_periods, 1], dtype=time.dtype)
    time = tf.reshape(time, [batch_size, -1, 1, 1])
    window_offset = time / self._periodicities
    # Cast to appropriate type and scale to [0, 1) range
    mod = (
        tf.cast(time % periods, self.dtype) * self._buckets /
        tf.cast(periods, self.dtype))
    # Bucketize based on some fixed width intervals. For a value t and interval
    # [a, b), we return (t - a) if a <= t < b, else 0.
    intervals = tf.reshape(
        tf.range(self._buckets, dtype=self.dtype), [1, 1, 1, self._buckets])
    mod = tf.nn.relu(mod - intervals)
    mod = tf.where(mod < 1.0, mod, tf.compat.v1.zeros_like(mod))
    return window_offset, mod
