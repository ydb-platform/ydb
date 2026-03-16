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
"""Miscellaneous utilities used by time series models."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import tensorflow as tf
from tensorflow.python.ops import gen_math_ops
from tensorflow_estimator.python.estimator.canned.timeseries.feature_keys import TrainEvalFeatures


def replicate_state(start_state, batch_size):
  """Create batch versions of state.

  Takes a list of Tensors, adds a batch dimension, and replicates
  batch_size times across that batch dimension. Used to replicate the
  non-batch state returned by get_start_state in define_loss.

  Args:
    start_state: Model-defined state to replicate.
    batch_size: Batch dimension for data.

  Returns:
    Replicated versions of the state.
  """
  flattened_state = tf.nest.flatten(start_state)
  replicated_state = [
      tf.tile(
          tf.compat.v1.expand_dims(state_nonbatch, 0),
          tf.concat([[batch_size],
                     tf.ones([tf.rank(state_nonbatch)], dtype=tf.dtypes.int32)],
                    0)) for state_nonbatch in flattened_state
  ]
  return tf.nest.pack_sequence_as(start_state, replicated_state)


Moments = collections.namedtuple("Moments", ["mean", "variance"])

# Currently all of these statistics are computed incrementally (i.e. are updated
# every time a new mini-batch of training data is presented) when this object is
# created in InputStatisticsFromMiniBatch.
InputStatistics = collections.namedtuple(
    "InputStatistics",
    [
        # The mean and variance of each feature in a chunk (with a size
        # configured in the statistics object) at the start of the series. A
        # tuple of (mean, variance), each with shape [number of features],
        # floating point. One use is in state space models, to keep priors
        # calibrated even as earlier parts of the series are presented. If this
        # object was created by InputStatisticsFromMiniBatch, these moments are
        # computed based on the earliest chunk of data presented so far.
        # However, there is a race condition in the update, so these may reflect
        # statistics later in the series, but should eventually reflect
        # statistics in a chunk at the series start.
        "series_start_moments",
        # The mean and variance of each feature over the entire series. A tuple
        # of (mean, variance), each with shape [number of features]. If this
        # object was created by InputStatisticsFromMiniBatch, these moments are
        # estimates based on the data seen so far.
        "overall_feature_moments",
        # The first (lowest) time in the series, a scalar integer. If this
        # object was created by InputStatisticsFromMiniBatch, this is the lowest
        # time seen so far rather than the lowest time that will ever be seen
        # (guaranteed to be at least as low as the lowest time presented in the
        # current minibatch).
        "start_time",
        # Count of data points, a scalar integer. If this object was created by
        # InputStatisticsFromMiniBatch, this is an estimate of the total number
        # of observations in the whole dataset computed based on the density of
        # the series and the minimum and maximum times seen.
        "total_observation_count",
    ])


# TODO(allenl): It would be nice to do something with full series statistics
# when the user provides that.
class InputStatisticsFromMiniBatch(object):
  """Generate statistics from mini-batch input."""

  def __init__(self, num_features, dtype, starting_variance_window_size=16):
    """Configure the input statistics object.

    Args:
      num_features: Number of features for the time series
      dtype: The floating point data type to use.
      starting_variance_window_size: The number of datapoints to use when
        computing the mean and variance at the start of the series.
    """
    self._starting_variance_window_size = starting_variance_window_size
    self._num_features = num_features
    self._dtype = dtype

  def initialize_graph(self, features, update_statistics=True):
    """Create any ops needed to provide input statistics.

    Should be called before statistics are requested.

    Args:
      features: A dictionary, the output of a `TimeSeriesInputFn` (with keys
        TrainEvalFeatures.TIMES and TrainEvalFeatures.VALUES).
      update_statistics: Whether `features` should be used to update adaptive
        statistics. Typically True for training and false for evaluation.

    Returns:
      An InputStatistics object composed of Variables, which will be updated
      based on mini-batches of data if requested.
    """
    if (TrainEvalFeatures.TIMES in features and
        TrainEvalFeatures.VALUES in features):
      times = features[TrainEvalFeatures.TIMES]
      values = features[TrainEvalFeatures.VALUES]
    else:
      # times and values may not be available, for example during prediction. We
      # still need to retrieve our variables so that they can be read from, even
      # if we're not going to update them.
      times = None
      values = None
    # Create/retrieve variables representing input statistics, initialized
    # without data to avoid deadlocking if variables are initialized before
    # queue runners are started.
    with tf.compat.v1.variable_scope("input_statistics", use_resource=True):
      statistics = self._create_variable_statistics_object()
    with tf.compat.v1.variable_scope(
        "input_statistics_auxiliary", use_resource=True):
      # Secondary statistics, necessary for the incremental computation of the
      # primary statistics (e.g. counts and sums for computing a mean
      # incrementally).
      auxiliary_variables = self._AdaptiveInputAuxiliaryStatistics(
          num_features=self._num_features, dtype=self._dtype)
    if update_statistics and times is not None and values is not None:
      # If we have times and values from mini-batch input, create update ops to
      # take the new data into account.
      assign_op = self._update_statistics_from_mini_batch(
          statistics, auxiliary_variables, times, values)
      with tf.control_dependencies([assign_op]):
        stat_variables = tf.nest.pack_sequence_as(
            statistics,
            [tf.identity(tensor) for tensor in tf.nest.flatten(statistics)])
        # Since start time updates have a race condition, ensure that the
        # reported start time is at least as low as the lowest time in this
        # mini-batch. The start time should converge on the correct value
        # eventually even with the race condition, but for example state space
        # models have an assertion which could fail without this
        # post-processing.
        min_time = tf.cast(tf.math.reduce_min(times), tf.dtypes.int64)
        start_time = tf.math.minimum(stat_variables.start_time, min_time)
        return stat_variables._replace(start_time=start_time)
    else:
      return statistics

  class _AdaptiveInputAuxiliaryStatistics(
      collections.namedtuple(
          "_AdaptiveInputAuxiliaryStatistics",
          [
              # The maximum time seen (best effort if updated from multiple
              # workers; see notes about race condition below).
              "max_time_seen",
              # The number of chunks seen.
              "chunk_count",
              # The sum across chunks of their "time density" (number of times
              # per example).
              "inter_observation_duration_sum",
              # The number of examples seen (each example has a single time
              # associated with it and one or more real-valued features).
              "example_count",
              # The sum of values for each feature. Shape [number of features].
              "overall_feature_sum",
              # The sum of squared values for each feature.
              # Shape [number of features].
              "overall_feature_sum_of_squares",
          ])):
    """Extra statistics used to incrementally update InputStatistics."""

    def __new__(cls, num_features, dtype):
      return super(
          InputStatisticsFromMiniBatch  # pylint: disable=protected-access
          ._AdaptiveInputAuxiliaryStatistics,
          cls).__new__(
              cls,
              max_time_seen=tf.compat.v1.get_variable(
                  name="max_time_seen",
                  initializer=tf.dtypes.int64.min,
                  dtype=tf.dtypes.int64,
                  trainable=False),
              chunk_count=tf.compat.v1.get_variable(
                  name="chunk_count",
                  initializer=tf.compat.v1.initializers.zeros(),
                  shape=[],
                  dtype=tf.dtypes.int64,
                  trainable=False),
              inter_observation_duration_sum=tf.compat.v1.get_variable(
                  name="inter_observation_duration_sum",
                  initializer=tf.compat.v1.initializers.zeros(),
                  shape=[],
                  dtype=dtype,
                  trainable=False),
              example_count=tf.compat.v1.get_variable(
                  name="example_count",
                  shape=[],
                  dtype=tf.dtypes.int64,
                  trainable=False),
              overall_feature_sum=tf.compat.v1.get_variable(
                  name="overall_feature_sum",
                  shape=[num_features],
                  dtype=dtype,
                  initializer=tf.compat.v1.initializers.zeros(),
                  trainable=False),
              overall_feature_sum_of_squares=tf.compat.v1.get_variable(
                  name="overall_feature_sum_of_squares",
                  shape=[num_features],
                  dtype=dtype,
                  initializer=tf.compat.v1.initializers.zeros(),
                  trainable=False))

  def _update_statistics_from_mini_batch(self, statistics, auxiliary_variables,
                                         times, values):
    """Given mini-batch input, update `statistics` and `auxiliary_variables`."""
    values = tf.cast(values, self._dtype)
    # The density (measured in times per observation) that we see in each part
    # of the mini-batch.
    batch_inter_observation_duration = (
        tf.cast(
            tf.math.reduce_max(times, axis=1) -
            tf.math.reduce_min(times, axis=1), self._dtype) /
        tf.cast(tf.compat.v1.shape(times)[1] - 1, self._dtype))
    # Co-locate updates with their variables to minimize race conditions when
    # updating statistics.
    with tf.compat.v1.device(auxiliary_variables.max_time_seen.device):
      # There is a race condition if this value is being updated from multiple
      # workers. However, it should eventually reach the correct value if the
      # last chunk is presented enough times.
      latest_time = tf.cast(tf.math.reduce_max(times), tf.dtypes.int64)
      max_time_seen = tf.math.maximum(auxiliary_variables.max_time_seen,
                                      latest_time)
      max_time_seen_assign = tf.compat.v1.assign(
          auxiliary_variables.max_time_seen, max_time_seen)
    with tf.compat.v1.device(auxiliary_variables.chunk_count.device):
      chunk_count_assign = tf.compat.v1.assign_add(
          auxiliary_variables.chunk_count,
          tf.compat.v1.shape(times, out_type=tf.dtypes.int64)[0])
    with tf.compat.v1.device(
        auxiliary_variables.inter_observation_duration_sum.device):
      inter_observation_duration_assign = tf.compat.v1.assign_add(
          auxiliary_variables.inter_observation_duration_sum,
          tf.math.reduce_sum(batch_inter_observation_duration))
    with tf.compat.v1.device(auxiliary_variables.example_count.device):
      example_count_assign = tf.compat.v1.assign_add(
          auxiliary_variables.example_count,
          tf.compat.v1.size(times, out_type=tf.dtypes.int64))
    # Note: These mean/variance updates assume that all points are equally
    # likely, which is not true if _chunks_ are sampled uniformly from the space
    # of all possible contiguous chunks, since points at the start and end of
    # the series are then members of fewer chunks. For series which are much
    # longer than the chunk size (the usual/expected case), this effect becomes
    # irrelevant.
    with tf.compat.v1.device(auxiliary_variables.overall_feature_sum.device):
      overall_feature_sum_assign = tf.compat.v1.assign_add(
          auxiliary_variables.overall_feature_sum,
          tf.math.reduce_sum(values, axis=[0, 1]))
    with tf.compat.v1.device(
        auxiliary_variables.overall_feature_sum_of_squares.device):
      overall_feature_sum_of_squares_assign = tf.compat.v1.assign_add(
          auxiliary_variables.overall_feature_sum_of_squares,
          tf.math.reduce_sum(values**2, axis=[0, 1]))
    per_chunk_aux_updates = tf.group(max_time_seen_assign, chunk_count_assign,
                                     inter_observation_duration_assign,
                                     example_count_assign,
                                     overall_feature_sum_assign,
                                     overall_feature_sum_of_squares_assign)
    with tf.control_dependencies([per_chunk_aux_updates]):
      example_count_float = tf.cast(auxiliary_variables.example_count,
                                    self._dtype)
      new_feature_mean = (
          auxiliary_variables.overall_feature_sum / example_count_float)
      overall_feature_mean_update = tf.compat.v1.assign(
          statistics.overall_feature_moments.mean, new_feature_mean)
      overall_feature_var_update = tf.compat.v1.assign(
          statistics.overall_feature_moments.variance,
          # De-biased n / (n - 1) variance correction
          example_count_float / (example_count_float - 1.) *
          (auxiliary_variables.overall_feature_sum_of_squares /
           example_count_float - new_feature_mean**2))
      # TODO(b/35675805): Remove this cast
      min_time_batch = tf.cast(
          tf.compat.v1.math.argmin(times[:, 0]), tf.dtypes.int32)

      def series_start_updates():
        # If this is the lowest-time chunk that we have seen so far, update
        # series start moments to reflect that. Note that these statistics are
        # "best effort", as there are race conditions in the update (however,
        # they should eventually converge if the start of the series is
        # presented enough times).
        mean, variance = tf.compat.v1.nn.moments(
            values[min_time_batch, :self._starting_variance_window_size],
            axes=[0])
        return tf.group(
            tf.compat.v1.assign(statistics.series_start_moments.mean, mean),
            tf.compat.v1.assign(statistics.series_start_moments.variance,
                                variance))

      with tf.compat.v1.device(statistics.start_time.device):
        series_start_update = tf.compat.v1.cond(
            # Update moments whenever we even match the lowest time seen so far,
            # to ensure that series start statistics are eventually updated to
            # their correct values, despite race conditions (i.e. eventually
            # statistics.start_time will reflect the global lowest time, and
            # given that we will eventually update the series start moments to
            # their correct values).
            tf.math.less_equal(times[min_time_batch, 0],
                               tf.cast(statistics.start_time, times.dtype)),
            series_start_updates,
            tf.no_op)
        with tf.control_dependencies([series_start_update]):
          # There is a race condition if this update is performed in parallel on
          # multiple workers. Since models may be sensitive to being presented
          # with times before the putative start time, the value of this
          # variable is post-processed above to guarantee that each worker is
          # presented with a start time which is at least as low as the lowest
          # time in its current mini-batch.
          min_time = tf.cast(tf.math.reduce_min(times), tf.dtypes.int64)
          start_time = tf.math.minimum(statistics.start_time, min_time)
          start_time_update = tf.compat.v1.assign(statistics.start_time,
                                                  start_time)
      inter_observation_duration_estimate = (
          auxiliary_variables.inter_observation_duration_sum /
          tf.cast(auxiliary_variables.chunk_count, self._dtype))
      # Estimate the total number of observations as:
      #   (end time - start time + 1) * average intra-chunk time density
      total_observation_count_update = tf.compat.v1.assign(
          statistics.total_observation_count,
          tf.cast(
              gen_math_ops.round(
                  tf.cast(max_time_seen_assign - start_time_update + 1,
                          self._dtype) / inter_observation_duration_estimate),
              tf.dtypes.int64))
      per_chunk_stat_updates = tf.group(overall_feature_mean_update,
                                        overall_feature_var_update,
                                        series_start_update, start_time_update,
                                        total_observation_count_update)
    return per_chunk_stat_updates

  def _create_variable_statistics_object(self):
    """Creates non-trainable variables representing input statistics."""
    series_start_moments = Moments(
        mean=tf.compat.v1.get_variable(
            name="series_start_mean",
            shape=[self._num_features],
            dtype=self._dtype,
            initializer=tf.compat.v1.initializers.zeros(),
            trainable=False),
        variance=tf.compat.v1.get_variable(
            name="series_start_variance",
            shape=[self._num_features],
            dtype=self._dtype,
            initializer=tf.compat.v1.initializers.ones(),
            trainable=False))
    overall_feature_moments = Moments(
        mean=tf.compat.v1.get_variable(
            name="overall_feature_mean",
            shape=[self._num_features],
            dtype=self._dtype,
            initializer=tf.compat.v1.initializers.zeros(),
            trainable=False),
        variance=tf.compat.v1.get_variable(
            name="overall_feature_var",
            shape=[self._num_features],
            dtype=self._dtype,
            initializer=tf.compat.v1.initializers.ones(),
            trainable=False))
    start_time = tf.compat.v1.get_variable(
        name="start_time",
        dtype=tf.dtypes.int64,
        initializer=tf.dtypes.int64.max,
        trainable=False)
    total_observation_count = tf.compat.v1.get_variable(
        name="total_observation_count",
        shape=[],
        dtype=tf.dtypes.int64,
        initializer=tf.compat.v1.initializers.ones(),
        trainable=False)
    return InputStatistics(
        series_start_moments=series_start_moments,
        overall_feature_moments=overall_feature_moments,
        start_time=start_time,
        total_observation_count=total_observation_count)
