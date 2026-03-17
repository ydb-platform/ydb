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
"""Classes for wrapping a model to operate on different data shapes."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
from tensorflow_estimator.python.estimator import estimator_lib
from tensorflow_estimator.python.estimator.canned.timeseries import feature_keys


class PassthroughStateManager(object):
  """A minimal wrapper for models which do not need state management."""

  def __init__(self):
    self._input_statistics = None
    self._graph_initialized = False

  def initialize_graph(self, model, input_statistics=None):
    """Adds required operations to the graph."""
    del model  # unused
    self._graph_initialized = True
    self._input_statistics = input_statistics

  def define_loss(self, model, features, mode):
    """Wrap "model" with StateManager-specific operations.

    Args:
      model: The model (inheriting from TimeSeriesModel) to manage state for.
      features: A dictionary with the following key/value pairs:
        feature_keys.TrainEvalFeatures.TIMES: A [batch size x window size]
          Tensor with times for each observation.
        feature_keys.TrainEvalFeatures.VALUES: A [batch size x window size x num
          features] Tensor with values for each observation.
      mode: The tf.estimator.ModeKeys mode to use (TRAIN or EVAL).

    Returns:
      A ModelOutputs object.
    Raises:
      ValueError: If start state was specified.
    """
    if feature_keys.State.STATE_TUPLE in features:
      raise ValueError(
          "Overriding start state is not supported for this model.")
    return model.define_loss(features, mode)


class _OverridableStateManager(PassthroughStateManager):
  """Base class for state managers which support overriding model state."""

  @abc.abstractmethod
  def _define_loss_with_saved_state(self, model, features, mode):
    pass

  def define_loss(self, model, features, mode):
    """Switches between explicit start state and managed state."""
    if feature_keys.FilteringFeatures.STATE_TUPLE in features:
      # Explicit start state has been provided, so we should use that.
      if mode == estimator_lib.ModeKeys.TRAIN:
        raise ValueError(
            "Overriding saved state for training is not supported (but a value "
            "for feature {} was specified).".format(
                feature_keys.FilteringFeatures.STATE_TUPLE))
      start_state = features[feature_keys.FilteringFeatures.STATE_TUPLE]
      del features[feature_keys.FilteringFeatures.STATE_TUPLE]
      return model.get_batch_loss(
          features=features, mode=mode, state=start_state)
    else:
      # No explicit start state; use managed state.
      return self._define_loss_with_saved_state(
          model=model, features=features, mode=mode)


class FilteringOnlyStateManager(_OverridableStateManager):
  """State manager for models which use state only for filtering.

  Window-based models (ARModel) do not require state to be fed during training
  (instead requiring a specific window size). Rather than requiring a minimum
  window size for filtering, these models maintain this window in their state,
  and so need state to be fed.
  """

  def _define_loss_with_saved_state(self, model, features, mode):
    return model.define_loss(features, mode)
