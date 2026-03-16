# Copyright 2019 The TensorFlow Authors. All Rights Reserved.
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
"""Estimator classes for BoostedTrees."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import collections
import contextlib
import functools

import numpy as np
import six
import tensorflow as tf
from tensorflow.core.kernels.boosted_trees import boosted_trees_pb2
from tensorflow.python.feature_column import feature_column as fc_old
from tensorflow.python.feature_column import feature_column_lib
from tensorflow.python.feature_column import feature_column_v2
from tensorflow.python.framework import ops
from tensorflow.python.ops import boosted_trees_ops
from tensorflow.python.ops import cond_v2
from tensorflow.python.ops import lookup_ops
from tensorflow.python.ops.array_ops import identity as tf_identity
from tensorflow.python.ops.parallel_for import gradients as parallel_for_gradients
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator import estimator
from tensorflow_estimator.python.estimator.canned import boosted_trees_utils
from tensorflow_estimator.python.estimator.canned import head as head_lib
from tensorflow_estimator.python.estimator.mode_keys import ModeKeys

# TODO(nponomareva): Reveal pruning params here.
_TreeHParams = collections.namedtuple('TreeHParams', [
    'n_trees', 'max_depth', 'learning_rate', 'l1', 'l2', 'tree_complexity',
    'min_node_weight', 'center_bias', 'pruning_mode', 'quantile_sketch_epsilon'
])

_DUMMY_NUM_BUCKETS = -1
_DUMMY_NODE_ID = -1
_EQUALITY_SPLIT = 'equality'
_INEQUALITY_SPLIT = 'inequality'
_QUANTILE_ACCUMULATOR_RESOURCE_NAME = 'QuantileAccumulator'


def _is_numeric_column(feature_column):
  """Returns True if column is a continuous numeric that should be bucketized."""
  # These columns always produce categorical integers and do not require
  # additional bucketization.
  if isinstance(
      feature_column,
      (
          feature_column_lib.CategoricalColumn,
          fc_old._CategoricalColumn,  # pylint:disable=protected-access
          feature_column_lib.BucketizedColumn,
          fc_old._BucketizedColumn,  # pylint:disable=protected-access
          feature_column_lib.IndicatorColumn,
          fc_old._IndicatorColumn)):  # pylint:disable=protected-access
    return False
  # NumericColumns are always interpreted as continuous numerics.
  if isinstance(feature_column,
                (feature_column_lib.NumericColumn, fc_old._NumericColumn)):
    return True
  # For other dense columns, the dtype is used.
  if isinstance(feature_column,
                (feature_column_lib.DenseColumn, fc_old._DenseColumn)):
    # NOTE: GBDT requires that all DenseColumns expose a dtype attribute
    return feature_column.dtype.is_floating
  else:
    raise ValueError('Encountered unexpected column {}'.format(feature_column))


def _get_feature_dimensions(feature_ids_list, input_feature_list):
  """Get feature dimensions.

  Args:
    feature_ids_list: a list of lists of feature ids for each bucket size.
    input_feature_list: a list of input feature tensors.

  Returns:
    feature_dimensions: a list of dimensions for each feature.
  """
  # TODO(crawles): group feature dimensions by bucket (similar to feature_ids).
  feature_dimensions = []
  for feature_ids in feature_ids_list:
    for feature_id in feature_ids:
      feature_dimensions.append(input_feature_list[feature_id].shape[1])
  return feature_dimensions


def _get_float_feature_columns(sorted_feature_columns):
  """Get float feature columns.

  Args:
    sorted_feature_columns: a list of feature columns sorted by name.

  Returns:
    float_columns: a list of float feature columns sorted by name.
  """
  float_columns = []
  for feature_column in sorted_feature_columns:
    if _is_numeric_column(feature_column):
      float_columns.append(feature_column)
  return float_columns


def _apply_feature_transformations(features, feature_columns):
  """Applies feature column transformations to the provided features.

  Supports V1 and V2 FeatureColumns.

  Args:
    features: a dicionary of feature name to Tensor.
    feature_columns: an iterable of tf.feature_columns.

  Returns:
    A dict from feature_column to transformed feature tensor.
  """
  v2_columns, v1_columns = [], []
  for fc in feature_columns:
    if feature_column_lib.is_feature_column_v2([fc]):
      v2_columns.append(fc)
    else:
      v1_columns.append(fc)

  if v2_columns:
    state_manager = feature_column_v2._StateManagerImpl(
        layer=None, trainable=False)

    transformed_columns = feature_column_v2._transform_features_v2(
        features, v2_columns, state_manager)
  else:
    transformed_columns = {}
  if v1_columns:
    transformed_columns.update(fc_old._transform_features(features, v1_columns))
  return transformed_columns


def _get_transformed_features(
    features,
    sorted_feature_columns,
    bucket_boundaries_dict=None,
):
  """Gets the transformed features from features/feature_columns pair.

  Args:
    features: a dicionary of name to Tensor.
    sorted_feature_columns: a list/set of tf.feature_column, sorted by name.
    bucket_boundaries_dict: a dict of name to list of Tensors.

  Returns:
    result_features: a list of the transformed features, sorted by the name.

  Raises:
    ValueError: when unsupported features/columns are tried.
  """
  return _get_transformed_features_and_merge_with_previously_transformed(
      features, sorted_feature_columns, sorted_feature_columns,
      bucket_boundaries_dict)


def _get_transformed_features_and_merge_with_previously_transformed(
    features,
    sorted_feature_columns,
    all_sorted_columns,
    bucket_boundaries_dict=None,
    already_transformed_features={},
):
  """Gets the transformed features from features/feature_columns pair.

  This signature allows to pass in previously transformed features.

  Args:
    features: a dicionary of name to Tensor.
    sorted_feature_columns: a list/set of tf.feature_column, sorted by name, to
      be used for transforming features.
    all_sorted_columns: a total list of feature columns, including those that
      were already used for transformation.
    bucket_boundaries_dict: a dict of name to list of Tensors.
    already_transformed_features: features that were already transformed (for
      columns all_sorted_columns-sorted_feature_columns)

  Returns:
    result_features: a list of the transformed features, sorted by the name.

  Raises:
    ValueError: when unsupported features/columns are tried.
  """
  # pylint:disable=protected-access
  transformed_features = _apply_feature_transformations(features,
                                                        sorted_feature_columns)
  result_features = []

  if sorted_feature_columns != all_sorted_columns:
    # Add previously transformed features.
    transformed_features.update(already_transformed_features)

  for column in all_sorted_columns:
    if isinstance(
        column,
        (feature_column_lib.BucketizedColumn, fc_old._BucketizedColumn)):
      source_name = column.source_column.name
      bucketized_feature = transformed_features[column]
      if len(bucketized_feature.shape) > 2:
        raise ValueError('For now, only supports features equivalent to rank 2 '
                         'but column `{}` got: {}'.format(
                             source_name, features[source_name].shape))
      result_features.append(bucketized_feature)
    elif isinstance(
        column, (feature_column_lib.IndicatorColumn, fc_old._IndicatorColumn)):
      source_name = column.categorical_column.name
      tensor = tf.cast(transformed_features[column], dtype=tf.dtypes.int32)
      if len(tensor.shape) > 2:
        raise ValueError('Rank of indicator column must be no more than 2, '
                         'but column `{}` got: {}'.format(
                             source_name, features[source_name].shape))
      # TODO(nponomareva): consider treating as one, multi-dimensional feature
      # now that multi-dimensional features are supported.
      unstacked = [
          tf.compat.v1.expand_dims(t, axis=1)
          for t in tf.unstack(tensor, axis=1)
      ]
      result_features.extend(unstacked)
    elif isinstance(column,
                    (feature_column_lib.DenseColumn, fc_old._DenseColumn)):
      source_name = column.name
      tensor = transformed_features[column]
      # TODO(tanzheny): Add support for multi dim with rank > 1
      if _get_variable_shape(column).rank > 1:
        raise ValueError('For now, we only support Dense column with rank of '
                         '1, but column `{}` got: {}'.format(
                             source_name, column.variable_shape))
      # TODO(nponomareva): consider treating as one, multi-dimensional feature
      # now that multi-dimensional features are supported.
      if not bucket_boundaries_dict:
        unstacked = [
            tf.compat.v1.expand_dims(t, axis=1)
            for t in tf.unstack(tensor, axis=1)
        ]
        result_features.extend(unstacked)
      else:
        unstacked = tf.unstack(tensor, axis=1)
        assert source_name in bucket_boundaries_dict
        num_float_features = (
            _get_variable_shape(column)[0]
            if _get_variable_shape(column).as_list() else 1)
        assert num_float_features == len(bucket_boundaries_dict[source_name])
        bucketized = boosted_trees_ops.boosted_trees_bucketize(
            unstacked, bucket_boundaries_dict[source_name])
        expanded = [tf.compat.v1.expand_dims(t, axis=1) for t in bucketized]
        result_features.extend(expanded)
    elif isinstance(
        column,
        (feature_column_lib.CategoricalColumn, fc_old._CategoricalColumn)):
      # SparseTensor(indices=Tensor(..., shape= [?, 2]),
      # values=Tensor(..., shape=[?]), dense_shape=Tensor([batch_size, 1],...)))
      sparse_feature = transformed_features[column]
      dense = tf.cast(tf.sparse.to_dense(sparse_feature), tf.dtypes.int32)
      result_features.append(dense)
    else:
      raise ValueError('Got unexpected feature column type'.format(column))
    # pylint:enable=protected-access

  return result_features


def _variable(initial_value, trainable=False, name=None):
  """Stores a tensor as a local Variable for faster read."""
  if tf.compat.forward_compatible(2019, 8, 8):
    return tf.compat.v1.Variable(
        initial_value=initial_value,
        trainable=trainable,
        validate_shape=False,
        name=name,
        use_resource=True)
  return tf.compat.v1.Variable(
      initial_value=initial_value,
      trainable=trainable,
      validate_shape=False,
      name=name,
      use_resource=False)


def _group_features_by_num_buckets_and_split_type(sorted_feature_columns,
                                                  num_quantiles):
  """Group feature ids by number of buckets; determine when to equality split.

  Derives the feature ids based on iterating through ordered feature columns
  and groups them by the number of buckets each feature require. Returns a
  sorted list of buckets, a list of lists of feature ids for each of those
  buckets, and a list of lists containing split types for each feature.

  Args:
    sorted_feature_columns: a list/set of tf.feature_column sorted by name.
    num_quantiles: int representing the number of quantile buckets for all
      numeric columns.

  Returns:
    bucket_size_list: a list of required bucket sizes.
    feature_ids_list: a list of lists of feature ids for each bucket size.
    split_types_list: a list of lists indicating feature split type

  Raises:
    ValueError: when unsupported features columns are provided.
  """
  bucket_size_to_f_ids_and_splits = collections.OrderedDict()

  # TODO(nponomareva) for now we preserve the previous functionality and bucket
  # all numeric into the same num of buckets. Can be easily changed to using
  # each numeric's real buckets num, but we need to test that it does not cause
  # a performance hit.

  max_buckets_for_bucketized = 2
  max_buckets_for_indicator = 2
  # We will replace this dummy key with the real max after we calculate it.
  bucket_size_to_f_ids_and_splits.setdefault(_DUMMY_NUM_BUCKETS, [])

  feature_idx = 0
  # pylint:disable=protected-access
  for column in sorted_feature_columns:
    if isinstance(
        column, (feature_column_lib.IndicatorColumn, fc_old._IndicatorColumn)):
      num_categorical_features = column.categorical_column._num_buckets
      for _ in range(num_categorical_features):
        # We use bucket size of 2 for categorical.
        bucket_size_to_f_ids_and_splits.setdefault(max_buckets_for_indicator,
                                                   []).append(
                                                       (feature_idx,
                                                        _INEQUALITY_SPLIT))
        feature_idx += 1
    elif isinstance(
        column,
        (feature_column_lib.BucketizedColumn, fc_old._BucketizedColumn)):
      max_buckets_for_bucketized = max(max_buckets_for_bucketized,
                                       len(column.boundaries) + 1)
      bucket_size_to_f_ids_and_splits[_DUMMY_NUM_BUCKETS].append(
          (feature_idx, _INEQUALITY_SPLIT))
      feature_idx += 1
    elif isinstance(column,
                    (feature_column_lib.DenseColumn, fc_old._DenseColumn)):
      num_float_features = _get_variable_shape(
          column)[0] if _get_variable_shape(column).as_list() else 1
      for _ in range(num_float_features):
        bucket_size_to_f_ids_and_splits.setdefault(num_quantiles, []).append(
            (feature_idx, _INEQUALITY_SPLIT))
        feature_idx += 1
    elif isinstance(
        column,
        (feature_column_lib.CategoricalColumn, fc_old._CategoricalColumn)):
      bucket_size_to_f_ids_and_splits.setdefault(column.num_buckets, [])
      bucket_size_to_f_ids_and_splits[column.num_buckets].append(
          (feature_idx, _EQUALITY_SPLIT))
      feature_idx += 1
    else:
      raise ValueError('Got unexpected feature column type: {}'.format(column))

  # Replace the dummy key with the real max num of buckets for all bucketized
  # columns.
  bucketized_feature_ids = bucket_size_to_f_ids_and_splits[_DUMMY_NUM_BUCKETS]
  if max_buckets_for_bucketized in bucket_size_to_f_ids_and_splits:
    bucket_size_to_f_ids_and_splits[max_buckets_for_bucketized].extend(
        bucketized_feature_ids)
  elif bucketized_feature_ids:
    bucket_size_to_f_ids_and_splits[
        max_buckets_for_bucketized] = bucketized_feature_ids
  del bucket_size_to_f_ids_and_splits[_DUMMY_NUM_BUCKETS]

  # pylint:enable=protected-access
  bucket_size_list = []
  feature_ids_list = []
  split_types_list = []
  for bucket, f_ixs_and_splits in bucket_size_to_f_ids_and_splits.items():
    bucket_size_list.append(bucket)
    feature_ids, split_types = zip(*f_ixs_and_splits)
    feature_ids_list.append(feature_ids)
    split_types_list.append(split_types)
  return bucket_size_list, feature_ids_list, split_types_list


def _calculate_num_features(sorted_feature_columns):
  """Calculate the total number of features."""
  num_features = 0
  # pylint:disable=protected-access
  for column in sorted_feature_columns:
    if isinstance(
        column, (fc_old._IndicatorColumn, feature_column_lib.IndicatorColumn)):
      num_features += column.categorical_column._num_buckets
    elif isinstance(
        column,
        (fc_old._BucketizedColumn, feature_column_lib.BucketizedColumn)):
      num_features += 1
    elif isinstance(column,
                    (feature_column_lib.DenseColumn, fc_old._DenseColumn)):
      num_features += _get_variable_shape(column)[0] if _get_variable_shape(
          column).as_list() else 1
    elif isinstance(
        column,
        (feature_column_lib.CategoricalColumn, fc_old._CategoricalColumn)):
      num_features += 1
    else:
      raise ValueError('Got unexpected feature column type'.format(column))
  # pylint:enable=protected-access
  return num_features


def _generate_feature_col_name_mapping(sorted_feature_columns):
  """Return a list of feature column names for feature ids.

    Example:

    ```
    gender_col = indicator_column(
        categorical_column_with_vocabulary_list(
          'gender', ['male', 'female', 'n/a']))
    # Results in 3 binary features for which we store the mapping to the
    # original feature column.
    _generate_feature_col_name_mapping([gender_col])
    ['gender', 'gender', 'gender]
    ```

  Args:
    sorted_feature_columns: a list/set of tf.feature_column sorted by name.

  Returns:
    feature_col_name_mapping: a list of feature column names indexed by the
    feature ids.

  Raises:
    ValueError: when unsupported features/columns are tried.
  """
  # pylint:disable=protected-access
  names = []
  for column in sorted_feature_columns:
    if isinstance(
        column, (feature_column_lib.IndicatorColumn, fc_old._IndicatorColumn)):
      categorical_column = column.categorical_column
      if hasattr(categorical_column, 'num_buckets'):
        one_hot_depth = categorical_column.num_buckets
      else:
        assert hasattr(categorical_column, '_num_buckets')
        one_hot_depth = categorical_column._num_buckets
      for _ in range(one_hot_depth):
        names.append(categorical_column.name)
    elif isinstance(
        column,
        (feature_column_lib.BucketizedColumn, fc_old._BucketizedColumn)):
      names.append(column.name)
    elif isinstance(column,
                    (fc_old._DenseColumn, feature_column_lib.DenseColumn)):
      num_float_features = _get_variable_shape(
          column)[0] if _get_variable_shape(column).as_list() else 1
      for _ in range(num_float_features):
        names.append(column.name)
    elif isinstance(
        column,
        (feature_column_lib.CategoricalColumn, fc_old._CategoricalColumn)):
      names.append(column.name)
    else:
      raise ValueError('Got unexpected feature column type'.format(column))
  return names
  # pylint:enable=protected-access


def _cond(var, true_branch, false_branch, name=None):
  if tf.compat.forward_compatible(2019, 8, 8):
    # Always force to use cond v2 (even in v1 setting).
    return cond_v2.cond_v2(var, true_branch, false_branch, name=name)

  @contextlib.contextmanager
  def disable_control_flow_v2():
    control_flow_v2_enabled = tf.compat.v1.control_flow_v2_enabled()
    tf.compat.v1.disable_control_flow_v2()
    yield
    if control_flow_v2_enabled:
      tf.compat.v1.enable_control_flow_v2()

  with disable_control_flow_v2():
    return tf.compat.v1.cond(
        tf.math.logical_and(var, tf.constant(True)),
        true_branch,
        false_branch,
        name=name)


def _accumulator(dtype, shape, shared_name):
  return tf.compat.v1.ConditionalAccumulator(
      dtype=dtype, shape=shape, shared_name=shared_name)


def _cache_transformed_features(features, sorted_feature_columns, ind_columns,
                                other_columns, batch_size,
                                bucket_boundaries_dict, are_boundaries_ready):
  """Transform features and cache, then returns (cached_features, cache_op)."""
  num_features = _calculate_num_features(sorted_feature_columns)
  cached_features = []

  # TODO(crawles): figure out how to get feature dimension here. Right now it's
  # hard coded as 1.
  cached_features = [
      _variable(
          tf.zeros([batch_size, 1], dtype=tf.dtypes.int32),
          name='cached_feature_{}'.format(i)) for i in range(num_features)
  ]
  are_features_cached = _variable(False, name='are_features_cached')

  # An ugly hack - for indicator features, in order to have lookup tables
  # initialized, transform should happen outside of cond. So we always transform
  # ind columns separately (it is not as expensive as bucketizing) and then
  # merge these processed features with other columns in cond branches.
  ind_transformed = []
  if len(ind_columns) > 0:
    ind_transformed = _apply_feature_transformations(features, ind_columns)

  def get_features_without_cache():
    """Returns transformed features"""
    transformed_features = _get_transformed_features_and_merge_with_previously_transformed(
        features, other_columns, sorted_feature_columns, bucket_boundaries_dict,
        ind_transformed)

    return transformed_features, tf.no_op()

  def get_features_with_cache():
    """Either returns from cache or transforms and caches features."""

    def _cache_features_and_return():
      """Caches transformed features.

      The intention is to hide get_transformed_features() from the graph by
      caching the result except the first step, since bucketize operation
      (inside get_transformed_features) is expensive.

      Returns:
        input_feature_list: a list of input features.
        cache_flip_op: op to add to graph to make sure cache update is included
        to
            the graph.
      """
      transformed_features = _get_transformed_features_and_merge_with_previously_transformed(
          features, other_columns, sorted_feature_columns,
          bucket_boundaries_dict, ind_transformed)

      cached = [
          tf.compat.v1.assign(cached_features[i], transformed_features[i])
          for i in range(num_features)
      ]
      # TODO(youngheek): Try other combination of dependencies so that the
      # function returns a single result, not a tuple.
      with tf.control_dependencies(cached):
        cache_flip_op = are_features_cached.assign(True)
      return cached, cache_flip_op

    return _cond(are_features_cached, lambda: (cached_features, tf.no_op()),
                 _cache_features_and_return)

  input_feature_list, cache_flip_op = _cond(are_boundaries_ready,
                                            get_features_without_cache,
                                            get_features_with_cache)

  return input_feature_list, cache_flip_op


class _CacheTrainingStatesUsingHashTable(object):
  """Caching logits, etc. using MutableHashTable."""

  def __init__(self, example_ids, logits_dimension):
    """Creates a cache with the given configuration.

    It maintains a MutableDenseHashTable for all values.
    The API lookup() and insert() would have those specs,
      tree_ids: shape=[batch_size], dtype=int32
      node_ids: shape=[batch_size], dtype=int32
      logits: shape=[batch_size, logits_dimension], dtype=float32
    However in the MutableDenseHashTable, ids are bitcasted into float32 and
    all values are concatenated as a single tensor (of float32).

    Hence conversion happens internally before inserting to the HashTable and
    after lookup from it.

    Args:
      example_ids: a Rank 1 tensor to be used as a key of the cache.
      logits_dimension: a constant (int) for the dimension of logits.

    Raises:
      ValueError: if example_ids is other than int64 or string.
    """
    if tf.dtypes.as_dtype(tf.dtypes.int64).is_compatible_with(
        example_ids.dtype):
      empty_key = -1 << 62
      deleted_key = -1 << 61
    elif tf.dtypes.as_dtype(tf.dtypes.string).is_compatible_with(
        example_ids.dtype):
      empty_key = ''
      deleted_key = 'NEVER_USED_DELETED_KEY'
    else:
      raise ValueError('Unsupported example_id_feature dtype %s.' %
                       example_ids.dtype)
    # Cache holds latest <tree_id, node_id, logits> for each example.
    # tree_id and node_id are both int32 but logits is a float32.
    # To reduce the overhead, we store all of them together as float32 and
    # bitcast the ids to int32.
    self._table_ref = lookup_ops.mutable_dense_hash_table_v2(
        empty_key=empty_key,
        deleted_key=deleted_key,
        value_dtype=tf.dtypes.float32,
        value_shape=[3])
    self._example_ids = ops.convert_to_tensor(example_ids)
    if self._example_ids.shape.ndims not in (None, 1):
      raise ValueError('example_id should have rank 1, but got %s' %
                       self._example_ids)
    self._logits_dimension = logits_dimension

  def lookup(self):
    """Returns cached_tree_ids, cached_node_ids, cached_logits."""
    cached_tree_ids, cached_node_ids, cached_logits = tf.split(
        lookup_ops.lookup_table_find_v2(
            self._table_ref,
            self._example_ids,
            default_value=[0.0, _DUMMY_NODE_ID, 0.0]),
        [1, 1, self._logits_dimension],
        axis=1)
    cached_tree_ids = tf.compat.v1.squeeze(
        tf.bitcast(cached_tree_ids, tf.dtypes.int32))
    cached_node_ids = tf.compat.v1.squeeze(
        tf.bitcast(cached_node_ids, tf.dtypes.int32))
    if self._example_ids.shape.ndims is not None:
      cached_logits.set_shape(
          [self._example_ids.shape[0], self._logits_dimension])
    return (cached_tree_ids, cached_node_ids, cached_logits)

  def insert(self, tree_ids, node_ids, logits):
    """Inserts values and returns the op."""
    insert_op = lookup_ops.lookup_table_insert_v2(
        self._table_ref, self._example_ids,
        tf.concat([
            tf.compat.v1.expand_dims(
                tf.bitcast(tree_ids, tf.dtypes.float32), 1),
            tf.compat.v1.expand_dims(
                tf.bitcast(node_ids, tf.dtypes.float32), 1),
            logits,
        ],
                  axis=1,
                  name='value_concat_for_cache_insert'))
    return insert_op


class _CacheTrainingStatesUsingVariables(object):
  """Caching logits, etc. using Variables."""

  def __init__(self, batch_size, logits_dimension):
    """Creates a cache with the given configuration.

    It maintains three variables, tree_ids, node_ids, logits, for caching.
      tree_ids: shape=[batch_size], dtype=int32
      node_ids: shape=[batch_size], dtype=int32
      logits: shape=[batch_size, logits_dimension], dtype=float32

    Note, this can be used only with in-memory data setting.

    Args:
      batch_size: `int`, the size of the cache.
      logits_dimension: a constant (int) for the dimension of logits.
    """
    self._logits_dimension = logits_dimension
    self._tree_ids = _variable(
        tf.zeros([batch_size], dtype=tf.dtypes.int32), name='tree_ids_cache')
    self._node_ids = _variable(
        _DUMMY_NODE_ID * tf.ones([batch_size], dtype=tf.dtypes.int32),
        name='node_ids_cache')
    self._logits = _variable(
        tf.zeros([batch_size, logits_dimension], dtype=tf.dtypes.float32),
        name='logits_cache')

  def lookup(self):
    """Returns cached_tree_ids, cached_node_ids, cached_logits."""
    return (self._tree_ids, self._node_ids, self._logits)

  def insert(self, tree_ids, node_ids, logits):
    """Inserts values and returns the op."""
    return tf.group([
        self._tree_ids.assign(tree_ids),
        self._node_ids.assign(node_ids),
        self._logits.assign(logits)
    ],
                    name='cache_insert')


class _StopAtAttemptsHook(tf.compat.v1.train.SessionRunHook):
  """Hook that requests stop at the number of attempts."""

  def __init__(self, num_finalized_trees_tensor, num_attempted_layers_tensor,
               max_trees, max_depth):
    self._num_finalized_trees_tensor = num_finalized_trees_tensor
    self._num_attempted_layers_tensor = num_attempted_layers_tensor
    self._max_trees = max_trees
    self._max_depth = max_depth

  def before_run(self, run_context):
    return tf.compat.v1.train.SessionRunArgs(
        [self._num_finalized_trees_tensor, self._num_attempted_layers_tensor])

  def after_run(self, run_context, run_values):
    # num_* tensors should be retrieved by a separate session than the training
    # one, in order to read the values after growing.
    # So, if it's approaching to the limit, get the actual value by additional
    # session.
    num_finalized_trees, num_attempted_layers = run_values.results
    if (num_finalized_trees >= self._max_trees - 1 or
        num_attempted_layers > 2 * self._max_trees * self._max_depth - 1):
      num_finalized_trees, num_attempted_layers = run_context.session.run(
          [self._num_finalized_trees_tensor, self._num_attempted_layers_tensor])
    if (num_finalized_trees >= self._max_trees or
        num_attempted_layers > 2 * self._max_trees * self._max_depth):
      run_context.request_stop()


def _get_max_splits(tree_hparams):
  """Calculates the max possible number of splits based on tree params."""
  # maximum number of splits possible in the whole tree =2^(D-1)-1
  max_splits = (1 << tree_hparams.max_depth) - 1
  return max_splits


class _EnsembleGrower(object):
  """Abstract base class for different types of ensemble growers.

  Use it to receive training ops for growing and centering bias, depending
  on the implementation (for example, in memory or accumulator-based
  distributed):
    grower = ...create subclass grower(tree_ensemble, tree_hparams)
    grow_op = grower.grow_tree(stats_summaries_list, last_layer_nodes_range,
                               split_types_list)
    training_ops.append(grow_op)
  """

  def __init__(self, tree_ensemble, quantile_accumulator, tree_hparams,
               feature_ids_list, logits_dimension):
    """Initializes a grower object.

    Args:
      tree_ensemble: A TreeEnsemble variable.
      quantile_accumulator: A QuantileAccumulator variable.
      tree_hparams: TODO. collections.namedtuple for hyper parameters.
      feature_ids_list: a list of lists of feature ids for each bucket size.
      logits_dimension: a constant (int) for the dimension of logits.

    Raises:
      ValueError: when pruning mode is invalid or pruning is used and no tree
      complexity is set.
    """
    self._tree_ensemble = tree_ensemble
    self._tree_hparams = tree_hparams
    self._quantile_accumulator = quantile_accumulator
    self._feature_ids_list = feature_ids_list
    self._logits_dimension = logits_dimension
    # pylint: disable=protected-access
    self._pruning_mode_parsed = boosted_trees_ops.PruningMode.from_str(
        tree_hparams.pruning_mode)

    if tree_hparams.tree_complexity > 0:
      if self._pruning_mode_parsed == boosted_trees_ops.PruningMode.NO_PRUNING:
        raise ValueError(
            'Tree complexity have no effect unless pruning mode is chosen.')
    else:
      if self._pruning_mode_parsed != boosted_trees_ops.PruningMode.NO_PRUNING:
        raise ValueError('For pruning, tree_complexity must be positive.')
    # pylint: enable=protected-access

  @abc.abstractmethod
  def accumulate_quantiles(self, float_features, weights, are_boundaries_ready):
    """Accumulate quantile information for float features.

    Args:
      float_features: float features.
      weights: weights Tensor.
      are_boundaries_ready: bool variable.

    Returns:
      An operation for accumulate quantile.
    """

  @abc.abstractmethod
  def center_bias(self, center_bias_var, gradients, hessians):
    """Centers bias, if ready, based on statistics.

    Args:
      center_bias_var: A variable that will be updated when bias centering
        finished.
      gradients: A rank 2 tensor of gradients.
      hessians: A rank 2 tensor of hessians.

    Returns:
      An operation for centering bias.
    """

  @abc.abstractmethod
  def grow_tree(self, stats_summaries_list, last_layer_nodes_range,
                split_types_list):
    """Grows a tree, if ready, based on provided statistics.

    Args:
      stats_summaries_list: List of stats summary tensors, representing sums of
        gradients and hessians for each feature bucket.
      last_layer_nodes_range: A tensor representing ids of the nodes in the
        current layer, to be split.
      split_types_list: a list of lists indicating feature split type

    Returns:
      An op for growing a tree.
    """

  def chief_init_op(self):
    """Ops that chief needs to run to initialize the state."""
    return tf.no_op()

  #  ============= Helper methods ===========

  def _center_bias_fn(self, center_bias_var, mean_gradients, mean_hessians):
    """Updates the ensembles and cache (if needed) with logits prior."""
    continue_centering = boosted_trees_ops.center_bias(
        self._tree_ensemble.resource_handle,
        mean_gradients=mean_gradients,
        mean_hessians=mean_hessians,
        l1=self._tree_hparams.l1,
        l2=self._tree_hparams.l2)
    return center_bias_var.assign(continue_centering)

  def _grow_tree_from_stats_summaries(self, stats_summaries_list,
                                      last_layer_nodes_range, split_types_list):
    """Updates ensemble based on the best gains from stats summaries."""
    assert (len(stats_summaries_list) == len(self._feature_ids_list) ==
            len(split_types_list))

    # These lists will be of size num_buckets. Each element will be a list.
    node_ids_list = []
    gains_list = []
    best_feature_ids_list = []
    best_feature_dimensions_list = []
    best_feature_splits_list = []
    thresholds_list = []
    left_node_contribs_list = []
    right_node_contribs_list = []
    for bucket_ix in range(len(self._feature_ids_list)):
      (node_ids, gains, best_feature_ids, best_feature_dimensions, thresholds,
       left_node_contribs, right_node_contribs, best_feature_split_types) = (
           boosted_trees_ops.calculate_best_feature_split_v2(
               last_layer_nodes_range,
               stats_summaries_list[bucket_ix],
               candidate_feature_ids=self._feature_ids_list[bucket_ix],
               split_types=split_types_list[bucket_ix],
               l1=self._tree_hparams.l1,
               l2=self._tree_hparams.l2,
               tree_complexity=self._tree_hparams.tree_complexity,
               min_node_weight=self._tree_hparams.min_node_weight,
               logits_dimension=self._logits_dimension))
      node_ids_list.append(node_ids)
      gains_list.append(gains)
      best_feature_ids_list.append(best_feature_ids)
      best_feature_dimensions_list.append(best_feature_dimensions)
      best_feature_splits_list.append(best_feature_split_types)
      thresholds_list.append(thresholds)
      left_node_contribs_list.append(left_node_contribs)
      right_node_contribs_list.append(right_node_contribs)
    grow_op = boosted_trees_ops.update_ensemble_v2(
        # Confirm if local_tree_ensemble or tree_ensemble should be used.
        self._tree_ensemble.resource_handle,
        feature_ids=best_feature_ids_list,
        dimension_ids=best_feature_dimensions_list,
        node_ids=node_ids_list,
        gains=gains_list,
        thresholds=thresholds_list,
        left_node_contribs=left_node_contribs_list,
        right_node_contribs=right_node_contribs_list,
        split_types=best_feature_splits_list,
        learning_rate=self._tree_hparams.learning_rate,
        max_depth=self._tree_hparams.max_depth,
        pruning_mode=self._pruning_mode_parsed,
        logits_dimension=self._logits_dimension)
    return grow_op


class _InMemoryEnsembleGrower(_EnsembleGrower):
  """An in-memory ensemble grower."""

  def __init__(self, tree_ensemble, quantile_accumulator, tree_hparams,
               feature_ids_list, logits_dimension):

    super(_InMemoryEnsembleGrower, self).__init__(
        tree_ensemble=tree_ensemble,
        quantile_accumulator=quantile_accumulator,
        tree_hparams=tree_hparams,
        feature_ids_list=feature_ids_list,
        logits_dimension=logits_dimension)

  def accumulate_quantiles(self, float_features, weights, are_boundaries_ready):
    summary_op = self._quantile_accumulator.add_summaries(
        float_features, weights)
    with tf.control_dependencies([summary_op]):
      flush = self._quantile_accumulator.flush()
      with tf.control_dependencies([flush]):
        return are_boundaries_ready.assign(True).op

  def center_bias(self, center_bias_var, gradients, hessians):
    # For in memory, we already have a full batch of gradients and hessians,
    # so just take a mean and proceed with centering.
    mean_gradients = tf.compat.v1.expand_dims(
        tf.math.reduce_mean(gradients, 0), 0)
    mean_heassians = tf.compat.v1.expand_dims(
        tf.math.reduce_mean(hessians, 0), 0)
    return self._center_bias_fn(center_bias_var, mean_gradients, mean_heassians)

  def grow_tree(self, stats_summaries_list, last_layer_nodes_range,
                split_types_list):
    # For in memory, we already have full data in one batch, so we can grow the
    # tree immediately.
    return self._grow_tree_from_stats_summaries(stats_summaries_list,
                                                last_layer_nodes_range,
                                                split_types_list)


class _AccumulatorEnsembleGrower(_EnsembleGrower):
  """An accumulator based ensemble grower."""

  def __init__(self, tree_ensemble, quantile_accumulator, tree_hparams,
               stamp_token, n_batches_per_layer, bucket_size_list, is_chief,
               center_bias, feature_ids_list, logits_dimension,
               feature_dimensions):
    super(_AccumulatorEnsembleGrower, self).__init__(
        tree_ensemble=tree_ensemble,
        quantile_accumulator=quantile_accumulator,
        tree_hparams=tree_hparams,
        feature_ids_list=feature_ids_list,
        logits_dimension=logits_dimension)
    self._stamp_token = stamp_token
    self._n_batches_per_layer = n_batches_per_layer
    self._bucket_size_list = bucket_size_list
    self._is_chief = is_chief
    self._feature_dimensions = feature_dimensions
    self._growing_accumulators = []
    self._chief_init_ops = []
    max_splits = _get_max_splits(self._tree_hparams)
    i = 0
    # TODO(crawles): Allow to create an accumulator per feature, instead of
    # accumulator per bucket_size.
    for bucket_size, feature_ids in zip(self._bucket_size_list,
                                        self._feature_ids_list):
      grad_size = logits_dimension
      hessian_size = logits_dimension * logits_dimension
      accumulator = _accumulator(
          dtype=tf.dtypes.float32,
          # The stats consist of grads and hessians (the last dimension).
          shape=[
              len(feature_ids),
              max_splits,
              1,  # TODO(crawles): Support multi_dim features.
              bucket_size + 1,  # +1 for missing/default bucket.
              grad_size + hessian_size
          ],
          shared_name='numeric_stats_summary_accumulator_' + str(i))
      i += 1
      self._chief_init_ops.append(
          accumulator.set_global_step(self._stamp_token))
      self._growing_accumulators.append(accumulator)
    self._center_bias = center_bias
    if center_bias:
      self._bias_accumulator = _accumulator(
          dtype=tf.dtypes.float32,
          # The stats consist of grads and hessians means only.
          # TODO(nponomareva): this will change for a multiclass
          shape=[2, 1],
          shared_name='bias_accumulator')
      self._chief_init_ops.append(
          self._bias_accumulator.set_global_step(self._stamp_token))

  def accumulate_quantiles(self, float_features, weights, are_boundaries_ready):
    summary_op = self._quantile_accumulator.add_summaries(
        float_features, weights)
    cond_accum = _accumulator(
        dtype=tf.dtypes.float32, shape={}, shared_name='quantile_summary_accum')
    cond_accum_step = cond_accum.set_global_step(self._stamp_token)
    apply_grad = cond_accum.apply_grad(tf.constant(0.), self._stamp_token)
    update_quantile_op = tf.group(summary_op, cond_accum_step, apply_grad)
    if not self._is_chief:
      return update_quantile_op

    with tf.control_dependencies([update_quantile_op]):

      def flush_fn():
        grad = cond_accum.take_grad(1)
        flush_op = self._quantile_accumulator.flush()
        boundaries_ready_op = are_boundaries_ready.assign(True).op
        return tf.group(flush_op, grad, boundaries_ready_op)

      finalize_quantile_op = _cond(
          tf.math.greater_equal(cond_accum.num_accumulated(),
                                self._n_batches_per_layer),
          flush_fn,
          tf.no_op,
          name='wait_until_quaniles_accumulated')
    return finalize_quantile_op

  def center_bias(self, center_bias_var, gradients, hessians):
    # For not in memory situation, we need to accumulate enough of batches first
    # before proceeding with centering bias.

    # Create an accumulator.
    if not self._center_bias:
      raise RuntimeError('center_bias called but bias centering is disabled.')
    bias_dependencies = []
    grads_and_hess = tf.stack([gradients, hessians], axis=0)
    grads_and_hess = tf.math.reduce_mean(grads_and_hess, axis=1)

    apply_grad = self._bias_accumulator.apply_grad(grads_and_hess,
                                                   self._stamp_token)
    bias_dependencies.append(apply_grad)

    # Center bias if enough batches were processed.
    with tf.control_dependencies(bias_dependencies):
      if not self._is_chief:
        return tf.no_op()

      def _set_accumulators_stamp():
        return tf.group([
            acc.set_global_step(self._stamp_token + 1)
            for acc in self._growing_accumulators
        ])

      def center_bias_from_accumulator():
        accumulated = tf.unstack(self._bias_accumulator.take_grad(1), axis=0)
        center_bias_op = self._center_bias_fn(
            center_bias_var, tf.compat.v1.expand_dims(accumulated[0], 0),
            tf.compat.v1.expand_dims(accumulated[1], 0))
        with tf.control_dependencies([center_bias_op]):
          return _cond(center_bias_var, tf.no_op, _set_accumulators_stamp)

      center_bias_op = _cond(
          tf.math.greater_equal(self._bias_accumulator.num_accumulated(),
                                self._n_batches_per_layer),
          center_bias_from_accumulator,
          tf.no_op,
          name='wait_until_n_batches_for_bias_accumulated')
      return center_bias_op

  def grow_tree(self, stats_summaries_list, last_layer_nodes_range,
                split_types_list):
    dependencies = []
    for i in range(len(self._feature_ids_list)):
      stats_summaries = stats_summaries_list[i]
      apply_grad = self._growing_accumulators[i].apply_grad(
          tf.stack(stats_summaries, axis=0), self._stamp_token)
      dependencies.append(apply_grad)

    # Grow the tree if enough batches is accumulated.
    with tf.control_dependencies(dependencies):
      if not self._is_chief:
        return tf.no_op()

      min_accumulated = tf.math.reduce_min(
          tf.stack(
              [acc.num_accumulated() for acc in self._growing_accumulators]))

      def grow_tree_from_accumulated_summaries_fn():
        """Updates tree with the best layer from accumulated summaries."""
        # Take out the accumulated summaries from the accumulator and grow.
        accumulated_summaries_list = [
            tf.unstack(accumulator.take_grad(1), axis=0)
            for accumulator in self._growing_accumulators
        ]
        grow_op = self._grow_tree_from_stats_summaries(
            accumulated_summaries_list, last_layer_nodes_range,
            split_types_list)
        return grow_op

      grow_model = _cond(
          tf.math.greater_equal(min_accumulated, self._n_batches_per_layer),
          grow_tree_from_accumulated_summaries_fn,
          tf.no_op,
          name='wait_until_n_batches_accumulated')
      return grow_model

  def chief_init_op(self):
    """Ops that chief needs to run to initialize the state."""
    return tf.group(self._chief_init_ops)


def _bt_model_fn(features,
                 labels,
                 mode,
                 head,
                 feature_columns,
                 tree_hparams,
                 n_batches_per_layer,
                 config,
                 closed_form_grad_and_hess_fn=None,
                 example_id_column_name=None,
                 weight_column=None,
                 train_in_memory=False,
                 name='boosted_trees'):
  """Gradient Boosted Trees model_fn.

  Args:
    features: dict of `Tensor`.
    labels: `Tensor` of shape [batch_size, 1] or [batch_size] labels of dtype
      `int32` or `int64` in the range `[0, n_classes)`.
    mode: Defines whether this is training, evaluation or prediction. See
      `ModeKeys`.
    head: A `head_lib._Head` instance.
    feature_columns: Iterable of `fc_old._FeatureColumn` model inputs.
    tree_hparams: TODO. collections.namedtuple for hyper parameters.
    n_batches_per_layer: A `Tensor` of `int64`. Each layer is built after at
      least n_batches_per_layer accumulations.
    config: `RunConfig` object to configure the runtime settings.
    closed_form_grad_and_hess_fn: a function that accepts logits and labels and
      returns gradients and hessians. By default, they are created by
      tf.gradients() from the loss.
    example_id_column_name: Name of the feature for a unique ID per example.
      Currently experimental -- not exposed to public API.
    weight_column: A string or a `_NumericColumn` created by
      `tf.fc_old.numeric_column` defining feature column representing weights.
      It is used to downweight or boost examples during training. It will be
      multiplied by the loss of the example. If it is a string, it is used as a
      key to fetch weight tensor from the `features`. If it is a
      `_NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
      weight_column.normalizer_fn is applied on it to get weight tensor.
    train_in_memory: `bool`, when true, it assumes the dataset is in memory,
      i.e., input_fn should return the entire dataset as a single batch,
      n_batches_per_layer should be set as 1, num_worker_replicas should be 1,
      and num_ps_replicas should be 0 in `tf.Estimator.RunConfig`.
    name: Name to use for the model.

  Returns:
      An `EstimatorSpec` instance.

  Raises:
    ValueError: mode or params are invalid, or features has the wrong type.
  """
  logits_dimension = head.logits_dimension
  sorted_feature_columns = sorted(feature_columns, key=lambda tc: tc.name)
  float_columns = _get_float_feature_columns(sorted_feature_columns)

  with ops.name_scope(name) as name:
    # Prepare.
    global_step = tf.compat.v1.train.get_or_create_global_step()
    # Create Ensemble resources.
    tree_ensemble = boosted_trees_ops.TreeEnsemble(name=name)

    # Create Quantile accumulator resource.
    eps = tree_hparams.quantile_sketch_epsilon
    num_quantiles = int(1. / eps)
    bucket_boundaries_dict = {}
    quantile_accumulator = None

    if float_columns:
      num_float_features = _calculate_num_features(float_columns)
      quantile_accumulator = boosted_trees_ops.QuantileAccumulator(
          epsilon=eps,
          num_streams=num_float_features,
          num_quantiles=num_quantiles,
          name=_QUANTILE_ACCUMULATOR_RESOURCE_NAME)
      bucket_boundaries = quantile_accumulator.get_bucket_boundaries()
      bucket_boundaries_dict = _get_float_boundaries_dict(
          float_columns, bucket_boundaries)
      are_boundaries_ready_initial = False
    else:
      are_boundaries_ready_initial = True

    bucket_size_list, feature_ids_list, split_types_list = _group_features_by_num_buckets_and_split_type(
        sorted_feature_columns, num_quantiles)

    # Create logits.
    if mode != ModeKeys.TRAIN:
      input_feature_list = _get_transformed_features(features,
                                                     sorted_feature_columns,
                                                     bucket_boundaries_dict)
      logits = boosted_trees_ops.predict(
          # For non-TRAIN mode, ensemble doesn't change after initialization,
          # so no local copy is needed; using tree_ensemble directly.
          tree_ensemble_handle=tree_ensemble.resource_handle,
          bucketized_features=input_feature_list,
          logits_dimension=logits_dimension)
      return head.create_estimator_spec(
          features=features,
          mode=mode,
          labels=labels,
          train_op_fn=tf.no_op,
          logits=logits)

    # ============== Training graph ==============
    center_bias = tree_hparams.center_bias
    is_single_machine = (config.num_worker_replicas <= 1)

    if train_in_memory:
      assert n_batches_per_layer == 1, (
          'When train_in_memory is enabled, input_fn should return the entire '
          'dataset as a single batch, and n_batches_per_layer should be set as '
          '1.')
      if (not config.is_chief or config.num_worker_replicas > 1 or
          config.num_ps_replicas > 0):
        raise ValueError('train_in_memory is supported only for '
                         'non-distributed training.')
    worker_device = tf.no_op().device
    # Extract input features and set up cache for training.
    training_state_cache = None

    are_boundaries_ready = _variable(
        initial_value=are_boundaries_ready_initial,
        name='are_boundaries_ready',
        trainable=False)

    if train_in_memory:
      # Cache transformed features as well for in-memory training.
      batch_size = tf.compat.v1.shape(labels)[0]

      def _split_into_indicator_and_other_columns():
        indicator_columns = []
        other_columns = []
        for fc in sorted_feature_columns:
          if isinstance(
              fc,
              (feature_column_lib.IndicatorColumn, fc_old._IndicatorColumn)):
            indicator_columns.append(fc)
          else:
            other_columns.append(fc)
        return indicator_columns, other_columns

      # Split columns into indicator and other columns.
      indicator_columns, other_columns = (
          _split_into_indicator_and_other_columns())

      input_feature_list, input_cache_op = _cache_transformed_features(
          features, sorted_feature_columns, indicator_columns, other_columns,
          batch_size, bucket_boundaries_dict, are_boundaries_ready)

      training_state_cache = _CacheTrainingStatesUsingVariables(
          batch_size, logits_dimension)
    else:
      input_feature_list = _get_transformed_features(features,
                                                     sorted_feature_columns,
                                                     bucket_boundaries_dict)
      if example_id_column_name:
        example_ids = features[example_id_column_name]
        training_state_cache = _CacheTrainingStatesUsingHashTable(
            example_ids, logits_dimension)
    if training_state_cache:
      cached_tree_ids, cached_node_ids, cached_logits = (
          training_state_cache.lookup())
    else:
      # Always start from the beginning when no cache is set up.
      batch_size = tf.compat.v1.shape(labels)[0]
      cached_tree_ids, cached_node_ids, cached_logits = (
          tf.zeros([batch_size], dtype=tf.dtypes.int32),
          _DUMMY_NODE_ID * tf.ones([batch_size], dtype=tf.dtypes.int32),
          tf.zeros([batch_size, logits_dimension], dtype=tf.dtypes.float32))

    if is_single_machine:
      local_tree_ensemble = tree_ensemble
      ensemble_reload = tf.no_op()
    else:
      # Have a local copy of ensemble for the distributed setting.
      with tf.compat.v1.device(worker_device):
        local_tree_ensemble = boosted_trees_ops.TreeEnsemble(
            name=name + '_local', is_local=True)
      # TODO(soroush): Do partial updates if this becomes a bottleneck.
      ensemble_reload = local_tree_ensemble.deserialize(
          *tree_ensemble.serialize())
    with tf.control_dependencies([ensemble_reload]):
      (stamp_token, num_trees, num_finalized_trees, num_attempted_layers,
       last_layer_nodes_range) = local_tree_ensemble.get_states()
      partial_logits, tree_ids, node_ids = boosted_trees_ops.training_predict(
          tree_ensemble_handle=local_tree_ensemble.resource_handle,
          cached_tree_ids=cached_tree_ids,
          cached_node_ids=cached_node_ids,
          bucketized_features=input_feature_list,
          logits_dimension=logits_dimension)
    logits = cached_logits + partial_logits
    if train_in_memory:
      grower = _InMemoryEnsembleGrower(tree_ensemble, quantile_accumulator,
                                       tree_hparams, feature_ids_list,
                                       logits_dimension)
    else:
      feature_dimensions = _get_feature_dimensions(feature_ids_list,
                                                   input_feature_list)
      grower = _AccumulatorEnsembleGrower(tree_ensemble, quantile_accumulator,
                                          tree_hparams, stamp_token,
                                          n_batches_per_layer, bucket_size_list,
                                          config.is_chief, center_bias,
                                          feature_ids_list, logits_dimension,
                                          feature_dimensions)

    tf.compat.v1.summary.scalar('ensemble/num_trees', num_trees)
    tf.compat.v1.summary.scalar('ensemble/num_finalized_trees',
                                num_finalized_trees)
    tf.compat.v1.summary.scalar('ensemble/num_attempted_layers',
                                num_attempted_layers)

    # Variable that determines whether bias centering is needed.
    center_bias_var = _variable(
        initial_value=center_bias, name='center_bias_needed', trainable=False)
    if weight_column is None:
      weights = tf.constant(1., shape=[1])
    else:
      if isinstance(weight_column, six.string_types):
        weight_column = tf.feature_column.numeric_column(
            key=weight_column, shape=(1,))
      weights = tf.compat.v1.squeeze(
          _get_transformed_features(features, [weight_column])[0], axis=1)

    # Create training graph.
    def _train_op_fn(loss):
      """Run one training iteration."""

      def _update_quantile_fn():
        """Accumulates quantiles."""
        with ops.name_scope('UpdateQuantile'):
          float_features = _get_transformed_features(features, float_columns)
          squeezed = [tf.compat.v1.squeeze(f, axis=1) for f in float_features]
          return grower.accumulate_quantiles(squeezed, weights,
                                             are_boundaries_ready)

      def _grow_tree_fn():
        """Grow tree."""
        grow_op = [input_cache_op] if train_in_memory else []
        if training_state_cache:
          # Cache logits only after center_bias is complete,
          # if it's in progress.
          def insert_fn():
            return training_state_cache.insert(tree_ids, node_ids, logits)

          grow_op.append(_cond(center_bias_var, tf.no_op, insert_fn))

        if logits_dimension == 1 and closed_form_grad_and_hess_fn:
          gradients, hessians = closed_form_grad_and_hess_fn(logits, labels)
        else:
          gradients = tf.compat.v1.gradients(loss, logits, name='Gradients')[0]
          if logits_dimension == 1:
            hessians = tf.compat.v1.gradients(
                gradients, logits, name='Hessians')[0]
          else:
            # TODO(crawles): support diagonal hessian.
            hessians = _compute_full_hessian(gradients, logits, logits_dimension)
        # TODO(youngheek): perhaps storage could be optimized by storing stats
        # with the dimension max_splits_per_layer, instead of max_splits (for
        # the entire tree).
        max_splits = _get_max_splits(tree_hparams)
        stats_summaries_list = []
        for num_buckets, feature_ids in zip(bucket_size_list, feature_ids_list):
          bucket_summaries = []
          for feature_id in feature_ids:
            stats_summary = boosted_trees_ops.boosted_trees_aggregate_stats(
                node_ids=node_ids,
                gradients=gradients,
                hessians=hessians,
                feature=input_feature_list[feature_id],
                max_splits=max_splits,
                num_buckets=num_buckets)
            bucket_summaries.append(stats_summary)
          stats_summaries_list.append(bucket_summaries)
        if center_bias:
          update_model = _cond(
              center_bias_var,
              functools.partial(grower.center_bias, center_bias_var, gradients,
                                hessians),
              functools.partial(grower.grow_tree, stats_summaries_list,
                                last_layer_nodes_range, split_types_list))
        else:
          update_model = grower.grow_tree(stats_summaries_list,
                                          last_layer_nodes_range,
                                          split_types_list)
        grow_op.append(update_model)

        with tf.control_dependencies([update_model]):
          increment_global = tf.compat.v1.assign_add(global_step, 1).op
          grow_op.append(increment_global)

        return tf.group(grow_op, name='grow_op')

      if not float_columns:
        return _grow_tree_fn()
      else:
        return _cond(are_boundaries_ready, _grow_tree_fn, _update_quantile_fn)

  estimator_spec = head.create_estimator_spec(
      features=features,
      mode=mode,
      labels=labels,
      train_op_fn=_train_op_fn,
      logits=logits)
  # Add an early stop hook.
  estimator_spec = estimator_spec._replace(
      training_hooks=estimator_spec.training_hooks +
      (_StopAtAttemptsHook(num_finalized_trees, num_attempted_layers,
                           tree_hparams.n_trees, tree_hparams.max_depth),),
      training_chief_hooks=[GrowerInitializationHook(grower.chief_init_op())] +
      list(estimator_spec.training_chief_hooks))
  return estimator_spec


class GrowerInitializationHook(tf.compat.v1.train.SessionRunHook):
  """A SessionRunHook handles initialization of `_EnsembleGrower`."""

  def __init__(self, init_op):
    self._init_op = init_op

  def after_create_session(self, session, coord):
    session.run(self._init_op)


# This is classical form of Maximum entropy loss, that is twice differentiable
# (the built-in sparse_softmax_cross_entropy is not twice differentiable).
def per_example_maxent_loss(labels, weights, logits, num_classes, eps=1e-15):
  """Maximum entropy loss for multiclass problems.

  Maximum entropy is a generalization of logistic loss for the case when more
  than 2 classes are present.

  Args:
    labels: Rank 2 (N, 1) or Rank 1 (N) tensor of per-example labels.
    weights: Rank 2 (N, 1) tensor of per-example weights.
    logits: Rank 2 (N, K) tensor of per-example predictions, K - num of classes.
    num_classes: number of classes in classification task. Used to expand label
      indices into one-hot encodings.
    eps: tolerance, used as a minimum possible value.

  Returns:
    loss: A Rank 2 (N, 1) tensor of per-example maxent loss
    update_op: An update operation to update the loss's internal state.
  """
  labels = tf.cast(labels, tf.dtypes.int64)
  # If labels are of rank 1, make them rank 2.
  labels_shape = labels.get_shape()
  if len(labels_shape) != 2:
    labels = tf.compat.v1.expand_dims(labels, 1)
  # Labels are indices of classes, convert them to one hot encodings.
  target_one_hot = tf.one_hot(indices=labels, depth=num_classes)
  labels = tf.math.reduce_sum(input_tensor=target_one_hot, axis=[1])
  labels = tf.cast(labels, tf.dtypes.float32)

  # Calculate softmax probabilities for each class.
  unnormalized_probs = tf.math.exp(logits)
  normalizers = tf.math.reduce_sum(unnormalized_probs, 1, keepdims=True)
  softmax_predictions = tf.math.divide(unnormalized_probs,
                                       tf.math.add(normalizers, eps))

  # Pull out the probabilities for real label.
  probs_for_real_class = tf.math.reduce_sum(labels * softmax_predictions, 1)

  # Add handling for values near 0 and 1.
  zeros = tf.compat.v1.zeros_like(
      probs_for_real_class, dtype=logits.dtype) + eps
  one_minus_eps = tf.compat.v1.ones_like(
      probs_for_real_class, dtype=logits.dtype) - eps

  # Take maximum(eps, pred)
  cond = (probs_for_real_class >= eps)
  probs_for_real_class = tf.compat.v1.where(cond, probs_for_real_class, zeros)

  # Take minimum(1-eps, pred)
  cond = (probs_for_real_class <= 1 - eps)
  probs_for_real_class = tf.compat.v1.where(cond, probs_for_real_class,
                                            one_minus_eps)

  unweighted_loss = tf.compat.v1.expand_dims(-tf.math.log(probs_for_real_class),
                                             1)
  if weights is None:
    return unweighted_loss, tf.no_op()
  else:
    return unweighted_loss * weights, tf.no_op()


def _compute_full_hessian(grads, logits, logits_dimension):
  """Computes hessians for full-hessian multiclass strategy."""
  gradients_list = tf.compat.v1.unstack(
      grads, num=logits_dimension, axis=1)
  hessian_rows = []

  for row in range(logits_dimension):
    # If current row is i, K is number of classes,each row returns a tensor of
    # size batch_size x K representing for each example dx_i dx_1, dx_i dx_2
    # etc dx_i dx_K
    hessian_row = tf.compat.v1.gradients(
        gradients_list[row],
        logits,
        name='Hessian_%d' % row,
        colocate_gradients_with_ops=False,
        gate_gradients=0,
        aggregation_method=None)

    # hessian_row is of dimension 1, batch_size, K, => trim first dimension
    # to get batch_size x K
    hessian_row = tf.compat.v1.squeeze(tf.compat.v1.unstack(hessian_row), [0])
    hessian_rows.append(hessian_row)
  stacked = tf.compat.v1.stack(hessian_rows, axis=1)
  return tf.compat.v1.reshape(stacked, (-1, logits_dimension**2))


def _multiclass_head(
    n_classes,
    weight_column=None,
    label_vocabulary=None,
    loss_reduction=tf.compat.v1.losses.Reduction.SUM_OVER_NONZERO_WEIGHTS):
  """Core head for multiclass problems."""

  def loss_fn(labels, logits):
    result = per_example_maxent_loss(
        labels=labels,
        logits=logits,
        weights=weight_column,
        num_classes=n_classes)
    return result[0]

  # pylint:disable=protected-access
  head_fn = head_lib._multi_class_head_with_softmax_cross_entropy_loss(
      n_classes=n_classes,
      loss_fn=loss_fn,
      loss_reduction=loss_reduction,
      label_vocabulary=label_vocabulary,
      weight_column=weight_column)
  # pylint:enable=protected-access

  return head_fn


def _create_classification_head(n_classes,
                                weight_column=None,
                                label_vocabulary=None):
  """Creates a classification head. Refer to canned.head for details on args."""
  if n_classes == 2:
    # pylint: disable=protected-access
    return head_lib._binary_logistic_head_with_sigmoid_cross_entropy_loss(
        weight_column=weight_column,
        label_vocabulary=label_vocabulary,
        loss_reduction=tf.compat.v1.losses.Reduction.SUM_OVER_BATCH_SIZE)
    # pylint: enable=protected-access
  else:
    return _multiclass_head(
        n_classes,
        weight_column,
        label_vocabulary=label_vocabulary,
        loss_reduction=tf.compat.v1.losses.Reduction.SUM_OVER_NONZERO_WEIGHTS)


def _create_classification_head_and_closed_form(n_classes, weight_column,
                                                label_vocabulary):
  """Creates a head for classifier and the closed form gradients/hessians."""
  head = _create_classification_head(n_classes, weight_column, label_vocabulary)
  if (n_classes == 2 and head.logits_dimension == 1 and
      weight_column is None and label_vocabulary is None):
    # Use the closed-form gradients/hessians for 2 class.
    def _grad_and_hess_for_logloss(logits, labels):
      """A closed form gradient and hessian for logistic loss."""
      # TODO(youngheek): add weights handling.
      predictions = tf.math.reciprocal(tf.math.exp(-logits) + 1.0)
      normalizer = tf.math.reciprocal(
          tf.cast(tf.compat.v1.size(predictions), tf.dtypes.float32))
      labels = tf.cast(labels, tf.dtypes.float32)
      labels = head_lib._check_dense_labels_match_logits_and_reshape(  # pylint: disable=protected-access
          labels, logits, head.logits_dimension)
      gradients = (predictions - labels) * normalizer
      hessians = predictions * (1.0 - predictions) * normalizer
      return gradients, hessians

    closed_form = _grad_and_hess_for_logloss
  else:
    closed_form = None
  return (head, closed_form)


def _create_regression_head(label_dimension, weight_column=None):
  # pylint: disable=protected-access
  return head_lib._regression_head(
      label_dimension=label_dimension,
      weight_column=weight_column,
      loss_reduction=tf.compat.v1.losses.Reduction.SUM_OVER_BATCH_SIZE)
  # pylint: enable=protected-access


def _compute_feature_importances_per_tree(tree, num_features):
  """Computes the importance of each feature in the tree."""
  importances = np.zeros(num_features)

  for node in tree.nodes:
    node_type = node.WhichOneof('node')
    # TODO(crawles): support for categorical features.
    if node_type == 'bucketized_split':
      feature_id = node.bucketized_split.feature_id
      importances[feature_id] += node.metadata.gain
    elif node_type == 'leaf':
      assert node.metadata.gain == 0
    else:
      raise ValueError('Unexpected split type %s' % node_type)

  return importances


def _compute_feature_importances(tree_ensemble, num_features, normalize):
  """Computes gain-based feature importances.

  The higher the value, the more important the feature.

  Args:
    tree_ensemble: a trained tree ensemble, instance of proto
      boosted_trees.TreeEnsemble.
    num_features: The total number of feature ids.
    normalize: If True, normalize the feature importances.

  Returns:
    feature_importances: A list of corresponding feature importances indexed by
    the original feature ids.

  Raises:
    AssertionError: When normalize = True, if feature importances
      contain negative value, or if normalization is not possible
      (e.g. ensemble is empty or trees contain only a root node).
  """
  tree_importances = [
      _compute_feature_importances_per_tree(tree, num_features)
      for tree in tree_ensemble.trees
  ]
  tree_importances = np.array(tree_importances)
  tree_weights = np.array(tree_ensemble.tree_weights).reshape(-1, 1)
  feature_importances = np.sum(tree_importances * tree_weights, axis=0)
  if normalize:
    assert np.all(feature_importances >= 0), ('feature_importances '
                                              'must be non-negative.')
    normalizer = np.sum(feature_importances)
    assert normalizer > 0, 'Trees are all empty or contain only a root node.'
    feature_importances /= normalizer

  return feature_importances


def _bt_explanations_fn(features,
                        head,
                        sorted_feature_columns,
                        quantile_sketch_epsilon,
                        name='boosted_trees'):
  """Gradient Boosted Trees predict with explanations model_fn.

  Args:
    features: dict of `Tensor`.
    head: A `head_lib._Head` instance.
    sorted_feature_columns: Sorted iterable of `fc_old._FeatureColumn` model
      inputs.
    quantile_sketch_epsilon: float between 0 and 1. Error bound for quantile
      computation. This is only used for float feature columns, and the number
      of buckets generated per float feature is 1/quantile_sketch_epsilon.
    name: Name used for the model.

  Returns:
      An `EstimatorSpec` instance.

  Raises:
    ValueError: mode or params are invalid, or features has the wrong type.
  """
  mode = ModeKeys.PREDICT
  with ops.name_scope(name) as name:
    # Create Ensemble resources.
    tree_ensemble = boosted_trees_ops.TreeEnsemble(name=name)

    # pylint: disable=protected-access
    float_columns = _get_float_feature_columns(sorted_feature_columns)
    num_float_features = _calculate_num_features(float_columns)
    # pylint: enable=protected-access
    num_quantiles = int(1. / quantile_sketch_epsilon)
    if not num_float_features:
      input_feature_list = _get_transformed_features(features,
                                                     sorted_feature_columns)
    # Create Quantile accumulator resource.
    else:
      quantile_accumulator = boosted_trees_ops.QuantileAccumulator(
          epsilon=quantile_sketch_epsilon,
          num_streams=num_float_features,
          num_quantiles=num_quantiles,
          name=_QUANTILE_ACCUMULATOR_RESOURCE_NAME)
      bucket_boundaries = quantile_accumulator.get_bucket_boundaries()
      bucket_boundaries_dict = _get_float_boundaries_dict(
          float_columns, bucket_boundaries)
      input_feature_list = _get_transformed_features(features,
                                                     sorted_feature_columns,
                                                     bucket_boundaries_dict)
    logits = boosted_trees_ops.predict(
        # For non-TRAIN mode, ensemble doesn't change after initialization,
        # so no local copy is needed; using tree_ensemble directly.
        tree_ensemble_handle=tree_ensemble.resource_handle,
        bucketized_features=input_feature_list,
        logits_dimension=head.logits_dimension)

    estimator_spec = head.create_estimator_spec(
        features=features,
        mode=mode,
        labels=None,
        train_op_fn=tf.no_op,
        logits=logits)

    debug_op = boosted_trees_ops.example_debug_outputs(
        tree_ensemble.resource_handle,
        bucketized_features=input_feature_list,
        logits_dimension=head.logits_dimension)
    estimator_spec.predictions[boosted_trees_utils._DEBUG_PROTO_KEY] = debug_op  # pylint: disable=protected-access
    return estimator_spec


def _get_float_boundaries_dict(float_columns, bucket_boundaries):
  """Create a dict where key is column name, value is bucket boundaries."""
  bucket_boundaries_dict = {}
  feature_idx = 0
  for column in float_columns:
    num_column_dimensions = _get_variable_shape(
        column)[0] if _get_variable_shape(column).as_list() else 1
    bucket_boundaries_dict[
        column.name] = bucket_boundaries[feature_idx:feature_idx +
                                         num_column_dimensions]
    feature_idx += num_column_dimensions
  return bucket_boundaries_dict


class _BoostedTreesBase(estimator.Estimator):
  """Base class for boosted trees estimators.

  This class is intended to keep tree-specific functions (E.g., methods for
  feature importances and directional feature contributions) in one central
  place.

  It is not a valid (working) Estimator on its own and should only be used as a
  base class.
  """

  def __init__(self, model_fn, model_dir, config, feature_columns, head,
               center_bias, is_classification, quantile_sketch_epsilon):
    """Initializes a `_BoostedTreesBase` instance.

    Args:
      model_fn: model_fn: Model function. See base class for more detail.
      model_dir: Directory to save model parameters, graph and etc. See base
        class for more detail.
      config: `estimator.RunConfig` configuration object.
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`
      head: A `head_lib._Head` instance.
      center_bias: Whether bias centering needs to occur. Bias centering refers
        to the first node in the very first tree returning the prediction that
        is aligned with the original labels distribution. For example, for
        regression problems, the first node will return the mean of the labels.
        For binary classification problems, it will return a logit for a prior
        probability of label 1.
      is_classification: If the estimator is for classification.
      quantile_sketch_epsilon: float between 0 and 1. Error bound for quantile
        computation. This is only used for float feature columns, and the number
        of buckets generated per float feature is 1/quantile_sketch_epsilon.
    """
    # We need it so the global step is also a resource var.
    tf.compat.v1.enable_resource_variables()

    super(_BoostedTreesBase, self).__init__(
        model_fn=model_fn, model_dir=model_dir, config=config)
    self._sorted_feature_columns = sorted(
        feature_columns, key=lambda tc: tc.name)
    self._head = head
    self._n_features = _calculate_num_features(self._sorted_feature_columns)
    self._feature_col_names = _generate_feature_col_name_mapping(
        self._sorted_feature_columns)
    self._center_bias = center_bias
    self._is_classification = is_classification
    self._quantile_sketch_epsilon = quantile_sketch_epsilon

  def experimental_feature_importances(self, normalize=False):
    """Computes gain-based feature importances.

    The higher the value, the more important the corresponding feature.

    Args:
      normalize: If True, normalize the feature importances.

    Returns:
      feature_importances: an OrderedDict, where the keys are the feature column
      names and the values are importances. It is sorted by importance.

    Raises:
      ValueError: When attempting to normalize on an empty ensemble
        or an ensemble of trees which have no splits. Or when attempting
        to normalize and feature importances have negative values.
    """
    reader = tf.train.load_checkpoint(self._model_dir)
    serialized = reader.get_tensor('boosted_trees:0_serialized')
    if not serialized:
      raise ValueError('Found empty serialized string for TreeEnsemble.'
                       'You should only call this method after training.')
    ensemble_proto = boosted_trees_pb2.TreeEnsemble()
    ensemble_proto.ParseFromString(serialized)

    importances = _compute_feature_importances(ensemble_proto, self._n_features,
                                               normalize)
    # pylint:disable=protected-access
    return boosted_trees_utils._sum_by_feature_col_name_and_sort(
        self._feature_col_names, importances)
    # pylint:enable=protected-access

  def experimental_predict_with_explanations(self,
                                             input_fn,
                                             predict_keys=None,
                                             hooks=None,
                                             checkpoint_path=None):
    """Computes model explainability outputs per example along with predictions.

    Currently supports directional feature contributions (DFCs). For each
    instance, DFCs indicate the aggregate contribution of each feature. See
    https://arxiv.org/abs/1312.1121 and
    http://blog.datadive.net/interpreting-random-forests/ for more details.

    Args:
      input_fn: A function that provides input data for predicting as
        minibatches. See [Premade Estimators](
        https://tensorflow.org/guide/premade_estimators#create_input_functions)
          for more information. The function should construct and return one of
        the following:
        * A `tf.data.Dataset` object: Outputs of `Dataset` object must be a
          tuple `(features, labels)` with same constraints as below.
        * A tuple `(features, labels)`: Where `features` is a `tf.Tensor` or a
          dictionary of string feature name to `Tensor` and `labels` is a
          `Tensor` or a dictionary of string label name to `Tensor`. Both
          `features` and `labels` are consumed by `model_fn`. They should
          satisfy the expectation of `model_fn` from inputs.
      predict_keys: list of `str`, name of the keys to predict. It is used if
        the `tf.estimator.EstimatorSpec.predictions` is a `dict`. If
        `predict_keys` is used then rest of the predictions will be filtered
        from the dictionary, with the exception of 'bias' and 'dfc', which will
        always be in the dictionary. If `None`, returns all keys in prediction
        dict, as well as two new keys 'dfc' and 'bias'.
      hooks: List of `tf.train.SessionRunHook` subclass instances. Used for
        callbacks inside the prediction call.
      checkpoint_path: Path of a specific checkpoint to predict. If `None`, the
        latest checkpoint in `model_dir` is used.  If there are no checkpoints
        in `model_dir`, prediction is run with newly initialized `Variables`
        instead of ones restored from checkpoint.

    Yields:
      Evaluated values of `predictions` tensors. The `predictions` tensors will
      contain at least two keys 'dfc' and 'bias' for model explanations. The
      `dfc` value corresponds to the contribution of each feature to the overall
      prediction for this instance (positive indicating that the feature makes
      it more likely to select class 1 and negative less likely). The `dfc` is
      an OrderedDict, where the keys are the feature column names and the values
      are the contributions. It is sorted by the absolute value of the
      contribution (e.g OrderedDict([('age', -0.54), ('gender', 0.4), ('fare',
      0.21)])). The 'bias' value will be the same across all the instances,
      corresponding to the probability (classification) or prediction
      (regression) of the training data distribution.

    Raises:
      ValueError: when wrong arguments are given or unsupported functionalities
       are requested.
    """
    if not self._center_bias:
      raise ValueError('center_bias must be enabled during estimator '
                       'instantiation when using '
                       'experimental_predict_with_explanations.')
    elif self._head.logits_dimension > 1:
      raise ValueError('experimental_predict_with_explanations does not yet '
                       'support multi-class classification and multi-label '
                       'regression.')
    # pylint: disable=protected-access
    if not self._is_classification:
      identity_inverse_link_fn = self._head._inverse_link_fn in (None,
                                                                 tf_identity)
      # pylint:enable=protected-access
      if not identity_inverse_link_fn:
        raise ValueError(
            'For now only identity inverse_link_fn in regression_head is '
            'supported for experimental_predict_with_explanations.')

    # pylint:disable=unused-argument
    def new_model_fn(features, labels, mode):
      return _bt_explanations_fn(features, self._head,
                                 self._sorted_feature_columns,
                                 self._quantile_sketch_epsilon)

    # pylint:enable=unused-argument
    est = estimator.Estimator(
        model_fn=new_model_fn,
        model_dir=self.model_dir,
        config=self.config,
        warm_start_from=self._warm_start_settings)
    # Make sure bias and dfc will be in prediction dict.
    user_supplied_predict_keys = predict_keys is not None
    if user_supplied_predict_keys:
      predict_keys = set(predict_keys)
      predict_keys.add(boosted_trees_utils._DEBUG_PROTO_KEY)
    predictions = est.predict(
        input_fn,
        predict_keys=predict_keys,
        hooks=hooks,
        checkpoint_path=checkpoint_path,
        yield_single_examples=True)
    for pred in predictions:
      bias, dfcs = boosted_trees_utils._parse_explanations_from_prediction(
          pred[boosted_trees_utils._DEBUG_PROTO_KEY], self._feature_col_names,
          self._is_classification)
      pred['bias'] = bias
      pred['dfc'] = dfcs
      # Don't need to expose serialized proto to end user.
      del pred[boosted_trees_utils._DEBUG_PROTO_KEY]
      yield pred


def _validate_input_params(tree_params):
  """Validate input parameters."""
  positive = ('n_trees', 'max_depth', 'learning_rate',
              'quantile_sketch_epsilon')
  for p in positive:
    if getattr(tree_params, p) <= 0:
      raise ValueError('Expected {} > 0, received: {}'.format(
          p, getattr(tree_params, p)))
  non_negative = ('l1', 'l2', 'tree_complexity', 'min_node_weight')
  for p in non_negative:
    if getattr(tree_params, p) < 0:
      raise ValueError('Expected {} >= 0, received: {}'.format(
          p, getattr(tree_params, p)))


# pylint: disable=protected-access
@estimator_export('estimator.BoostedTreesClassifier')
class BoostedTreesClassifier(_BoostedTreesBase):
  """A Classifier for Tensorflow Boosted Trees models.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(self,
               feature_columns,
               n_batches_per_layer,
               model_dir=None,
               n_classes=2,
               weight_column=None,
               label_vocabulary=None,
               n_trees=100,
               max_depth=6,
               learning_rate=0.1,
               l1_regularization=0.,
               l2_regularization=0.,
               tree_complexity=0.,
               min_node_weight=0.,
               config=None,
               center_bias=False,
               pruning_mode='none',
               quantile_sketch_epsilon=0.01,
               train_in_memory=False):
    """Initializes a `BoostedTreesClassifier` instance.

    Example:

    ```python
    bucketized_feature_1 = bucketized_column(
      numeric_column('feature_1'), BUCKET_BOUNDARIES_1)
    bucketized_feature_2 = bucketized_column(
      numeric_column('feature_2'), BUCKET_BOUNDARIES_2)

    # Need to see a large portion of the data before we can build a layer, for
    # example half of data n_batches_per_layer = 0.5 * NUM_EXAMPLES / BATCH_SIZE
    # Also note that it is usually beneficial to set some regularization, for
    # example, l2. A good default value is 1./number of examples per layer.
    classifier = estimator.BoostedTreesClassifier(
        feature_columns=[bucketized_feature_1, bucketized_feature_2],
        n_batches_per_layer=n_batches_per_layer,
        n_trees=100,
        l2_regularization = 1./(n_batches_per_layer*batch_size)
        ... <some other params>
    )

    def input_fn_train():
      ...
      return dataset

    classifier.train(input_fn=input_fn_train)

    def input_fn_eval():
      ...
      return dataset

    metrics = classifier.evaluate(input_fn=input_fn_eval)

    when train_in_memory = True, make sure the input fn is not batched:
    def input_fn_train():
      return tf.data.Dataset.zip(
        (tf.data.Dataset.from_tensors({'f1': f1_array, ...}),
         tf.data.Dataset.from_tensors(label_array)))
    ```

    Args:
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      n_batches_per_layer: the number of batches to collect statistics per
        layer. The total number of batches is total number of data divided by
        batch size.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      n_classes: number of label classes. Default is binary classification.
      weight_column: A string or a `NumericColumn` created by
        `tf.fc_old.numeric_column` defining feature column representing weights.
        It is used to downweight or boost examples during training. It will be
        multiplied by the loss of the example. If it is a string, it is used as
        a key to fetch weight tensor from the `features`. If it is a
        `NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
        `weight_column.normalizer_fn` is applied on it to get weight tensor.
      label_vocabulary: A list of strings represents possible label values. If
        given, labels must be string type and have any value in
        `label_vocabulary`. If it is not given, that means labels are already
        encoded as integer or float within `[0, 1]` for `n_classes=2` and
        encoded as integer values in {0, 1,..., n_classes-1} for `n_classes>2`.
        Also, there will be errors if vocabulary is not provided and labels are
        string.
      n_trees: number trees to be created.
      max_depth: maximum depth of the tree to grow.
      learning_rate: shrinkage parameter to be used when a tree added to the
        model.
      l1_regularization: regularization multiplier applied to the absolute
        weights of the tree leafs. This is a per instance value. A good default
        is 1./(n_batches_per_layer*batch_size).
      l2_regularization: regularization multiplier applied to the square weights
        of the tree leafs. This is a per instance value. A good default is
        1./(n_batches_per_layer*batch_size).
      tree_complexity: regularization factor to penalize trees with more leaves.
        This is a per instance value. A good default is
        1./(n_batches_per_layer*batch_size).
      min_node_weight: min_node_weight: minimum hessian a node must have for a
        split to be considered. This is a per instance value. The value will be
        compared with `sum(leaf_hessian)/(batch_size * n_batches_per_layer)`.
      config: `RunConfig` object to configure the runtime settings.
      center_bias: Whether bias centering needs to occur. Bias centering refers
        to the first node in the very first tree returning the prediction that
        is aligned with the original labels distribution. For example, for
        regression problems, the first node will return the mean of the labels.
        For binary classification problems, it will return a logit for a prior
        probability of label 1.
      pruning_mode: one of `none`, `pre`, `post` to indicate no pruning, pre-
        pruning (do not split a node if not enough gain is observed) and post
        pruning (build the tree up to a max depth and then prune branches with
        negative gain). For pre and post pruning, you MUST provide
        `tree_complexity >0`.
      quantile_sketch_epsilon: float between 0 and 1. Error bound for quantile
        computation. This is only used for float feature columns, and the number
        of buckets generated per float feature is `1/quantile_sketch_epsilon`.
      train_in_memory: `bool`, when true, it assumes the dataset is in memory,
        i.e., `input_fn` should return the entire dataset as a single batch,
        `n_batches_per_layer` should be set as 1, `num_worker_replicas` should
        be 1, and `num_ps_replicas` should be 0 in `tf.Estimator.RunConfig`.

    Raises:
      ValueError: when wrong arguments are given or unsupported functionalities
         are requested.
    """
    if n_classes > 2 and center_bias:
      raise ValueError('center_bias not yet support with n_classes > 2.')
    head, closed_form = _create_classification_head_and_closed_form(
        n_classes, weight_column, label_vocabulary=label_vocabulary)
    # HParams for the model.
    tree_hparams = _TreeHParams(n_trees, max_depth, learning_rate,
                                l1_regularization, l2_regularization,
                                tree_complexity, min_node_weight, center_bias,
                                pruning_mode, quantile_sketch_epsilon)
    _validate_input_params(tree_hparams)

    def _model_fn(features, labels, mode, config):
      return _bt_model_fn(
          features,
          labels,
          mode,
          head,
          feature_columns,
          tree_hparams,
          n_batches_per_layer,
          config,
          closed_form_grad_and_hess_fn=closed_form,
          weight_column=weight_column,
          train_in_memory=train_in_memory)

    super(BoostedTreesClassifier, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        feature_columns=feature_columns,
        head=head,
        center_bias=center_bias,
        is_classification=True,
        quantile_sketch_epsilon=quantile_sketch_epsilon)


@estimator_export('estimator.BoostedTreesRegressor')
class BoostedTreesRegressor(_BoostedTreesBase):
  """A Regressor for Tensorflow Boosted Trees models.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(self,
               feature_columns,
               n_batches_per_layer,
               model_dir=None,
               label_dimension=1,
               weight_column=None,
               n_trees=100,
               max_depth=6,
               learning_rate=0.1,
               l1_regularization=0.,
               l2_regularization=0.,
               tree_complexity=0.,
               min_node_weight=0.,
               config=None,
               center_bias=False,
               pruning_mode='none',
               quantile_sketch_epsilon=0.01,
               train_in_memory=False):
    """Initializes a `BoostedTreesRegressor` instance.

    Example:

    ```python
    bucketized_feature_1 = bucketized_column(
      numeric_column('feature_1'), BUCKET_BOUNDARIES_1)
    bucketized_feature_2 = bucketized_column(
      numeric_column('feature_2'), BUCKET_BOUNDARIES_2)

    # Need to see a large portion of the data before we can build a layer, for
    # example half of data n_batches_per_layer = 0.5 * NUM_EXAMPLES / BATCH_SIZE
    regressor = estimator.BoostedTreesRegressor(
        feature_columns=[bucketized_feature_1, bucketized_feature_2],
        n_batches_per_layer=n_batches_per_layer,
        n_trees=100,
        ... <some other params>
    )

    def input_fn_train():
      ...
      return dataset

    regressor.train(input_fn=input_fn_train)

    def input_fn_eval():
      ...
      return dataset

    metrics = regressor.evaluate(input_fn=input_fn_eval)
    ```

    Args:
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      n_batches_per_layer: the number of batches to collect statistics per
        layer. The total number of batches is total number of data divided by
        batch size.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      label_dimension: Number of regression targets per example.
      weight_column: A string or a `NumericColumn` created by
        `tf.fc_old.numeric_column` defining feature column representing weights.
        It is used to downweight or boost examples during training. It will be
        multiplied by the loss of the example. If it is a string, it is used as
        a key to fetch weight tensor from the `features`. If it is a
        `NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
        `weight_column.normalizer_fn` is applied on it to get weight tensor.
      n_trees: number trees to be created.
      max_depth: maximum depth of the tree to grow.
      learning_rate: shrinkage parameter to be used when a tree added to the
        model.
      l1_regularization: regularization multiplier applied to the absolute
        weights of the tree leafs.
      l2_regularization: regularization multiplier applied to the square weights
        of the tree leafs.
      tree_complexity: regularization factor to penalize trees with more leaves.
      min_node_weight: min_node_weight: minimum hessian a node must have for a
        split to be considered. The value will be compared with
        `sum(leaf_hessian)/(batch_size * n_batches_per_layer)`.
      config: `RunConfig` object to configure the runtime settings.
      center_bias: Whether bias centering needs to occur. Bias centering refers
        to the first node in the very first tree returning the prediction that
        is aligned with the original labels distribution. For example, for
        regression problems, the first node will return the mean of the labels.
        For binary classification problems, it will return a logit for a prior
        probability of label 1.
      pruning_mode: one of `none`, `pre`, `post` to indicate no pruning, pre-
        pruning (do not split a node if not enough gain is observed) and post
        pruning (build the tree up to a max depth and then prune branches with
        negative gain). For pre and post pruning, you MUST provide
        `tree_complexity>0`.
      quantile_sketch_epsilon: float between 0 and 1. Error bound for quantile
        computation. This is only used for float feature columns, and the number
        of buckets generated per float feature is `1/quantile_sketch_epsilon`.
      train_in_memory: `bool`, when true, it assumes the dataset is in memory,
        i.e., `input_fn` should return the entire dataset as a single batch,
        `n_batches_per_layer` should be set as 1, `num_worker_replicas` should
        be 1, and `num_ps_replicas` should be 0 in `tf.Estimator.RunConfig`.

    Raises:
      ValueError: when wrong arguments are given or unsupported functionalities
         are requested.
    """
    if label_dimension > 1 and center_bias:
      raise ValueError('center_bias not yet support with label_dimension > 1.')
    head = _create_regression_head(label_dimension, weight_column)

    # HParams for the model.
    tree_hparams = _TreeHParams(n_trees, max_depth, learning_rate,
                                l1_regularization, l2_regularization,
                                tree_complexity, min_node_weight, center_bias,
                                pruning_mode, quantile_sketch_epsilon)
    _validate_input_params(tree_hparams)

    def _model_fn(features, labels, mode, config):
      return _bt_model_fn(
          features,
          labels,
          mode,
          head,
          feature_columns,
          tree_hparams,
          n_batches_per_layer,
          config,
          weight_column=weight_column,
          train_in_memory=train_in_memory)

    super(BoostedTreesRegressor, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        feature_columns=feature_columns,
        head=head,
        center_bias=center_bias,
        is_classification=False,
        quantile_sketch_epsilon=quantile_sketch_epsilon)


@estimator_export('estimator.BoostedTreesEstimator')
class BoostedTreesEstimator(_BoostedTreesBase):  # pylint: disable=protected-access
  """An Estimator for Tensorflow Boosted Trees models."""

  def __init__(self,
               feature_columns,
               n_batches_per_layer,
               head,
               model_dir=None,
               weight_column=None,
               n_trees=100,
               max_depth=6,
               learning_rate=0.1,
               l1_regularization=0.,
               l2_regularization=0.,
               tree_complexity=0.,
               min_node_weight=0.,
               config=None,
               center_bias=False,
               pruning_mode='none',
               quantile_sketch_epsilon=0.01):
    """Initializes a `BoostedTreesEstimator` instance.

    Use this interface if you need to provide a custom loss/head.
    For example, the following will be equivalent to using
    BoostedTreesRegressor

    # Create a head with L2 loss
    from tensorflow_estimator.python.estimator.canned import
    head_lib

    head = head_lib._regression_head(label_dimension=1)
    est = boosted_trees.BoostedTreesEstimator(
        feature_columns=...,
        n_batches_per_layer=...,
        head=head,
        n_trees=...,
        max_depth=...)

    Args:
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      n_batches_per_layer: the number of batches to collect statistics per
        layer.
      head: the `Head` instance defined for Estimator.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into an estimator to
        continue training a previously saved model.
      weight_column: A string or a `_NumericColumn` created by
        `tf.feature_column.numeric_column` defining feature column representing
        weights. It is used to downweight or boost examples during training. It
        will be multiplied by the loss of the example. If it is a string, it is
        used as a key to fetch weight tensor from the `features`. If it is a
        `_NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
        weight_column.normalizer_fn is applied on it to get weight tensor.
      n_trees: number trees to be created.
      max_depth: maximum depth of the tree to grow.
      learning_rate: shrinkage parameter to be used when a tree added to the
        model.
      l1_regularization: regularization multiplier applied to the absolute
        weights of the tree leafs.
      l2_regularization: regularization multiplier applied to the square weights
        of the tree leafs.
      tree_complexity: regularization factor to penalize trees with more leaves.
      min_node_weight: minimum hessian a node must have for a split to be
        considered. The value will be compared with `sum(leaf_hessian)/
        (batch_size * n_batches_per_layer)`.
      config: `RunConfig` object to configure the runtime settings.
      center_bias: Whether bias centering needs to occur. Bias centering refers
        to the first node in the very first tree returning the prediction that
        is aligned with the original labels distribution. For example, for
        regression problems, the first node will return the mean of the labels.
        For binary classification problems, it will return a logit for a prior
        probability of label 1.
      pruning_mode: one of `none`, `pre`, `post` to indicate no pruning, pre-
        pruning (do not split a node if not enough gain is observed) and post
        pruning (build the tree up to a max depth and then prune branches with
        negative gain). For pre and post pruning, you MUST provide
        `tree_complexity>0`.
      quantile_sketch_epsilon: float between 0 and 1. Error bound for quantile
        computation. This is only used for float feature columns, and the number
        of buckets generated per float feature is `1/quantile_sketch_epsilon`.

    Raises:
      ValueError: when wrong arguments are given or unsupported functionalities
         are requested.
    """
    # HParams for the model.
    # pylint: disable=protected-access
    tree_hparams = _TreeHParams(n_trees, max_depth, learning_rate,
                                l1_regularization, l2_regularization,
                                tree_complexity, min_node_weight, center_bias,
                                pruning_mode, quantile_sketch_epsilon)
    _validate_input_params(tree_hparams)

    def _model_fn(features, labels, mode, config):
      return _bt_model_fn(
          features,
          labels,
          mode,
          head,
          feature_columns,
          tree_hparams,
          n_batches_per_layer,
          config=config)

    def _is_classification_head(head):
      """Infers if the head is a classification head."""
      # Check using all classification heads defined in canned/head.py. However, it
      # is not a complete list - it does not check for other classification heads
      # not defined in the head library.
      # pylint: disable=protected-access
      return isinstance(
          head, (head_lib._BinaryLogisticHeadWithSigmoidCrossEntropyLoss,
                 head_lib._MultiClassHeadWithSoftmaxCrossEntropyLoss))
      # pylint: enable=protected-access

    super(BoostedTreesEstimator, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        feature_columns=feature_columns,
        head=head,
        center_bias=center_bias,
        is_classification=_is_classification_head(head),
        quantile_sketch_epsilon=quantile_sketch_epsilon)
    # pylint: enable=protected-access


def _get_variable_shape(column):
  """Returns the variable shape of the provided column."""
  if feature_column_lib.is_feature_column_v2([column]):
    return column.variable_shape
  else:
    return column._variable_shape
