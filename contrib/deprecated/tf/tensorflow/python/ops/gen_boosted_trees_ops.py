"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: boosted_trees_ops.cc
"""

import collections as _collections
import six as _six

from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
from tensorflow.python.eager import context as _context
from tensorflow.python.eager import core as _core
from tensorflow.python.eager import execute as _execute
from tensorflow.python.framework import dtypes as _dtypes
from tensorflow.python.framework import errors as _errors
from tensorflow.python.framework import tensor_shape as _tensor_shape

from tensorflow.core.framework import op_def_pb2 as _op_def_pb2
# Needed to trigger the call to _set_call_cpp_shape_fn.
from tensorflow.python.framework import common_shapes as _common_shapes
from tensorflow.python.framework import op_def_registry as _op_def_registry
from tensorflow.python.framework import ops as _ops
from tensorflow.python.framework import op_def_library as _op_def_library
from tensorflow.python.util.tf_export import tf_export


_boosted_trees_calculate_best_gains_per_feature_outputs = ["node_ids_list",
                                                          "gains_list",
                                                          "thresholds_list",
                                                          "left_node_contribs_list",
                                                          "right_node_contribs_list"]
_BoostedTreesCalculateBestGainsPerFeatureOutput = _collections.namedtuple(
    "BoostedTreesCalculateBestGainsPerFeature",
    _boosted_trees_calculate_best_gains_per_feature_outputs)


def boosted_trees_calculate_best_gains_per_feature(node_id_range, stats_summary_list, l1, l2, tree_complexity, min_node_weight, max_splits, name=None):
  r"""Calculates gains for each feature and returns the best possible split information for the feature.

  The split information is the best threshold (bucket id), gains and left/right node contributions per node for each feature.

  It is possible that not all nodes can be split on each feature. Hence, the list of possible nodes can differ between the features. Therefore, we return `node_ids_list` for each feature, containing the list of nodes that this feature can be used to split.

  In this manner, the output is the best split per features and per node, so that it needs to be combined later to produce the best split for each node (among all possible features).

  The length of output lists are all of the same length, `num_features`.
  The output shapes are compatible in a way that the first dimension of all tensors of all lists are the same and equal to the number of possible split nodes for each feature.

  Args:
    node_id_range: A `Tensor` of type `int32`.
      A Rank 1 tensor (shape=[2]) to specify the range [first, last) of node ids to process within `stats_summary_list`. The nodes are iterated between the two nodes specified by the tensor, as like `for node_id in range(node_id_range[0], node_id_range[1])` (Note that the last index node_id_range[1] is exclusive).
    stats_summary_list: A list of at least 1 `Tensor` objects with type `float32`.
      A list of Rank 3 tensor (#shape=[max_splits, bucket, 2]) for accumulated stats summary (gradient/hessian) per node per buckets for each feature. The first dimension of the tensor is the maximum number of splits, and thus not all elements of it will be used, but only the indexes specified by node_ids will be used.
    l1: A `Tensor` of type `float32`.
      l1 regularization factor on leaf weights, per instance based.
    l2: A `Tensor` of type `float32`.
      l2 regularization factor on leaf weights, per instance based.
    tree_complexity: A `Tensor` of type `float32`.
      adjustment to the gain, per leaf based.
    min_node_weight: A `Tensor` of type `float32`.
      mininum avg of hessians in a node before required for the node to be considered for splitting.
    max_splits: An `int` that is `>= 1`.
      the number of nodes that can be split in the whole tree. Used as a dimension of output tensors.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (node_ids_list, gains_list, thresholds_list, left_node_contribs_list, right_node_contribs_list).

    node_ids_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `int32`.
    gains_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `float32`.
    thresholds_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `int32`.
    left_node_contribs_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `float32`.
    right_node_contribs_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `float32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(stats_summary_list, (list, tuple)):
      raise TypeError(
          "Expected list for 'stats_summary_list' argument to "
          "'boosted_trees_calculate_best_gains_per_feature' Op, not %r." % stats_summary_list)
    _attr_num_features = len(stats_summary_list)
    max_splits = _execute.make_int(max_splits, "max_splits")
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesCalculateBestGainsPerFeature",
        node_id_range=node_id_range, stats_summary_list=stats_summary_list,
        l1=l1, l2=l2, tree_complexity=tree_complexity,
        min_node_weight=min_node_weight, max_splits=max_splits, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("max_splits", _op.get_attr("max_splits"), "num_features",
              _op.get_attr("num_features"))
    _execute.record_gradient(
      "BoostedTreesCalculateBestGainsPerFeature", _inputs_flat, _attrs, _result, name)
    _result = [_result[:_attr_num_features]] + _result[_attr_num_features:]
    _result = _result[:1] + [_result[1:1 + _attr_num_features]] + _result[1 + _attr_num_features:]
    _result = _result[:2] + [_result[2:2 + _attr_num_features]] + _result[2 + _attr_num_features:]
    _result = _result[:3] + [_result[3:3 + _attr_num_features]] + _result[3 + _attr_num_features:]
    _result = _result[:4] + [_result[4:]]
    _result = _BoostedTreesCalculateBestGainsPerFeatureOutput._make(_result)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesCalculateBestGainsPerFeature", name,
        _ctx._post_execution_callbacks, node_id_range, stats_summary_list, l1,
        l2, tree_complexity, min_node_weight, "max_splits", max_splits)
      _result = _BoostedTreesCalculateBestGainsPerFeatureOutput._make(_result)
      return _result
    except _core._FallbackException:
      return boosted_trees_calculate_best_gains_per_feature_eager_fallback(
          node_id_range, stats_summary_list, l1, l2, tree_complexity,
          min_node_weight, max_splits=max_splits, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_calculate_best_gains_per_feature_eager_fallback(node_id_range, stats_summary_list, l1, l2, tree_complexity, min_node_weight, max_splits, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_calculate_best_gains_per_feature
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(stats_summary_list, (list, tuple)):
    raise TypeError(
        "Expected list for 'stats_summary_list' argument to "
        "'boosted_trees_calculate_best_gains_per_feature' Op, not %r." % stats_summary_list)
  _attr_num_features = len(stats_summary_list)
  max_splits = _execute.make_int(max_splits, "max_splits")
  node_id_range = _ops.convert_to_tensor(node_id_range, _dtypes.int32)
  stats_summary_list = _ops.convert_n_to_tensor(stats_summary_list, _dtypes.float32)
  l1 = _ops.convert_to_tensor(l1, _dtypes.float32)
  l2 = _ops.convert_to_tensor(l2, _dtypes.float32)
  tree_complexity = _ops.convert_to_tensor(tree_complexity, _dtypes.float32)
  min_node_weight = _ops.convert_to_tensor(min_node_weight, _dtypes.float32)
  _inputs_flat = [node_id_range] + list(stats_summary_list) + [l1, l2, tree_complexity, min_node_weight]
  _attrs = ("max_splits", max_splits, "num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesCalculateBestGainsPerFeature",
                             _attr_num_features + _attr_num_features +
                             _attr_num_features + _attr_num_features +
                             _attr_num_features, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BoostedTreesCalculateBestGainsPerFeature", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_features]] + _result[_attr_num_features:]
  _result = _result[:1] + [_result[1:1 + _attr_num_features]] + _result[1 + _attr_num_features:]
  _result = _result[:2] + [_result[2:2 + _attr_num_features]] + _result[2 + _attr_num_features:]
  _result = _result[:3] + [_result[3:3 + _attr_num_features]] + _result[3 + _attr_num_features:]
  _result = _result[:4] + [_result[4:]]
  _result = _BoostedTreesCalculateBestGainsPerFeatureOutput._make(_result)
  return _result


def boosted_trees_center_bias(tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2, name=None):
  r"""Calculates the prior from the training data (the bias) and fills in the first node with the logits' prior. Returns a boolean indicating whether to continue centering.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    mean_gradients: A `Tensor` of type `float32`.
      A tensor with shape=[logits_dimension] with mean of gradients for a first node.
    mean_hessians: A `Tensor` of type `float32`.
      A tensor with shape=[logits_dimension] mean of hessians for a first node.
    l1: A `Tensor` of type `float32`.
      l1 regularization factor on leaf weights, per instance based.
    l2: A `Tensor` of type `float32`.
      l2 regularization factor on leaf weights, per instance based.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesCenterBias", tree_ensemble_handle=tree_ensemble_handle,
        mean_gradients=mean_gradients, mean_hessians=mean_hessians, l1=l1,
        l2=l2, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = None
    _execute.record_gradient(
      "BoostedTreesCenterBias", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesCenterBias", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2)
      return _result
    except _core._FallbackException:
      return boosted_trees_center_bias_eager_fallback(
          tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2,
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_center_bias_eager_fallback(tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_center_bias
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  mean_gradients = _ops.convert_to_tensor(mean_gradients, _dtypes.float32)
  mean_hessians = _ops.convert_to_tensor(mean_hessians, _dtypes.float32)
  l1 = _ops.convert_to_tensor(l1, _dtypes.float32)
  l2 = _ops.convert_to_tensor(l2, _dtypes.float32)
  _inputs_flat = [tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesCenterBias", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesCenterBias", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def boosted_trees_create_ensemble(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None):
  r"""Creates a tree ensemble model and returns a handle to it.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble resource to be created.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the initial value of the resource stamp.
    tree_ensemble_serialized: A `Tensor` of type `string`.
      Serialized proto of the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesCreateEnsemble",
        tree_ensemble_handle=tree_ensemble_handle, stamp_token=stamp_token,
        tree_ensemble_serialized=tree_ensemble_serialized, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesCreateEnsemble", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token, tree_ensemble_serialized)
      return _result
    except _core._FallbackException:
      return boosted_trees_create_ensemble_eager_fallback(
          tree_ensemble_handle, stamp_token, tree_ensemble_serialized,
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_create_ensemble_eager_fallback(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_create_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  tree_ensemble_serialized = _ops.convert_to_tensor(tree_ensemble_serialized, _dtypes.string)
  _inputs_flat = [tree_ensemble_handle, stamp_token, tree_ensemble_serialized]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesCreateEnsemble", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_deserialize_ensemble(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None):
  r"""Deserializes a serialized tree ensemble config and replaces current tree

  ensemble.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the new value of the resource stamp.
    tree_ensemble_serialized: A `Tensor` of type `string`.
      Serialized proto of the ensemble.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesDeserializeEnsemble",
        tree_ensemble_handle=tree_ensemble_handle, stamp_token=stamp_token,
        tree_ensemble_serialized=tree_ensemble_serialized, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesDeserializeEnsemble", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle, stamp_token,
        tree_ensemble_serialized)
      return _result
    except _core._FallbackException:
      return boosted_trees_deserialize_ensemble_eager_fallback(
          tree_ensemble_handle, stamp_token, tree_ensemble_serialized,
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_deserialize_ensemble_eager_fallback(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_deserialize_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  tree_ensemble_serialized = _ops.convert_to_tensor(tree_ensemble_serialized, _dtypes.string)
  _inputs_flat = [tree_ensemble_handle, stamp_token, tree_ensemble_serialized]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesDeserializeEnsemble", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_ensemble_resource_handle_op(container="", shared_name="", name=None):
  r"""Creates a handle to a BoostedTreesEnsembleResource

  Args:
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if container is None:
      container = ""
    container = _execute.make_str(container, "container")
    if shared_name is None:
      shared_name = ""
    shared_name = _execute.make_str(shared_name, "shared_name")
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesEnsembleResourceHandleOp", container=container,
        shared_name=shared_name, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("container", _op.get_attr("container"), "shared_name",
              _op.get_attr("shared_name"))
    _execute.record_gradient(
      "BoostedTreesEnsembleResourceHandleOp", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesEnsembleResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      return boosted_trees_ensemble_resource_handle_op_eager_fallback(
          container=container, shared_name=shared_name, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_ensemble_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_ensemble_resource_handle_op
  """
  _ctx = ctx if ctx else _context.context()
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name)
  _result = _execute.execute(b"BoostedTreesEnsembleResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesEnsembleResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def boosted_trees_example_debug_outputs(tree_ensemble_handle, bucketized_features, logits_dimension, name=None):
  r"""Debugging/model interpretability outputs for each example.

  It traverses all the trees and computes debug metrics for individual examples, 
  such as getting split feature ids and logits after each split along the decision
  path used to compute directional feature contributions.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
    bucketized_features: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors containing bucket id for each
      feature.
    logits_dimension: An `int`.
      scalar, dimension of the logits, to be used for constructing the protos in
      examples_debug_outputs_serialized.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(bucketized_features, (list, tuple)):
      raise TypeError(
          "Expected list for 'bucketized_features' argument to "
          "'boosted_trees_example_debug_outputs' Op, not %r." % bucketized_features)
    _attr_num_bucketized_features = len(bucketized_features)
    logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesExampleDebugOutputs",
        tree_ensemble_handle=tree_ensemble_handle,
        bucketized_features=bucketized_features,
        logits_dimension=logits_dimension, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("num_bucketized_features",
              _op.get_attr("num_bucketized_features"), "logits_dimension",
              _op.get_attr("logits_dimension"))
    _execute.record_gradient(
      "BoostedTreesExampleDebugOutputs", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesExampleDebugOutputs", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle,
        bucketized_features, "logits_dimension", logits_dimension)
      return _result
    except _core._FallbackException:
      return boosted_trees_example_debug_outputs_eager_fallback(
          tree_ensemble_handle, bucketized_features,
          logits_dimension=logits_dimension, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_example_debug_outputs_eager_fallback(tree_ensemble_handle, bucketized_features, logits_dimension, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_example_debug_outputs
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_example_debug_outputs' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  bucketized_features = _ops.convert_n_to_tensor(bucketized_features, _dtypes.int32)
  _inputs_flat = [tree_ensemble_handle] + list(bucketized_features)
  _attrs = ("num_bucketized_features", _attr_num_bucketized_features,
  "logits_dimension", logits_dimension)
  _result = _execute.execute(b"BoostedTreesExampleDebugOutputs", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesExampleDebugOutputs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_boosted_trees_get_ensemble_states_outputs = ["stamp_token", "num_trees",
                                             "num_finalized_trees",
                                             "num_attempted_layers",
                                             "last_layer_nodes_range"]
_BoostedTreesGetEnsembleStatesOutput = _collections.namedtuple(
    "BoostedTreesGetEnsembleStates",
    _boosted_trees_get_ensemble_states_outputs)


def boosted_trees_get_ensemble_states(tree_ensemble_handle, name=None):
  r"""Retrieves the tree ensemble resource stamp token, number of trees and growing statistics.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, num_trees, num_finalized_trees, num_attempted_layers, last_layer_nodes_range).

    stamp_token: A `Tensor` of type `int64`.
    num_trees: A `Tensor` of type `int32`.
    num_finalized_trees: A `Tensor` of type `int32`.
    num_attempted_layers: A `Tensor` of type `int32`.
    last_layer_nodes_range: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesGetEnsembleStates",
        tree_ensemble_handle=tree_ensemble_handle, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = None
    _execute.record_gradient(
      "BoostedTreesGetEnsembleStates", _inputs_flat, _attrs, _result, name)
    _result = _BoostedTreesGetEnsembleStatesOutput._make(_result)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesGetEnsembleStates", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle)
      _result = _BoostedTreesGetEnsembleStatesOutput._make(_result)
      return _result
    except _core._FallbackException:
      return boosted_trees_get_ensemble_states_eager_fallback(
          tree_ensemble_handle, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_get_ensemble_states_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_get_ensemble_states
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesGetEnsembleStates", 5,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesGetEnsembleStates", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesGetEnsembleStatesOutput._make(_result)
  return _result


def boosted_trees_make_stats_summary(node_ids, gradients, hessians, bucketized_features_list, max_splits, num_buckets, name=None):
  r"""Makes the summary of accumulated stats for the batch.

  The summary stats contains gradients and hessians accumulated into the corresponding node and bucket for each example.

  Args:
    node_ids: A `Tensor` of type `int32`.
      int32 Rank 1 Tensor containing node ids, which each example falls into for the requested layer.
    gradients: A `Tensor` of type `float32`.
      float32; Rank 2 Tensor (shape=[#examples, 1]) for gradients.
    hessians: A `Tensor` of type `float32`.
      float32; Rank 2 Tensor (shape=[#examples, 1]) for hessians.
    bucketized_features_list: A list of at least 1 `Tensor` objects with type `int32`.
      int32 list of Rank 1 Tensors, each containing the bucketized feature (for each feature column).
    max_splits: An `int` that is `>= 1`.
      int; the maximum number of splits possible in the whole tree.
    num_buckets: An `int` that is `>= 1`.
      int; equals to the maximum possible value of bucketized feature.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(bucketized_features_list, (list, tuple)):
      raise TypeError(
          "Expected list for 'bucketized_features_list' argument to "
          "'boosted_trees_make_stats_summary' Op, not %r." % bucketized_features_list)
    _attr_num_features = len(bucketized_features_list)
    max_splits = _execute.make_int(max_splits, "max_splits")
    num_buckets = _execute.make_int(num_buckets, "num_buckets")
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesMakeStatsSummary", node_ids=node_ids,
        gradients=gradients, hessians=hessians,
        bucketized_features_list=bucketized_features_list,
        max_splits=max_splits, num_buckets=num_buckets, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("max_splits", _op.get_attr("max_splits"), "num_buckets",
              _op.get_attr("num_buckets"), "num_features",
              _op.get_attr("num_features"))
    _execute.record_gradient(
      "BoostedTreesMakeStatsSummary", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesMakeStatsSummary", name, _ctx._post_execution_callbacks,
        node_ids, gradients, hessians, bucketized_features_list, "max_splits",
        max_splits, "num_buckets", num_buckets)
      return _result
    except _core._FallbackException:
      return boosted_trees_make_stats_summary_eager_fallback(
          node_ids, gradients, hessians, bucketized_features_list,
          max_splits=max_splits, num_buckets=num_buckets, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_make_stats_summary_eager_fallback(node_ids, gradients, hessians, bucketized_features_list, max_splits, num_buckets, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_make_stats_summary
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features_list, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features_list' argument to "
        "'boosted_trees_make_stats_summary' Op, not %r." % bucketized_features_list)
  _attr_num_features = len(bucketized_features_list)
  max_splits = _execute.make_int(max_splits, "max_splits")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  node_ids = _ops.convert_to_tensor(node_ids, _dtypes.int32)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  bucketized_features_list = _ops.convert_n_to_tensor(bucketized_features_list, _dtypes.int32)
  _inputs_flat = [node_ids, gradients, hessians] + list(bucketized_features_list)
  _attrs = ("max_splits", max_splits, "num_buckets", num_buckets,
  "num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesMakeStatsSummary", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesMakeStatsSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def boosted_trees_predict(tree_ensemble_handle, bucketized_features, logits_dimension, name=None):
  r"""Runs multiple additive regression ensemble predictors on input instances and

  computes the logits. It is designed to be used during prediction.
  It traverses all the trees and calculates the final score for each instance.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
    bucketized_features: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors containing bucket id for each
      feature.
    logits_dimension: An `int`.
      scalar, dimension of the logits, to be used for partial logits
      shape.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(bucketized_features, (list, tuple)):
      raise TypeError(
          "Expected list for 'bucketized_features' argument to "
          "'boosted_trees_predict' Op, not %r." % bucketized_features)
    _attr_num_bucketized_features = len(bucketized_features)
    logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesPredict", tree_ensemble_handle=tree_ensemble_handle,
        bucketized_features=bucketized_features,
        logits_dimension=logits_dimension, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("num_bucketized_features",
              _op.get_attr("num_bucketized_features"), "logits_dimension",
              _op.get_attr("logits_dimension"))
    _execute.record_gradient(
      "BoostedTreesPredict", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesPredict", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, bucketized_features, "logits_dimension",
        logits_dimension)
      return _result
    except _core._FallbackException:
      return boosted_trees_predict_eager_fallback(
          tree_ensemble_handle, bucketized_features,
          logits_dimension=logits_dimension, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_predict_eager_fallback(tree_ensemble_handle, bucketized_features, logits_dimension, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_predict
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_predict' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  bucketized_features = _ops.convert_n_to_tensor(bucketized_features, _dtypes.int32)
  _inputs_flat = [tree_ensemble_handle] + list(bucketized_features)
  _attrs = ("num_bucketized_features", _attr_num_bucketized_features,
  "logits_dimension", logits_dimension)
  _result = _execute.execute(b"BoostedTreesPredict", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BoostedTreesPredict", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_boosted_trees_serialize_ensemble_outputs = ["stamp_token",
                                            "tree_ensemble_serialized"]
_BoostedTreesSerializeEnsembleOutput = _collections.namedtuple(
    "BoostedTreesSerializeEnsemble",
    _boosted_trees_serialize_ensemble_outputs)


def boosted_trees_serialize_ensemble(tree_ensemble_handle, name=None):
  r"""Serializes the tree ensemble to a proto.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, tree_ensemble_serialized).

    stamp_token: A `Tensor` of type `int64`.
    tree_ensemble_serialized: A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesSerializeEnsemble",
        tree_ensemble_handle=tree_ensemble_handle, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = None
    _execute.record_gradient(
      "BoostedTreesSerializeEnsemble", _inputs_flat, _attrs, _result, name)
    _result = _BoostedTreesSerializeEnsembleOutput._make(_result)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesSerializeEnsemble", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle)
      _result = _BoostedTreesSerializeEnsembleOutput._make(_result)
      return _result
    except _core._FallbackException:
      return boosted_trees_serialize_ensemble_eager_fallback(
          tree_ensemble_handle, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_serialize_ensemble_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_serialize_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesSerializeEnsemble", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesSerializeEnsemble", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesSerializeEnsembleOutput._make(_result)
  return _result


_boosted_trees_training_predict_outputs = ["partial_logits", "tree_ids",
                                          "node_ids"]
_BoostedTreesTrainingPredictOutput = _collections.namedtuple(
    "BoostedTreesTrainingPredict", _boosted_trees_training_predict_outputs)


def boosted_trees_training_predict(tree_ensemble_handle, cached_tree_ids, cached_node_ids, bucketized_features, logits_dimension, name=None):
  r"""Runs multiple additive regression ensemble predictors on input instances and

  computes the update to cached logits. It is designed to be used during training.
  It traverses the trees starting from cached tree id and cached node id and
  calculates the updates to be pushed to the cache.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
    cached_tree_ids: A `Tensor` of type `int32`.
      Rank 1 Tensor containing cached tree ids which is the starting
      tree of prediction.
    cached_node_ids: A `Tensor` of type `int32`.
      Rank 1 Tensor containing cached node id which is the starting
      node of prediction.
    bucketized_features: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors containing bucket id for each
      feature.
    logits_dimension: An `int`.
      scalar, dimension of the logits, to be used for partial logits
      shape.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (partial_logits, tree_ids, node_ids).

    partial_logits: A `Tensor` of type `float32`.
    tree_ids: A `Tensor` of type `int32`.
    node_ids: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(bucketized_features, (list, tuple)):
      raise TypeError(
          "Expected list for 'bucketized_features' argument to "
          "'boosted_trees_training_predict' Op, not %r." % bucketized_features)
    _attr_num_bucketized_features = len(bucketized_features)
    logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesTrainingPredict",
        tree_ensemble_handle=tree_ensemble_handle,
        cached_tree_ids=cached_tree_ids, cached_node_ids=cached_node_ids,
        bucketized_features=bucketized_features,
        logits_dimension=logits_dimension, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("num_bucketized_features",
              _op.get_attr("num_bucketized_features"), "logits_dimension",
              _op.get_attr("logits_dimension"))
    _execute.record_gradient(
      "BoostedTreesTrainingPredict", _inputs_flat, _attrs, _result, name)
    _result = _BoostedTreesTrainingPredictOutput._make(_result)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesTrainingPredict", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, cached_tree_ids, cached_node_ids,
        bucketized_features, "logits_dimension", logits_dimension)
      _result = _BoostedTreesTrainingPredictOutput._make(_result)
      return _result
    except _core._FallbackException:
      return boosted_trees_training_predict_eager_fallback(
          tree_ensemble_handle, cached_tree_ids, cached_node_ids,
          bucketized_features, logits_dimension=logits_dimension, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_training_predict_eager_fallback(tree_ensemble_handle, cached_tree_ids, cached_node_ids, bucketized_features, logits_dimension, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_training_predict
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_training_predict' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  cached_tree_ids = _ops.convert_to_tensor(cached_tree_ids, _dtypes.int32)
  cached_node_ids = _ops.convert_to_tensor(cached_node_ids, _dtypes.int32)
  bucketized_features = _ops.convert_n_to_tensor(bucketized_features, _dtypes.int32)
  _inputs_flat = [tree_ensemble_handle, cached_tree_ids, cached_node_ids] + list(bucketized_features)
  _attrs = ("num_bucketized_features", _attr_num_bucketized_features,
  "logits_dimension", logits_dimension)
  _result = _execute.execute(b"BoostedTreesTrainingPredict", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesTrainingPredict", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesTrainingPredictOutput._make(_result)
  return _result


def boosted_trees_update_ensemble(tree_ensemble_handle, feature_ids, node_ids, gains, thresholds, left_node_contribs, right_node_contribs, max_depth, learning_rate, pruning_mode, name=None):
  r"""Updates the tree ensemble by either adding a layer to the last tree being grown

  or by starting a new tree.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the ensemble variable.
    feature_ids: A `Tensor` of type `int32`.
      Rank 1 tensor with ids for each feature. This is the real id of
      the feature that will be used in the split.
    node_ids: A list of `Tensor` objects with type `int32`.
      List of rank 1 tensors representing the nodes for which this feature
      has a split.
    gains: A list with the same length as `node_ids` of `Tensor` objects with type `float32`.
      List of rank 1 tensors representing the gains for each of the feature's
      split.
    thresholds: A list with the same length as `node_ids` of `Tensor` objects with type `int32`.
      List of rank 1 tensors representing the thesholds for each of the
      feature's split.
    left_node_contribs: A list with the same length as `node_ids` of `Tensor` objects with type `float32`.
      List of rank 2 tensors with left leaf contribs for each of
      the feature's splits. Will be added to the previous node values to constitute
      the values of the left nodes.
    right_node_contribs: A list with the same length as `node_ids` of `Tensor` objects with type `float32`.
      List of rank 2 tensors with right leaf contribs for each
      of the feature's splits. Will be added to the previous node values to constitute
      the values of the right nodes.
    max_depth: A `Tensor` of type `int32`. Max depth of the tree to build.
    learning_rate: A `Tensor` of type `float32`.
      shrinkage const for each new tree.
    pruning_mode: An `int` that is `>= 0`.
      0-No pruning, 1-Pre-pruning, 2-Post-pruning.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(node_ids, (list, tuple)):
      raise TypeError(
          "Expected list for 'node_ids' argument to "
          "'boosted_trees_update_ensemble' Op, not %r." % node_ids)
    _attr_num_features = len(node_ids)
    if not isinstance(gains, (list, tuple)):
      raise TypeError(
          "Expected list for 'gains' argument to "
          "'boosted_trees_update_ensemble' Op, not %r." % gains)
    if len(gains) != _attr_num_features:
      raise ValueError(
          "List argument 'gains' to 'boosted_trees_update_ensemble' Op with length %d "
          "must match length %d of argument 'node_ids'." %
          (len(gains), _attr_num_features))
    if not isinstance(thresholds, (list, tuple)):
      raise TypeError(
          "Expected list for 'thresholds' argument to "
          "'boosted_trees_update_ensemble' Op, not %r." % thresholds)
    if len(thresholds) != _attr_num_features:
      raise ValueError(
          "List argument 'thresholds' to 'boosted_trees_update_ensemble' Op with length %d "
          "must match length %d of argument 'node_ids'." %
          (len(thresholds), _attr_num_features))
    if not isinstance(left_node_contribs, (list, tuple)):
      raise TypeError(
          "Expected list for 'left_node_contribs' argument to "
          "'boosted_trees_update_ensemble' Op, not %r." % left_node_contribs)
    if len(left_node_contribs) != _attr_num_features:
      raise ValueError(
          "List argument 'left_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
          "must match length %d of argument 'node_ids'." %
          (len(left_node_contribs), _attr_num_features))
    if not isinstance(right_node_contribs, (list, tuple)):
      raise TypeError(
          "Expected list for 'right_node_contribs' argument to "
          "'boosted_trees_update_ensemble' Op, not %r." % right_node_contribs)
    if len(right_node_contribs) != _attr_num_features:
      raise ValueError(
          "List argument 'right_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
          "must match length %d of argument 'node_ids'." %
          (len(right_node_contribs), _attr_num_features))
    pruning_mode = _execute.make_int(pruning_mode, "pruning_mode")
    _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesUpdateEnsemble",
        tree_ensemble_handle=tree_ensemble_handle, feature_ids=feature_ids,
        node_ids=node_ids, gains=gains, thresholds=thresholds,
        left_node_contribs=left_node_contribs,
        right_node_contribs=right_node_contribs, max_depth=max_depth,
        learning_rate=learning_rate, pruning_mode=pruning_mode, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesUpdateEnsemble", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, feature_ids, node_ids, gains, thresholds,
        left_node_contribs, right_node_contribs, max_depth, learning_rate,
        "pruning_mode", pruning_mode)
      return _result
    except _core._FallbackException:
      return boosted_trees_update_ensemble_eager_fallback(
          tree_ensemble_handle, feature_ids, node_ids, gains, thresholds,
          left_node_contribs, right_node_contribs, max_depth, learning_rate,
          pruning_mode=pruning_mode, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def boosted_trees_update_ensemble_eager_fallback(tree_ensemble_handle, feature_ids, node_ids, gains, thresholds, left_node_contribs, right_node_contribs, max_depth, learning_rate, pruning_mode, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_update_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(node_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'node_ids' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % node_ids)
  _attr_num_features = len(node_ids)
  if not isinstance(gains, (list, tuple)):
    raise TypeError(
        "Expected list for 'gains' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % gains)
  if len(gains) != _attr_num_features:
    raise ValueError(
        "List argument 'gains' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(gains), _attr_num_features))
  if not isinstance(thresholds, (list, tuple)):
    raise TypeError(
        "Expected list for 'thresholds' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % thresholds)
  if len(thresholds) != _attr_num_features:
    raise ValueError(
        "List argument 'thresholds' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(thresholds), _attr_num_features))
  if not isinstance(left_node_contribs, (list, tuple)):
    raise TypeError(
        "Expected list for 'left_node_contribs' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % left_node_contribs)
  if len(left_node_contribs) != _attr_num_features:
    raise ValueError(
        "List argument 'left_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(left_node_contribs), _attr_num_features))
  if not isinstance(right_node_contribs, (list, tuple)):
    raise TypeError(
        "Expected list for 'right_node_contribs' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % right_node_contribs)
  if len(right_node_contribs) != _attr_num_features:
    raise ValueError(
        "List argument 'right_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(right_node_contribs), _attr_num_features))
  pruning_mode = _execute.make_int(pruning_mode, "pruning_mode")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  feature_ids = _ops.convert_to_tensor(feature_ids, _dtypes.int32)
  node_ids = _ops.convert_n_to_tensor(node_ids, _dtypes.int32)
  gains = _ops.convert_n_to_tensor(gains, _dtypes.float32)
  thresholds = _ops.convert_n_to_tensor(thresholds, _dtypes.int32)
  left_node_contribs = _ops.convert_n_to_tensor(left_node_contribs, _dtypes.float32)
  right_node_contribs = _ops.convert_n_to_tensor(right_node_contribs, _dtypes.float32)
  max_depth = _ops.convert_to_tensor(max_depth, _dtypes.int32)
  learning_rate = _ops.convert_to_tensor(learning_rate, _dtypes.float32)
  _inputs_flat = [tree_ensemble_handle, feature_ids] + list(node_ids) + list(gains) + list(thresholds) + list(left_node_contribs) + list(right_node_contribs) + [max_depth, learning_rate]
  _attrs = ("pruning_mode", pruning_mode, "num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesUpdateEnsemble", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def is_boosted_trees_ensemble_initialized(tree_ensemble_handle, name=None):
  r"""Checks whether a tree ensemble has been initialized.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble resouce.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "IsBoostedTreesEnsembleInitialized",
        tree_ensemble_handle=tree_ensemble_handle, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = None
    _execute.record_gradient(
      "IsBoostedTreesEnsembleInitialized", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IsBoostedTreesEnsembleInitialized", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle)
      return _result
    except _core._FallbackException:
      return is_boosted_trees_ensemble_initialized_eager_fallback(
          tree_ensemble_handle, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def is_boosted_trees_ensemble_initialized_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function is_boosted_trees_ensemble_initialized
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"IsBoostedTreesEnsembleInitialized", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IsBoostedTreesEnsembleInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BoostedTreesCalculateBestGainsPerFeature"
#   input_arg {
#     name: "node_id_range"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "stats_summary_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "l1"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l2"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_complexity"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_node_weight"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "node_ids_list"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "gains_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "thresholds_list"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "left_node_contribs_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "right_node_contribs_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   attr {
#     name: "max_splits"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "BoostedTreesCenterBias"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mean_gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "mean_hessians"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l1"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l2"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "continue_centering"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesCreateEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "tree_ensemble_serialized"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesDeserializeEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "tree_ensemble_serialized"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesEnsembleResourceHandleOp"
#   output_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "container"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesExampleDebugOutputs"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "bucketized_features"
#     type: DT_INT32
#     number_attr: "num_bucketized_features"
#   }
#   output_arg {
#     name: "examples_debug_outputs_serialized"
#     type: DT_STRING
#   }
#   attr {
#     name: "num_bucketized_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "logits_dimension"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesGetEnsembleStates"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_trees"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "num_finalized_trees"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "num_attempted_layers"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "last_layer_nodes_range"
#     type: DT_INT32
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesMakeStatsSummary"
#   input_arg {
#     name: "node_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "bucketized_features_list"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "stats_summary"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "max_splits"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "BoostedTreesPredict"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "bucketized_features"
#     type: DT_INT32
#     number_attr: "num_bucketized_features"
#   }
#   output_arg {
#     name: "logits"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_bucketized_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "logits_dimension"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesSerializeEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "tree_ensemble_serialized"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesTrainingPredict"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "cached_tree_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "cached_node_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "bucketized_features"
#     type: DT_INT32
#     number_attr: "num_bucketized_features"
#   }
#   output_arg {
#     name: "partial_logits"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "tree_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "node_ids"
#     type: DT_INT32
#   }
#   attr {
#     name: "num_bucketized_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "logits_dimension"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesUpdateEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "node_ids"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "gains"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "thresholds"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "left_node_contribs"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "right_node_contribs"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "max_depth"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "learning_rate"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "pruning_mode"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "IsBoostedTreesEnsembleInitialized"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\206\003\n(BoostedTreesCalculateBestGainsPerFeature\022\021\n\rnode_id_range\030\003\022$\n\022stats_summary_list\030\001*\014num_features\022\006\n\002l1\030\001\022\006\n\002l2\030\001\022\023\n\017tree_complexity\030\001\022\023\n\017min_node_weight\030\001\032\037\n\rnode_ids_list\030\003*\014num_features\032\034\n\ngains_list\030\001*\014num_features\032!\n\017thresholds_list\030\003*\014num_features\032)\n\027left_node_contribs_list\030\001*\014num_features\032*\n\030right_node_contribs_list\030\001*\014num_features\"\025\n\nmax_splits\022\003int(\0010\001\"\027\n\014num_features\022\003int(\0010\001\n\204\001\n\026BoostedTreesCenterBias\022\030\n\024tree_ensemble_handle\030\024\022\022\n\016mean_gradients\030\001\022\021\n\rmean_hessians\030\001\022\006\n\002l1\030\001\022\006\n\002l2\030\001\032\026\n\022continue_centering\030\n\210\001\001\nh\n\032BoostedTreesCreateEnsemble\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\034\n\030tree_ensemble_serialized\030\007\210\001\001\nm\n\037BoostedTreesDeserializeEnsemble\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\034\n\030tree_ensemble_serialized\030\007\210\001\001\nk\n$BoostedTreesEnsembleResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\324\001\n\037BoostedTreesExampleDebugOutputs\022\030\n\024tree_ensemble_handle\030\024\0220\n\023bucketized_features\030\003*\027num_bucketized_features\032%\n!examples_debug_outputs_serialized\030\007\"\"\n\027num_bucketized_features\022\003int(\0010\001\"\027\n\020logits_dimension\022\003int\210\001\001\n\253\001\n\035BoostedTreesGetEnsembleStates\022\030\n\024tree_ensemble_handle\030\024\032\017\n\013stamp_token\030\t\032\r\n\tnum_trees\030\003\032\027\n\023num_finalized_trees\030\003\032\030\n\024num_attempted_layers\030\003\032\032\n\026last_layer_nodes_range\030\003\210\001\001\n\320\001\n\034BoostedTreesMakeStatsSummary\022\014\n\010node_ids\030\003\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\022*\n\030bucketized_features_list\030\003*\014num_features\032\021\n\rstats_summary\030\001\"\025\n\nmax_splits\022\003int(\0010\001\"\026\n\013num_buckets\022\003int(\0010\001\"\027\n\014num_features\022\003int(\0010\001\n\255\001\n\023BoostedTreesPredict\022\030\n\024tree_ensemble_handle\030\024\0220\n\023bucketized_features\030\003*\027num_bucketized_features\032\n\n\006logits\030\001\"\"\n\027num_bucketized_features\022\003int(\0010\001\"\027\n\020logits_dimension\022\003int\210\001\001\nk\n\035BoostedTreesSerializeEnsemble\022\030\n\024tree_ensemble_handle\030\024\032\017\n\013stamp_token\030\t\032\034\n\030tree_ensemble_serialized\030\007\210\001\001\n\203\002\n\033BoostedTreesTrainingPredict\022\030\n\024tree_ensemble_handle\030\024\022\023\n\017cached_tree_ids\030\003\022\023\n\017cached_node_ids\030\003\0220\n\023bucketized_features\030\003*\027num_bucketized_features\032\022\n\016partial_logits\030\001\032\014\n\010tree_ids\030\003\032\014\n\010node_ids\030\003\"\"\n\027num_bucketized_features\022\003int(\0010\001\"\027\n\020logits_dimension\022\003int\210\001\001\n\272\002\n\032BoostedTreesUpdateEnsemble\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013feature_ids\030\003\022\032\n\010node_ids\030\003*\014num_features\022\027\n\005gains\030\001*\014num_features\022\034\n\nthresholds\030\003*\014num_features\022$\n\022left_node_contribs\030\001*\014num_features\022%\n\023right_node_contribs\030\001*\014num_features\022\r\n\tmax_depth\030\003\022\021\n\rlearning_rate\030\001\"\025\n\014pruning_mode\022\003int(\001\"\025\n\014num_features\022\003int(\001\210\001\001\nT\n!IsBoostedTreesEnsembleInitialized\022\030\n\024tree_ensemble_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001")
