"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: tpu_ops.cc
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


@tf_export('configure_distributed_tpu')
def configure_distributed_tpu(embedding_config="", tpu_embedding_config="", is_global_init=False, name=None):
  r"""An op that sets up the centralized structures for a distributed TPU

  system.

  Args:
    embedding_config: An optional `string`. Defaults to `""`.
      Reserved. Do not use.
    tpu_embedding_config: An optional `string`. Defaults to `""`.
      Serialized tensorflow.tpu.TPUEmbeddingConfiguration that
      describes the embedding lookups of the program.
    is_global_init: An optional `bool`. Defaults to `False`.
      Reserved. Do not use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
    A serialized tensorflow.tpu.TopologyProto that describes the TPU
    topology.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if embedding_config is None:
      embedding_config = ""
    embedding_config = _execute.make_str(embedding_config, "embedding_config")
    if tpu_embedding_config is None:
      tpu_embedding_config = ""
    tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
    if is_global_init is None:
      is_global_init = False
    is_global_init = _execute.make_bool(is_global_init, "is_global_init")
    _, _, _op = _op_def_lib._apply_op_helper(
        "ConfigureDistributedTPU", embedding_config=embedding_config,
        tpu_embedding_config=tpu_embedding_config,
        is_global_init=is_global_init, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("embedding_config", _op.get_attr("embedding_config"),
              "tpu_embedding_config", _op.get_attr("tpu_embedding_config"),
              "is_global_init", _op.get_attr("is_global_init"))
    _execute.record_gradient(
      "ConfigureDistributedTPU", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ConfigureDistributedTPU", name, _ctx._post_execution_callbacks,
        "embedding_config", embedding_config, "tpu_embedding_config",
        tpu_embedding_config, "is_global_init", is_global_init)
      return _result
    except _core._FallbackException:
      return configure_distributed_tpu_eager_fallback(
          embedding_config=embedding_config,
          tpu_embedding_config=tpu_embedding_config,
          is_global_init=is_global_init, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def configure_distributed_tpu_eager_fallback(embedding_config="", tpu_embedding_config="", is_global_init=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function configure_distributed_tpu
  """
  _ctx = ctx if ctx else _context.context()
  if embedding_config is None:
    embedding_config = ""
  embedding_config = _execute.make_str(embedding_config, "embedding_config")
  if tpu_embedding_config is None:
    tpu_embedding_config = ""
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  if is_global_init is None:
    is_global_init = False
  is_global_init = _execute.make_bool(is_global_init, "is_global_init")
  _inputs_flat = []
  _attrs = ("embedding_config", embedding_config, "tpu_embedding_config",
  tpu_embedding_config, "is_global_init", is_global_init)
  _result = _execute.execute(b"ConfigureDistributedTPU", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ConfigureDistributedTPU", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ConfigureDistributedTPU")(None)


@tf_export('cross_replica_sum')
def cross_replica_sum(input, group_assignment=[], name=None):
  r"""An Op to sum inputs across replicated TPU instances. Each

  instance supplies its own input. If group_assignment is empty, the output of
  each is the sum of all the inputs, otherwise the output of each is the sum of
  the inputs belonging to the same group.

  For example, suppose there are 4 TPU instances: `[A, B, C, D]`. Passing
  group_assignment=`[0,1,0,1]` sets `A, C` as group 0, and `B, D` as group 1.
  Thus we get the outputs: `[A+C, B+D, A+C, B+D]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `float32`.
      The local input to the sum.
    group_assignment: An optional list of `ints`. Defaults to `[]`.
      The list of group ids. `group_assignment[i]` represents the
      group id of replica i.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
    The sum of all the distributed inputs.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if group_assignment is None:
      group_assignment = []
    if not isinstance(group_assignment, (list, tuple)):
      raise TypeError(
          "Expected list for 'group_assignment' argument to "
          "'cross_replica_sum' Op, not %r." % group_assignment)
    group_assignment = [_execute.make_int(_i, "group_assignment") for _i in group_assignment]
    _, _, _op = _op_def_lib._apply_op_helper(
        "CrossReplicaSum", input=input, group_assignment=group_assignment,
        name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("T", _op.get_attr("T"), "group_assignment",
              _op.get_attr("group_assignment"))
    _execute.record_gradient(
      "CrossReplicaSum", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CrossReplicaSum", name, _ctx._post_execution_callbacks, input,
        "group_assignment", group_assignment)
      return _result
    except _core._FallbackException:
      return cross_replica_sum_eager_fallback(
          input, group_assignment=group_assignment, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def cross_replica_sum_eager_fallback(input, group_assignment=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cross_replica_sum
  """
  _ctx = ctx if ctx else _context.context()
  if group_assignment is None:
    group_assignment = []
  if not isinstance(group_assignment, (list, tuple)):
    raise TypeError(
        "Expected list for 'group_assignment' argument to "
        "'cross_replica_sum' Op, not %r." % group_assignment)
  group_assignment = [_execute.make_int(_i, "group_assignment") for _i in group_assignment]
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "group_assignment", group_assignment)
  _result = _execute.execute(b"CrossReplicaSum", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CrossReplicaSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("CrossReplicaSum")(None)


@tf_export('infeed_dequeue')
def infeed_dequeue(dtype, shape, name=None):
  r"""A placeholder op for a value that will be fed into the computation.

  Args:
    dtype: A `tf.DType`. The type of elements in the tensor.
    shape: A `tf.TensorShape` or list of `ints`. The shape of the tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
    A tensor that will be provided using the infeed mechanism.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    dtype = _execute.make_type(dtype, "dtype")
    shape = _execute.make_shape(shape, "shape")
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedDequeue", dtype=dtype, shape=shape, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"))
    _execute.record_gradient(
      "InfeedDequeue", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedDequeue", name, _ctx._post_execution_callbacks, "dtype", dtype,
        "shape", shape)
      return _result
    except _core._FallbackException:
      return infeed_dequeue_eager_fallback(
          dtype=dtype, shape=shape, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def infeed_dequeue_eager_fallback(dtype, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_dequeue
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape)
  _result = _execute.execute(b"InfeedDequeue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InfeedDequeue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("InfeedDequeue")(None)


@tf_export('infeed_dequeue_tuple')
def infeed_dequeue_tuple(dtypes, shapes, name=None):
  r"""A placeholder op for multiple values that will be fed into the computation

  simultaneously as an XLA tuple.

  Args:
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
      The element types of each element in `outputs`.
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shapes of each tensor in `outputs`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
    A list of tensors that will be provided using the infeed mechanism.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(dtypes, (list, tuple)):
      raise TypeError(
          "Expected list for 'dtypes' argument to "
          "'infeed_dequeue_tuple' Op, not %r." % dtypes)
    dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
    if not isinstance(shapes, (list, tuple)):
      raise TypeError(
          "Expected list for 'shapes' argument to "
          "'infeed_dequeue_tuple' Op, not %r." % shapes)
    shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedDequeueTuple", dtypes=dtypes, shapes=shapes, name=name)
    _result = _op.outputs[:]
    if not _result:
      return _op
    _inputs_flat = _op.inputs
    _attrs = ("dtypes", _op.get_attr("dtypes"), "shapes",
              _op.get_attr("shapes"))
    _execute.record_gradient(
      "InfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedDequeueTuple", name, _ctx._post_execution_callbacks, "dtypes",
        dtypes, "shapes", shapes)
      return _result
    except _core._FallbackException:
      return infeed_dequeue_tuple_eager_fallback(
          dtypes=dtypes, shapes=shapes, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def infeed_dequeue_tuple_eager_fallback(dtypes, shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_dequeue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'infeed_dequeue_tuple' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'infeed_dequeue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  _inputs_flat = []
  _attrs = ("dtypes", dtypes, "shapes", shapes)
  _result = _execute.execute(b"InfeedDequeueTuple", len(dtypes),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "InfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("InfeedDequeueTuple")(None)


@tf_export('infeed_enqueue')
def infeed_enqueue(input, shape=[], device_ordinal=-1, name=None):
  r"""An op which feeds a single Tensor value into the computation.

  Args:
    input: A `Tensor`.
      A tensor that will be provided using the infeed mechanism.
    shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `[]`.
      The shape of the tensor.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if shape is None:
      shape = []
    shape = _execute.make_shape(shape, "shape")
    if device_ordinal is None:
      device_ordinal = -1
    device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedEnqueue", input=input, shape=shape,
        device_ordinal=device_ordinal, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedEnqueue", name, _ctx._post_execution_callbacks, input, "shape",
        shape, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      return infeed_enqueue_eager_fallback(
          input, shape=shape, device_ordinal=device_ordinal, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def infeed_enqueue_eager_fallback(input, shape=[], device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_enqueue
  """
  _ctx = ctx if ctx else _context.context()
  if shape is None:
    shape = []
  shape = _execute.make_shape(shape, "shape")
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _attr_dtype, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("dtype", _attr_dtype, "shape", shape, "device_ordinal",
  device_ordinal)
  _result = _execute.execute(b"InfeedEnqueue", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("InfeedEnqueue")(None)


@tf_export('infeed_enqueue_tuple')
def infeed_enqueue_tuple(inputs, shapes, device_ordinal=-1, name=None):
  r"""An op which feeds multiple Tensor values into the computation as an XLA tuple.

  Args:
    inputs: A list of `Tensor` objects.
      A list of tensors that will be provided using the infeed mechanism.
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shapes of each tensor in `inputs`.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(shapes, (list, tuple)):
      raise TypeError(
          "Expected list for 'shapes' argument to "
          "'infeed_enqueue_tuple' Op, not %r." % shapes)
    shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
    if device_ordinal is None:
      device_ordinal = -1
    device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedEnqueueTuple", inputs=inputs, shapes=shapes,
        device_ordinal=device_ordinal, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedEnqueueTuple", name, _ctx._post_execution_callbacks, inputs,
        "shapes", shapes, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      return infeed_enqueue_tuple_eager_fallback(
          inputs, shapes=shapes, device_ordinal=device_ordinal, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def infeed_enqueue_tuple_eager_fallback(inputs, shapes, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_enqueue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'infeed_enqueue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _attr_dtypes, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("dtypes", _attr_dtypes, "shapes", shapes, "device_ordinal",
  device_ordinal)
  _result = _execute.execute(b"InfeedEnqueueTuple", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("InfeedEnqueueTuple")(None)


@tf_export('outfeed_dequeue')
def outfeed_dequeue(dtype, shape, device_ordinal=-1, name=None):
  r"""Retrieves a single tensor from the computation outfeed.  This operation will

  block indefinitely until data is available.

  Args:
    dtype: A `tf.DType`. The type of elements in the tensor.
    shape: A `tf.TensorShape` or list of `ints`. The shape of the tensor.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
    A tensor that will be read from the device outfeed.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    dtype = _execute.make_type(dtype, "dtype")
    shape = _execute.make_shape(shape, "shape")
    if device_ordinal is None:
      device_ordinal = -1
    device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
    _, _, _op = _op_def_lib._apply_op_helper(
        "OutfeedDequeue", dtype=dtype, shape=shape,
        device_ordinal=device_ordinal, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"),
              "device_ordinal", _op.get_attr("device_ordinal"))
    _execute.record_gradient(
      "OutfeedDequeue", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedDequeue", name, _ctx._post_execution_callbacks, "dtype",
        dtype, "shape", shape, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      return outfeed_dequeue_eager_fallback(
          dtype=dtype, shape=shape, device_ordinal=device_ordinal, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def outfeed_dequeue_eager_fallback(dtype, shape, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_dequeue
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape, "device_ordinal", device_ordinal)
  _result = _execute.execute(b"OutfeedDequeue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OutfeedDequeue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("OutfeedDequeue")(None)


@tf_export('outfeed_dequeue_tuple')
def outfeed_dequeue_tuple(dtypes, shapes, device_ordinal=-1, name=None):
  r"""Retrieve multiple values that will be emitted by the computation as an XLA

  tuple.  This operations will block indefinitely until data is available.
  Output `i` corresponds to XLA tuple element `i`.

  Args:
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
      The element types of each element in `outputs`.
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shapes of each tensor in `outputs`.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
    A list of tensors that will be read from the outfeed.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(dtypes, (list, tuple)):
      raise TypeError(
          "Expected list for 'dtypes' argument to "
          "'outfeed_dequeue_tuple' Op, not %r." % dtypes)
    dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
    if not isinstance(shapes, (list, tuple)):
      raise TypeError(
          "Expected list for 'shapes' argument to "
          "'outfeed_dequeue_tuple' Op, not %r." % shapes)
    shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
    if device_ordinal is None:
      device_ordinal = -1
    device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
    _, _, _op = _op_def_lib._apply_op_helper(
        "OutfeedDequeueTuple", dtypes=dtypes, shapes=shapes,
        device_ordinal=device_ordinal, name=name)
    _result = _op.outputs[:]
    if not _result:
      return _op
    _inputs_flat = _op.inputs
    _attrs = ("dtypes", _op.get_attr("dtypes"), "shapes",
              _op.get_attr("shapes"), "device_ordinal",
              _op.get_attr("device_ordinal"))
    _execute.record_gradient(
      "OutfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedDequeueTuple", name, _ctx._post_execution_callbacks, "dtypes",
        dtypes, "shapes", shapes, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      return outfeed_dequeue_tuple_eager_fallback(
          dtypes=dtypes, shapes=shapes, device_ordinal=device_ordinal,
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def outfeed_dequeue_tuple_eager_fallback(dtypes, shapes, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_dequeue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'outfeed_dequeue_tuple' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'outfeed_dequeue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _inputs_flat = []
  _attrs = ("dtypes", dtypes, "shapes", shapes, "device_ordinal",
  device_ordinal)
  _result = _execute.execute(b"OutfeedDequeueTuple", len(dtypes),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "OutfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("OutfeedDequeueTuple")(None)


@tf_export('outfeed_enqueue')
def outfeed_enqueue(input, name=None):
  r"""An op which emits a single Tensor value from an XLA computation.

  Args:
    input: A `Tensor`. A tensor that will be inserted into the outfeed queue.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "OutfeedEnqueue", input=input, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedEnqueue", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      return outfeed_enqueue_eager_fallback(
          input, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def outfeed_enqueue_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_enqueue
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("dtype", _attr_dtype)
  _result = _execute.execute(b"OutfeedEnqueue", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("OutfeedEnqueue")(None)


@tf_export('outfeed_enqueue_tuple')
def outfeed_enqueue_tuple(inputs, name=None):
  r"""An op which emits multiple Tensor values from an XLA computation.

  Args:
    inputs: A list of `Tensor` objects.
      A list of tensors that will be inserted into the outfeed queue as an
      XLA tuple.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "OutfeedEnqueueTuple", inputs=inputs, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedEnqueueTuple", name, _ctx._post_execution_callbacks, inputs)
      return _result
    except _core._FallbackException:
      return outfeed_enqueue_tuple_eager_fallback(
          inputs, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def outfeed_enqueue_tuple_eager_fallback(inputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_enqueue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtypes, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("dtypes", _attr_dtypes)
  _result = _execute.execute(b"OutfeedEnqueueTuple", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("OutfeedEnqueueTuple")(None)


@tf_export('shutdown_distributed_tpu')
def shutdown_distributed_tpu(name=None):
  r"""An op that shuts down a running distributed TPU system. The Op returns

  an error if no system is running.

  Args:
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ShutdownDistributedTPU", name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ShutdownDistributedTPU", name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      return shutdown_distributed_tpu_eager_fallback(
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def shutdown_distributed_tpu_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function shutdown_distributed_tpu
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"ShutdownDistributedTPU", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("ShutdownDistributedTPU")(None)


@tf_export('tpu_compilation_result')
def tpu_compilation_result(name=None):
  r"""TODO: add doc.

  Args:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUCompilationResult", name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = None
    _execute.record_gradient(
      "TPUCompilationResult", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUCompilationResult", name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      return tpu_compilation_result_eager_fallback(
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_compilation_result_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_compilation_result
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"TPUCompilationResult", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TPUCompilationResult", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TPUCompilationResult")(None)


@tf_export('tpu_embedding_activations')
def tpu_embedding_activations(embedding_variable, sliced_activations, table_id, lookup_id, name=None):
  r"""An op enabling differentiation of TPU Embeddings.

  This op simply returns its first input, which is assumed to have been sliced
  from the Tensors returned by TPUEmbeddingDequeueActivations. The presence of this
  op, and its first argument being a trainable Variable, enables automatic
  differentiation of graphs containing embeddings via the TPU Embedding Python
  libraries.

  Args:
    embedding_variable: A `Tensor` of type `float32`.
      A trainable variable, enabling optimizers to find this op.
    sliced_activations: A `Tensor` of type `float32`.
      The embedding activations Tensor to return.
    table_id: An `int` that is `>= 0`.
      The id of the table in the embedding layer configuration from which
      these activations were computed.
    lookup_id: An `int` that is `>= 0`.
      Identifier of the set of embedding indices which produced these
      activations.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    table_id = _execute.make_int(table_id, "table_id")
    lookup_id = _execute.make_int(lookup_id, "lookup_id")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingActivations", embedding_variable=embedding_variable,
        sliced_activations=sliced_activations, table_id=table_id,
        lookup_id=lookup_id, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("table_id", _op.get_attr("table_id"), "lookup_id",
              _op.get_attr("lookup_id"))
    _execute.record_gradient(
      "TPUEmbeddingActivations", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingActivations", name, _ctx._post_execution_callbacks,
        embedding_variable, sliced_activations, "table_id", table_id,
        "lookup_id", lookup_id)
      return _result
    except _core._FallbackException:
      return tpu_embedding_activations_eager_fallback(
          embedding_variable, sliced_activations, table_id=table_id,
          lookup_id=lookup_id, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_activations_eager_fallback(embedding_variable, sliced_activations, table_id, lookup_id, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_activations
  """
  _ctx = ctx if ctx else _context.context()
  table_id = _execute.make_int(table_id, "table_id")
  lookup_id = _execute.make_int(lookup_id, "lookup_id")
  embedding_variable = _ops.convert_to_tensor(embedding_variable, _dtypes.float32)
  sliced_activations = _ops.convert_to_tensor(sliced_activations, _dtypes.float32)
  _inputs_flat = [embedding_variable, sliced_activations]
  _attrs = ("table_id", table_id, "lookup_id", lookup_id)
  _result = _execute.execute(b"TPUEmbeddingActivations", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUEmbeddingActivations", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TPUEmbeddingActivations")(None)


@tf_export('tpu_embedding_enqueue_sparse_batch')
def tpu_embedding_enqueue_sparse_batch(sample_indices, embedding_indices, aggregation_weights, device_ordinal=-1, name=None):
  r"""An op that feeds a batch of embedding indices and weights to the TPU.

  Embedding lookups are equivalent to sparse-dense matrix multiplications: the
  sparse matrix contains nonzeros in column j in order to retrieve row j from the
  embedding table.

  The three Tensor list arguments (sample_indices, embedding_indices, and
  aggregation_weights) represent these sparse matrices in COO format. The Tensor
  lists each have one entry for each embedding table specified in the model.
  For the kth embedding table, the three Tensors at position k in the list
  specify a COO-format sparse matrix. For the kth table, the row indices,
  column indices, and nonzero values of the COO sparse matrix are specified by
  sample_indices[k], embedding_indices[k], and aggregation_weights[k],
  respectively. Entries must be sorted by row index, then by column index.

  There should be at most one TPUEmbeddingEnqueueSparseBatch op in a signle
  training step per TPU shard.

  Args:
    sample_indices: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors specifying row indices of the COO
      sparse matrix representing the embedding lookups for each table.
    embedding_indices: A list with the same length as `sample_indices` of `Tensor` objects with type `int32`.
      A list of rank 1 Tensors  specifying column indices of the
      COO sparse matrix representing the embedding lookups for each table.
    aggregation_weights: A list with the same length as `sample_indices` of `Tensor` objects with type `float32`.
      A list of rank 1 Tensors specifying the nonzero values
      of the COO sparse matrix representing the embedding lookups for each table.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(sample_indices, (list, tuple)):
      raise TypeError(
          "Expected list for 'sample_indices' argument to "
          "'tpu_embedding_enqueue_sparse_batch' Op, not %r." % sample_indices)
    _attr_num_tables = len(sample_indices)
    if not isinstance(embedding_indices, (list, tuple)):
      raise TypeError(
          "Expected list for 'embedding_indices' argument to "
          "'tpu_embedding_enqueue_sparse_batch' Op, not %r." % embedding_indices)
    if len(embedding_indices) != _attr_num_tables:
      raise ValueError(
          "List argument 'embedding_indices' to 'tpu_embedding_enqueue_sparse_batch' Op with length %d "
          "must match length %d of argument 'sample_indices'." %
          (len(embedding_indices), _attr_num_tables))
    if not isinstance(aggregation_weights, (list, tuple)):
      raise TypeError(
          "Expected list for 'aggregation_weights' argument to "
          "'tpu_embedding_enqueue_sparse_batch' Op, not %r." % aggregation_weights)
    if len(aggregation_weights) != _attr_num_tables:
      raise ValueError(
          "List argument 'aggregation_weights' to 'tpu_embedding_enqueue_sparse_batch' Op with length %d "
          "must match length %d of argument 'sample_indices'." %
          (len(aggregation_weights), _attr_num_tables))
    if device_ordinal is None:
      device_ordinal = -1
    device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingEnqueueSparseBatch", sample_indices=sample_indices,
        embedding_indices=embedding_indices,
        aggregation_weights=aggregation_weights,
        device_ordinal=device_ordinal, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingEnqueueSparseBatch", name,
        _ctx._post_execution_callbacks, sample_indices, embedding_indices,
        aggregation_weights, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      return tpu_embedding_enqueue_sparse_batch_eager_fallback(
          sample_indices, embedding_indices, aggregation_weights,
          device_ordinal=device_ordinal, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_enqueue_sparse_batch_eager_fallback(sample_indices, embedding_indices, aggregation_weights, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_enqueue_sparse_batch
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(sample_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sample_indices' argument to "
        "'tpu_embedding_enqueue_sparse_batch' Op, not %r." % sample_indices)
  _attr_num_tables = len(sample_indices)
  if not isinstance(embedding_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'embedding_indices' argument to "
        "'tpu_embedding_enqueue_sparse_batch' Op, not %r." % embedding_indices)
  if len(embedding_indices) != _attr_num_tables:
    raise ValueError(
        "List argument 'embedding_indices' to 'tpu_embedding_enqueue_sparse_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(embedding_indices), _attr_num_tables))
  if not isinstance(aggregation_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'aggregation_weights' argument to "
        "'tpu_embedding_enqueue_sparse_batch' Op, not %r." % aggregation_weights)
  if len(aggregation_weights) != _attr_num_tables:
    raise ValueError(
        "List argument 'aggregation_weights' to 'tpu_embedding_enqueue_sparse_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(aggregation_weights), _attr_num_tables))
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  sample_indices = _ops.convert_n_to_tensor(sample_indices, _dtypes.int32)
  embedding_indices = _ops.convert_n_to_tensor(embedding_indices, _dtypes.int32)
  aggregation_weights = _ops.convert_n_to_tensor(aggregation_weights, _dtypes.float32)
  _inputs_flat = list(sample_indices) + list(embedding_indices) + list(aggregation_weights)
  _attrs = ("num_tables", _attr_num_tables, "device_ordinal", device_ordinal)
  _result = _execute.execute(b"TPUEmbeddingEnqueueSparseBatch", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("TPUEmbeddingEnqueueSparseBatch")(None)


@tf_export('tpu_embedding_load_adagrad_parameters')
def tpu_embedding_load_adagrad_parameters(parameters, accumulators, tpu_embedding_config, table_id, num_hosts, host_id, name=None):
  r"""Load an embedding table shard into TensorNode memories for use with Adagrad.

  TPU embeddings use dedicated per-optimizer Ops for loading and retrieving
  trainable variables and optimizer state from TPU memory. This op enables
  functionality equivalent to AdagradOptimizer.

  Args:
    parameters: A `Tensor` of type `float32`.
      The shard of the embedding table resident on the host executing this
      op. For single-TPU models, this is the entire embedding table.
    accumulators: A `Tensor` of type `float32`.
      Shard of the Adagrad accumulators resident on the host executing
      this op.
    tpu_embedding_config: A `string`.
      Serialized TPUEmbeddingConfiguration proto.
    table_id: An `int` that is `>= 0`.
      The id of the table specified in the embedding_config.
    num_hosts: An `int` that is `>= 1`.
      The number of CPU hosts in the distributed training job.
    host_id: An `int` that is `>= 0`.
      Which CPU host in the distributed training job will execute this op.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
    table_id = _execute.make_int(table_id, "table_id")
    num_hosts = _execute.make_int(num_hosts, "num_hosts")
    host_id = _execute.make_int(host_id, "host_id")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingLoadAdagradParameters", parameters=parameters,
        accumulators=accumulators, tpu_embedding_config=tpu_embedding_config,
        table_id=table_id, num_hosts=num_hosts, host_id=host_id, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingLoadAdagradParameters", name,
        _ctx._post_execution_callbacks, parameters, accumulators,
        "tpu_embedding_config", tpu_embedding_config, "table_id", table_id,
        "num_hosts", num_hosts, "host_id", host_id)
      return _result
    except _core._FallbackException:
      return tpu_embedding_load_adagrad_parameters_eager_fallback(
          parameters, accumulators, tpu_embedding_config=tpu_embedding_config,
          table_id=table_id, num_hosts=num_hosts, host_id=host_id, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_load_adagrad_parameters_eager_fallback(parameters, accumulators, tpu_embedding_config, table_id, num_hosts, host_id, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_load_adagrad_parameters
  """
  _ctx = ctx if ctx else _context.context()
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  table_id = _execute.make_int(table_id, "table_id")
  num_hosts = _execute.make_int(num_hosts, "num_hosts")
  host_id = _execute.make_int(host_id, "host_id")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  _inputs_flat = [parameters, accumulators]
  _attrs = ("tpu_embedding_config", tpu_embedding_config, "table_id",
  table_id, "num_hosts", num_hosts, "host_id", host_id)
  _result = _execute.execute(b"TPUEmbeddingLoadAdagradParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("TPUEmbeddingLoadAdagradParameters")(None)


@tf_export('tpu_embedding_load_gradient_descent_parameters')
def tpu_embedding_load_gradient_descent_parameters(parameters, tpu_embedding_config, table_id, num_hosts, host_id, name=None):
  r"""Load an embedding table shard into TPU memory for use with GradientDescent.

  TPU embeddings use dedicated per-optimizer Ops for loading and retrieving
  trainable variables and optimizer state from TPU memory. This op enables
  functionality equivalent to GradientDescentOptimizer.

  Args:
    parameters: A `Tensor` of type `float32`.
      The shard of the embedding table resident on the host executing this
      op. For single-TPU models, this is the entire embedding table.
    tpu_embedding_config: A `string`.
      Serialized TPUEmbeddingConfiguration proto.
    table_id: An `int` that is `>= 0`.
      The id of the table specified in the tpu_embedding_config.
    num_hosts: An `int` that is `>= 1`.
      The number of CPU hosts in the distributed training job.
    host_id: An `int` that is `>= 0`.
      Which CPU host in the distributed training job will execute this op.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
    table_id = _execute.make_int(table_id, "table_id")
    num_hosts = _execute.make_int(num_hosts, "num_hosts")
    host_id = _execute.make_int(host_id, "host_id")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingLoadGradientDescentParameters", parameters=parameters,
        tpu_embedding_config=tpu_embedding_config, table_id=table_id,
        num_hosts=num_hosts, host_id=host_id, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingLoadGradientDescentParameters", name,
        _ctx._post_execution_callbacks, parameters, "tpu_embedding_config",
        tpu_embedding_config, "table_id", table_id, "num_hosts", num_hosts,
        "host_id", host_id)
      return _result
    except _core._FallbackException:
      return tpu_embedding_load_gradient_descent_parameters_eager_fallback(
          parameters, tpu_embedding_config=tpu_embedding_config,
          table_id=table_id, num_hosts=num_hosts, host_id=host_id, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_load_gradient_descent_parameters_eager_fallback(parameters, tpu_embedding_config, table_id, num_hosts, host_id, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_load_gradient_descent_parameters
  """
  _ctx = ctx if ctx else _context.context()
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  table_id = _execute.make_int(table_id, "table_id")
  num_hosts = _execute.make_int(num_hosts, "num_hosts")
  host_id = _execute.make_int(host_id, "host_id")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  _inputs_flat = [parameters]
  _attrs = ("tpu_embedding_config", tpu_embedding_config, "table_id",
  table_id, "num_hosts", num_hosts, "host_id", host_id)
  _result = _execute.execute(b"TPUEmbeddingLoadGradientDescentParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("TPUEmbeddingLoadGradientDescentParameters")(None)


@tf_export('tpu_embedding_receive_activations')
def tpu_embedding_receive_activations(num_tables, tpu_embedding_config, name=None):
  r"""An op that receives embedding activations on the TPU.

  The TPU system performs the embedding lookups and aggregations specified by
  the arguments to TPUEmbeddingEnqueueSparseBatch. The results of these
  aggregations are visible to the Tensorflow Graph as the outputs of a
  TPUEmbeddingDequeueActivations Op. This op returns a list containing one
  Tensor of activations per table specified in the model. There can be at most
  one ReceieveActivations op in the TPU graph.

  Args:
    num_tables: An `int` that is `>= 1`.
      The number of output activation tensors, equal to the number of
      embedding tables in the model.
    tpu_embedding_config: A `string`.
      Serialized TPUEmbeddingConfiguration proto.
    name: A name for the operation (optional).

  Returns:
    A list of `num_tables` `Tensor` objects with type `float32`.
    A TensorList of embedding activations containing one Tensor per
    embedding table in the model.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    num_tables = _execute.make_int(num_tables, "num_tables")
    tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingReceiveActivations", num_tables=num_tables,
        tpu_embedding_config=tpu_embedding_config, name=name)
    _result = _op.outputs[:]
    if not _result:
      return _op
    _inputs_flat = _op.inputs
    _attrs = ("num_tables", _op.get_attr("num_tables"),
              "tpu_embedding_config", _op.get_attr("tpu_embedding_config"))
    _execute.record_gradient(
      "TPUEmbeddingReceiveActivations", _inputs_flat, _attrs, _result, name)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingReceiveActivations", name,
        _ctx._post_execution_callbacks, "num_tables", num_tables,
        "tpu_embedding_config", tpu_embedding_config)
      return _result
    except _core._FallbackException:
      return tpu_embedding_receive_activations_eager_fallback(
          num_tables=num_tables, tpu_embedding_config=tpu_embedding_config,
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_receive_activations_eager_fallback(num_tables, tpu_embedding_config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_receive_activations
  """
  _ctx = ctx if ctx else _context.context()
  num_tables = _execute.make_int(num_tables, "num_tables")
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  _inputs_flat = []
  _attrs = ("num_tables", num_tables, "tpu_embedding_config",
  tpu_embedding_config)
  _result = _execute.execute(b"TPUEmbeddingReceiveActivations", num_tables,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUEmbeddingReceiveActivations", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("TPUEmbeddingReceiveActivations")(None)


_tpu_embedding_retrieve_adagrad_parameters_outputs = ["parameters",
                                                     "accumulators"]
_TPUEmbeddingRetrieveAdagradParametersOutput = _collections.namedtuple(
    "TPUEmbeddingRetrieveAdagradParameters",
    _tpu_embedding_retrieve_adagrad_parameters_outputs)


@tf_export('tpu_embedding_retrieve_adagrad_parameters')
def tpu_embedding_retrieve_adagrad_parameters(tpu_embedding_config, table_id, num_hosts, host_id, name=None):
  r"""Retrieve an embedding table shard from TPU memory.

  TPU embeddings use dedicated per-optimizer Ops for loading and retrieving
  trainable variables and optimizer state from TPU memory. This op enables
  functionality equivalent to AdagradOptimizer.

  Args:
    tpu_embedding_config: A `string`.
      Serialized TPUEmbeddingConfiguration proto.
    table_id: An `int` that is `>= 0`.
      The id of the table specified in the embedding_config_json.
    num_hosts: An `int` that is `>= 1`.
      The number of CPU hosts in the distributed training job.
    host_id: An `int` that is `>= 0`.
      Which CPU host in the distributed training job will execute this op.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators).

    parameters: A `Tensor` of type `float32`.
    accumulators: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
    table_id = _execute.make_int(table_id, "table_id")
    num_hosts = _execute.make_int(num_hosts, "num_hosts")
    host_id = _execute.make_int(host_id, "host_id")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingRetrieveAdagradParameters",
        tpu_embedding_config=tpu_embedding_config, table_id=table_id,
        num_hosts=num_hosts, host_id=host_id, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("tpu_embedding_config", _op.get_attr("tpu_embedding_config"),
              "table_id", _op.get_attr("table_id"), "num_hosts",
              _op.get_attr("num_hosts"), "host_id", _op.get_attr("host_id"))
    _execute.record_gradient(
      "TPUEmbeddingRetrieveAdagradParameters", _inputs_flat, _attrs, _result, name)
    _result = _TPUEmbeddingRetrieveAdagradParametersOutput._make(_result)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingRetrieveAdagradParameters", name,
        _ctx._post_execution_callbacks, "tpu_embedding_config",
        tpu_embedding_config, "table_id", table_id, "num_hosts", num_hosts,
        "host_id", host_id)
      _result = _TPUEmbeddingRetrieveAdagradParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      return tpu_embedding_retrieve_adagrad_parameters_eager_fallback(
          tpu_embedding_config=tpu_embedding_config, table_id=table_id,
          num_hosts=num_hosts, host_id=host_id, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_retrieve_adagrad_parameters_eager_fallback(tpu_embedding_config, table_id, num_hosts, host_id, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_retrieve_adagrad_parameters
  """
  _ctx = ctx if ctx else _context.context()
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  table_id = _execute.make_int(table_id, "table_id")
  num_hosts = _execute.make_int(num_hosts, "num_hosts")
  host_id = _execute.make_int(host_id, "host_id")
  _inputs_flat = []
  _attrs = ("tpu_embedding_config", tpu_embedding_config, "table_id",
  table_id, "num_hosts", num_hosts, "host_id", host_id)
  _result = _execute.execute(b"TPUEmbeddingRetrieveAdagradParameters", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUEmbeddingRetrieveAdagradParameters", _inputs_flat, _attrs, _result, name)
  _result = _TPUEmbeddingRetrieveAdagradParametersOutput._make(_result)
  return _result

_ops.RegisterShape("TPUEmbeddingRetrieveAdagradParameters")(None)


@tf_export('tpu_embedding_retrieve_gradient_descent_parameters')
def tpu_embedding_retrieve_gradient_descent_parameters(tpu_embedding_config, table_id, num_hosts, host_id, name=None):
  r"""Retrieve an embedding table shard from TPU memory.

  TPU embeddings use dedicated per-optimizer Ops for loading and retrieving
  trainable variables and optimizer state from TPU memory. This op enables
  functionality equivalent to GradientDescentOptimizer.

  Args:
    tpu_embedding_config: A `string`.
      Serialized TPUEmbeddingConfiguration proto.
    table_id: An `int`. The id of the table specified in tpu_embedding_config.
    num_hosts: An `int`.
      The number of CPU hosts in the distributed training job.
    host_id: An `int`.
      Which CPU host in the distributed training job will execute this op.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
    table_id = _execute.make_int(table_id, "table_id")
    num_hosts = _execute.make_int(num_hosts, "num_hosts")
    host_id = _execute.make_int(host_id, "host_id")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingRetrieveGradientDescentParameters",
        tpu_embedding_config=tpu_embedding_config, table_id=table_id,
        num_hosts=num_hosts, host_id=host_id, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("tpu_embedding_config", _op.get_attr("tpu_embedding_config"),
              "table_id", _op.get_attr("table_id"), "num_hosts",
              _op.get_attr("num_hosts"), "host_id", _op.get_attr("host_id"))
    _execute.record_gradient(
      "TPUEmbeddingRetrieveGradientDescentParameters", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingRetrieveGradientDescentParameters", name,
        _ctx._post_execution_callbacks, "tpu_embedding_config",
        tpu_embedding_config, "table_id", table_id, "num_hosts", num_hosts,
        "host_id", host_id)
      return _result
    except _core._FallbackException:
      return tpu_embedding_retrieve_gradient_descent_parameters_eager_fallback(
          tpu_embedding_config=tpu_embedding_config, table_id=table_id,
          num_hosts=num_hosts, host_id=host_id, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_retrieve_gradient_descent_parameters_eager_fallback(tpu_embedding_config, table_id, num_hosts, host_id, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_retrieve_gradient_descent_parameters
  """
  _ctx = ctx if ctx else _context.context()
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  table_id = _execute.make_int(table_id, "table_id")
  num_hosts = _execute.make_int(num_hosts, "num_hosts")
  host_id = _execute.make_int(host_id, "host_id")
  _inputs_flat = []
  _attrs = ("tpu_embedding_config", tpu_embedding_config, "table_id",
  table_id, "num_hosts", num_hosts, "host_id", host_id)
  _result = _execute.execute(b"TPUEmbeddingRetrieveGradientDescentParameters",
                             1, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUEmbeddingRetrieveGradientDescentParameters", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TPUEmbeddingRetrieveGradientDescentParameters")(None)


@tf_export('tpu_embedding_send_gradients')
def tpu_embedding_send_gradients(gradients, tpu_embedding_config, name=None):
  r"""An op that performs gradient updates of embedding tables.

  The TensorList argument has the same length and shapes as the return value of
  TPUEmbeddingReceiveActivations, but contains gradients of the model's loss
  with respect to the embedding activations. The embedding tables are updated
  from these gradients via the optimizer specified in the configuration given
  to tpu.initialize_system.

  Args:
    gradients: A list of at least 1 `Tensor` objects with type `float32`.
      A TensorList of gradients with which to update embedding tables.
    tpu_embedding_config: A `string`.
      Serialized TPUEmbeddingConfiguration proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(gradients, (list, tuple)):
      raise TypeError(
          "Expected list for 'gradients' argument to "
          "'tpu_embedding_send_gradients' Op, not %r." % gradients)
    _attr_num_tables = len(gradients)
    tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingSendGradients", gradients=gradients,
        tpu_embedding_config=tpu_embedding_config, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingSendGradients", name, _ctx._post_execution_callbacks,
        gradients, "tpu_embedding_config", tpu_embedding_config)
      return _result
    except _core._FallbackException:
      return tpu_embedding_send_gradients_eager_fallback(
          gradients, tpu_embedding_config=tpu_embedding_config, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_embedding_send_gradients_eager_fallback(gradients, tpu_embedding_config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_send_gradients
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(gradients, (list, tuple)):
    raise TypeError(
        "Expected list for 'gradients' argument to "
        "'tpu_embedding_send_gradients' Op, not %r." % gradients)
  _attr_num_tables = len(gradients)
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  gradients = _ops.convert_n_to_tensor(gradients, _dtypes.float32)
  _inputs_flat = list(gradients)
  _attrs = ("num_tables", _attr_num_tables, "tpu_embedding_config",
  tpu_embedding_config)
  _result = _execute.execute(b"TPUEmbeddingSendGradients", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("TPUEmbeddingSendGradients")(None)


@tf_export('tpu_replicate')
def tpu_replicate(inputs, broadcast_inputs, variables, guaranteed_constants, computation, num_replicas, output_types, topology="", use_tpu=True, device_assignment=[], host_compute_core=[], computation_shape=[], name=None):
  r"""Runs replicated computations on a distributed TPU system.

  Args:
    inputs: A list of `Tensor` objects.
      the inputs to 'computation', flattened, in replica-major order.
    broadcast_inputs: A list of `Tensor` objects.
      additional arguments to broadcast to all replicas. The
      broadcast inputs are appended to the per-replica inputs when calling
      computation.
    variables: A list of `Tensor` objects with type `resource`.
    guaranteed_constants: A list of `Tensor` objects.
      arguments which have been guaranteed to not
      change their values during the session lifetime. These contain tensors marked as
      constant using the GuaranteeConstOp.
    computation: A function decorated with @Defun.
      a function containing the computation to run.
    num_replicas: An `int` that is `>= 1`.
      the number of replicas of the computation to run.
    output_types: A list of `tf.DTypes`.
      the types of the outputs of 'computation'.
    topology: An optional `string`. Defaults to `""`.
      A serialized tensorflow.tpu.TopologyProto that describes the TPU
      topology.
    use_tpu: An optional `bool`. Defaults to `True`.
      a bool indicating if this computation will run on TPU or CPU/GPU.
      Currently, only supports a default placement (computation is placed on GPU
      if one is available, and on CPU if not).
    device_assignment: An optional list of `ints`. Defaults to `[]`.
      a flattened array with shape
      [replica] + computation_shape + [mesh_dimension] that maps the coordinates of
      logical cores in each replica of a computation to physical coordinates in
      the TPU topology.
    host_compute_core: An optional list of `strings`. Defaults to `[]`.
    computation_shape: An optional list of `ints`. Defaults to `[]`.
      a [mesh_dimension] array describing the shape of each
      computation replica in numbers of cores in the TPU mesh.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `output_types`.
    the outputs of 'computation'.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(variables, (list, tuple)):
      raise TypeError(
          "Expected list for 'variables' argument to "
          "'tpu_replicate' Op, not %r." % variables)
    _attr_NumVariables = len(variables)
    num_replicas = _execute.make_int(num_replicas, "num_replicas")
    if not isinstance(output_types, (list, tuple)):
      raise TypeError(
          "Expected list for 'output_types' argument to "
          "'tpu_replicate' Op, not %r." % output_types)
    output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
    if topology is None:
      topology = ""
    topology = _execute.make_str(topology, "topology")
    if use_tpu is None:
      use_tpu = True
    use_tpu = _execute.make_bool(use_tpu, "use_tpu")
    if device_assignment is None:
      device_assignment = []
    if not isinstance(device_assignment, (list, tuple)):
      raise TypeError(
          "Expected list for 'device_assignment' argument to "
          "'tpu_replicate' Op, not %r." % device_assignment)
    device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
    if host_compute_core is None:
      host_compute_core = []
    if not isinstance(host_compute_core, (list, tuple)):
      raise TypeError(
          "Expected list for 'host_compute_core' argument to "
          "'tpu_replicate' Op, not %r." % host_compute_core)
    host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
    if computation_shape is None:
      computation_shape = []
    if not isinstance(computation_shape, (list, tuple)):
      raise TypeError(
          "Expected list for 'computation_shape' argument to "
          "'tpu_replicate' Op, not %r." % computation_shape)
    computation_shape = [_execute.make_int(_i, "computation_shape") for _i in computation_shape]
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicate", inputs=inputs, broadcast_inputs=broadcast_inputs,
        variables=variables, guaranteed_constants=guaranteed_constants,
        computation=computation, num_replicas=num_replicas,
        output_types=output_types, topology=topology, use_tpu=use_tpu,
        device_assignment=device_assignment,
        host_compute_core=host_compute_core,
        computation_shape=computation_shape, name=name)
    _result = _op.outputs[:]
    if not _result:
      return _op
    _inputs_flat = _op.inputs
    _attrs = ("computation", _op.get_attr("computation"), "num_replicas",
              _op.get_attr("num_replicas"), "topology",
              _op.get_attr("topology"), "use_tpu", _op.get_attr("use_tpu"),
              "device_assignment", _op.get_attr("device_assignment"),
              "host_compute_core", _op.get_attr("host_compute_core"),
              "computation_shape", _op.get_attr("computation_shape"),
              "Tinputs", _op.get_attr("Tinputs"), "Tbroadcast_inputs",
              _op.get_attr("Tbroadcast_inputs"), "NumVariables",
              _op.get_attr("NumVariables"), "Tguaranteed_constants",
              _op.get_attr("Tguaranteed_constants"), "output_types",
              _op.get_attr("output_types"))
    _execute.record_gradient(
      "TPUReplicate", _inputs_flat, _attrs, _result, name)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TPUReplicate",
        name, _ctx._post_execution_callbacks, inputs, broadcast_inputs,
        variables, guaranteed_constants, "computation", computation,
        "num_replicas", num_replicas, "topology", topology, "use_tpu",
        use_tpu, "device_assignment", device_assignment, "host_compute_core",
        host_compute_core, "computation_shape", computation_shape,
        "output_types", output_types)
      return _result
    except _core._FallbackException:
      return tpu_replicate_eager_fallback(
          inputs, broadcast_inputs, variables, guaranteed_constants,
          computation=computation, num_replicas=num_replicas,
          topology=topology, use_tpu=use_tpu,
          device_assignment=device_assignment,
          host_compute_core=host_compute_core,
          computation_shape=computation_shape, output_types=output_types,
          name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_replicate_eager_fallback(inputs, broadcast_inputs, variables, guaranteed_constants, computation, num_replicas, output_types, topology="", use_tpu=True, device_assignment=[], host_compute_core=[], computation_shape=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicate
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(variables, (list, tuple)):
    raise TypeError(
        "Expected list for 'variables' argument to "
        "'tpu_replicate' Op, not %r." % variables)
  _attr_NumVariables = len(variables)
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'tpu_replicate' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if topology is None:
    topology = ""
  topology = _execute.make_str(topology, "topology")
  if use_tpu is None:
    use_tpu = True
  use_tpu = _execute.make_bool(use_tpu, "use_tpu")
  if device_assignment is None:
    device_assignment = []
  if not isinstance(device_assignment, (list, tuple)):
    raise TypeError(
        "Expected list for 'device_assignment' argument to "
        "'tpu_replicate' Op, not %r." % device_assignment)
  device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
  if host_compute_core is None:
    host_compute_core = []
  if not isinstance(host_compute_core, (list, tuple)):
    raise TypeError(
        "Expected list for 'host_compute_core' argument to "
        "'tpu_replicate' Op, not %r." % host_compute_core)
  host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
  if computation_shape is None:
    computation_shape = []
  if not isinstance(computation_shape, (list, tuple)):
    raise TypeError(
        "Expected list for 'computation_shape' argument to "
        "'tpu_replicate' Op, not %r." % computation_shape)
  computation_shape = [_execute.make_int(_i, "computation_shape") for _i in computation_shape]
  _attr_Tinputs, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _attr_Tbroadcast_inputs, broadcast_inputs = _execute.convert_to_mixed_eager_tensors(broadcast_inputs, _ctx)
  _attr_Tguaranteed_constants, guaranteed_constants = _execute.convert_to_mixed_eager_tensors(guaranteed_constants, _ctx)
  variables = _ops.convert_n_to_tensor(variables, _dtypes.resource)
  _inputs_flat = list(inputs) + list(broadcast_inputs) + list(variables) + list(guaranteed_constants)
  _attrs = ("computation", computation, "num_replicas", num_replicas,
  "topology", topology, "use_tpu", use_tpu, "device_assignment",
  device_assignment, "host_compute_core", host_compute_core,
  "computation_shape", computation_shape, "Tinputs", _attr_Tinputs,
  "Tbroadcast_inputs", _attr_Tbroadcast_inputs, "NumVariables",
  _attr_NumVariables, "Tguaranteed_constants", _attr_Tguaranteed_constants,
  "output_types", output_types)
  _result = _execute.execute(b"TPUReplicate", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUReplicate", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("TPUReplicate")(None)


@tf_export('tpu_replicate_metadata')
def tpu_replicate_metadata(num_replicas, topology="", use_tpu=True, device_assignment=[], computation_shape=[], host_compute_core=[], name=None):
  r"""TODO: add doc.

  Args:
    num_replicas: An `int` that is `>= 0`.
    topology: An optional `string`. Defaults to `""`.
    use_tpu: An optional `bool`. Defaults to `True`.
    device_assignment: An optional list of `ints`. Defaults to `[]`.
    computation_shape: An optional list of `ints`. Defaults to `[]`.
    host_compute_core: An optional list of `strings`. Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    num_replicas = _execute.make_int(num_replicas, "num_replicas")
    if topology is None:
      topology = ""
    topology = _execute.make_str(topology, "topology")
    if use_tpu is None:
      use_tpu = True
    use_tpu = _execute.make_bool(use_tpu, "use_tpu")
    if device_assignment is None:
      device_assignment = []
    if not isinstance(device_assignment, (list, tuple)):
      raise TypeError(
          "Expected list for 'device_assignment' argument to "
          "'tpu_replicate_metadata' Op, not %r." % device_assignment)
    device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
    if computation_shape is None:
      computation_shape = []
    if not isinstance(computation_shape, (list, tuple)):
      raise TypeError(
          "Expected list for 'computation_shape' argument to "
          "'tpu_replicate_metadata' Op, not %r." % computation_shape)
    computation_shape = [_execute.make_int(_i, "computation_shape") for _i in computation_shape]
    if host_compute_core is None:
      host_compute_core = []
    if not isinstance(host_compute_core, (list, tuple)):
      raise TypeError(
          "Expected list for 'host_compute_core' argument to "
          "'tpu_replicate_metadata' Op, not %r." % host_compute_core)
    host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicateMetadata", num_replicas=num_replicas, topology=topology,
        use_tpu=use_tpu, device_assignment=device_assignment,
        computation_shape=computation_shape,
        host_compute_core=host_compute_core, name=name)
    return _op
    _result = None
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUReplicateMetadata", name, _ctx._post_execution_callbacks,
        "num_replicas", num_replicas, "topology", topology, "use_tpu",
        use_tpu, "device_assignment", device_assignment, "computation_shape",
        computation_shape, "host_compute_core", host_compute_core)
      return _result
    except _core._FallbackException:
      return tpu_replicate_metadata_eager_fallback(
          num_replicas=num_replicas, topology=topology, use_tpu=use_tpu,
          device_assignment=device_assignment,
          computation_shape=computation_shape,
          host_compute_core=host_compute_core, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_replicate_metadata_eager_fallback(num_replicas, topology="", use_tpu=True, device_assignment=[], computation_shape=[], host_compute_core=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicate_metadata
  """
  _ctx = ctx if ctx else _context.context()
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  if topology is None:
    topology = ""
  topology = _execute.make_str(topology, "topology")
  if use_tpu is None:
    use_tpu = True
  use_tpu = _execute.make_bool(use_tpu, "use_tpu")
  if device_assignment is None:
    device_assignment = []
  if not isinstance(device_assignment, (list, tuple)):
    raise TypeError(
        "Expected list for 'device_assignment' argument to "
        "'tpu_replicate_metadata' Op, not %r." % device_assignment)
  device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
  if computation_shape is None:
    computation_shape = []
  if not isinstance(computation_shape, (list, tuple)):
    raise TypeError(
        "Expected list for 'computation_shape' argument to "
        "'tpu_replicate_metadata' Op, not %r." % computation_shape)
  computation_shape = [_execute.make_int(_i, "computation_shape") for _i in computation_shape]
  if host_compute_core is None:
    host_compute_core = []
  if not isinstance(host_compute_core, (list, tuple)):
    raise TypeError(
        "Expected list for 'host_compute_core' argument to "
        "'tpu_replicate_metadata' Op, not %r." % host_compute_core)
  host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
  _inputs_flat = []
  _attrs = ("num_replicas", num_replicas, "topology", topology, "use_tpu",
  use_tpu, "device_assignment", device_assignment, "computation_shape",
  computation_shape, "host_compute_core", host_compute_core)
  _result = _execute.execute(b"TPUReplicateMetadata", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("TPUReplicateMetadata")(None)


@tf_export('tpu_replicated_input')
def tpu_replicated_input(inputs, name=None):
  r"""Operator that connects N unreplicated inputs to an N-way replicated TPU computation.

  Args:
    inputs: A list of at least 1 `Tensor` objects with the same type.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `inputs`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    if not isinstance(inputs, (list, tuple)):
      raise TypeError(
          "Expected list for 'inputs' argument to "
          "'tpu_replicated_input' Op, not %r." % inputs)
    _attr_N = len(inputs)
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicatedInput", inputs=inputs, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"))
    _execute.record_gradient(
      "TPUReplicatedInput", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUReplicatedInput", name, _ctx._post_execution_callbacks, inputs)
      return _result
    except _core._FallbackException:
      return tpu_replicated_input_eager_fallback(
          inputs, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_replicated_input_eager_fallback(inputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicated_input
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'tpu_replicated_input' Op, not %r." % inputs)
  _attr_N = len(inputs)
  _attr_T, inputs = _execute.args_to_matching_eager(list(inputs), _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"TPUReplicatedInput", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TPUReplicatedInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TPUReplicatedInput")(None)


@tf_export('tpu_replicated_output')
def tpu_replicated_output(input, num_replicas, name=None):
  r"""Operator that connects the output of an N-way replicated TPU computation to N separate outputs.

  Args:
    input: A `Tensor`.
    num_replicas: An `int` that is `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A list of `num_replicas` `Tensor` objects with the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    num_replicas = _execute.make_int(num_replicas, "num_replicas")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicatedOutput", input=input, num_replicas=num_replicas,
        name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("num_replicas", _op.get_attr("num_replicas"), "T",
              _op.get_attr("T"))
    _execute.record_gradient(
      "TPUReplicatedOutput", _inputs_flat, _attrs, _result, name)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUReplicatedOutput", name, _ctx._post_execution_callbacks, input,
        "num_replicas", num_replicas)
      return _result
    except _core._FallbackException:
      return tpu_replicated_output_eager_fallback(
          input, num_replicas=num_replicas, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tpu_replicated_output_eager_fallback(input, num_replicas, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicated_output
  """
  _ctx = ctx if ctx else _context.context()
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("num_replicas", num_replicas, "T", _attr_T)
  _result = _execute.execute(b"TPUReplicatedOutput", num_replicas,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUReplicatedOutput", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("TPUReplicatedOutput")(None)


@tf_export('worker_heartbeat')
def worker_heartbeat(request, name=None):
  r"""Worker heartbeat op.

  Heartbeats may be sent periodically to indicate the coordinator is still active,
  to retrieve the current worker status and to expedite shutdown when necessary.

  Args:
    request: A `Tensor` of type `string`.
      A string tensor containing a serialized WorkerHeartbeatRequest
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
    A string tensor containing a serialized WorkerHeartbeatResponse
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "WorkerHeartbeat", request=request, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = None
    _execute.record_gradient(
      "WorkerHeartbeat", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "WorkerHeartbeat", name, _ctx._post_execution_callbacks, request)
      return _result
    except _core._FallbackException:
      return worker_heartbeat_eager_fallback(
          request, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def worker_heartbeat_eager_fallback(request, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function worker_heartbeat
  """
  _ctx = ctx if ctx else _context.context()
  request = _ops.convert_to_tensor(request, _dtypes.string)
  _inputs_flat = [request]
  _attrs = None
  _result = _execute.execute(b"WorkerHeartbeat", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "WorkerHeartbeat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("WorkerHeartbeat")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "ConfigureDistributedTPU"
#   output_arg {
#     name: "topology"
#     type: DT_STRING
#   }
#   attr {
#     name: "embedding_config"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "is_global_init"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "CrossReplicaSum"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "group_assignment"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
# }
# op {
#   name: "InfeedDequeue"
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   is_stateful: true
# }
# op {
#   name: "InfeedDequeueTuple"
#   output_arg {
#     name: "outputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#   }
#   is_stateful: true
# }
# op {
#   name: "InfeedEnqueue"
#   input_arg {
#     name: "input"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#     default_value {
#       shape {
#       }
#     }
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "InfeedEnqueueTuple"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedDequeue"
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedDequeueTuple"
#   output_arg {
#     name: "outputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedEnqueue"
#   input_arg {
#     name: "input"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedEnqueueTuple"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ShutdownDistributedTPU"
#   is_stateful: true
# }
# op {
#   name: "TPUCompilationResult"
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
# }
# op {
#   name: "TPUEmbeddingActivations"
#   input_arg {
#     name: "embedding_variable"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sliced_activations"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "lookup_id"
#     type: "int"
#     has_minimum: true
#   }
# }
# op {
#   name: "TPUEmbeddingEnqueueSparseBatch"
#   input_arg {
#     name: "sample_indices"
#     type: DT_INT32
#     number_attr: "num_tables"
#   }
#   input_arg {
#     name: "embedding_indices"
#     type: DT_INT32
#     number_attr: "num_tables"
#   }
#   input_arg {
#     name: "aggregation_weights"
#     type: DT_FLOAT
#     number_attr: "num_tables"
#   }
#   attr {
#     name: "num_tables"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUEmbeddingLoadAdagradParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_hosts"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "host_id"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUEmbeddingLoadGradientDescentParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_hosts"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "host_id"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUEmbeddingReceiveActivations"
#   output_arg {
#     name: "outputs"
#     type: DT_FLOAT
#     number_attr: "num_tables"
#   }
#   attr {
#     name: "num_tables"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUEmbeddingRetrieveAdagradParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_hosts"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "host_id"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUEmbeddingRetrieveGradientDescentParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#   }
#   attr {
#     name: "num_hosts"
#     type: "int"
#   }
#   attr {
#     name: "host_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUEmbeddingSendGradients"
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#     number_attr: "num_tables"
#   }
#   attr {
#     name: "num_tables"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUReplicate"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "Tinputs"
#   }
#   input_arg {
#     name: "broadcast_inputs"
#     type_list_attr: "Tbroadcast_inputs"
#   }
#   input_arg {
#     name: "variables"
#     type: DT_RESOURCE
#     number_attr: "NumVariables"
#   }
#   input_arg {
#     name: "guaranteed_constants"
#     type_list_attr: "Tguaranteed_constants"
#   }
#   output_arg {
#     name: "outputs"
#     type_list_attr: "output_types"
#   }
#   attr {
#     name: "computation"
#     type: "func"
#   }
#   attr {
#     name: "num_replicas"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "topology"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "use_tpu"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "device_assignment"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "host_compute_core"
#     type: "list(string)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "computation_shape"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "Tinputs"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tbroadcast_inputs"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "NumVariables"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "Tguaranteed_constants"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUReplicateMetadata"
#   attr {
#     name: "num_replicas"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "topology"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "use_tpu"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "device_assignment"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "computation_shape"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "host_compute_core"
#     type: "list(string)"
#     default_value {
#       list {
#       }
#     }
#   }
# }
# op {
#   name: "TPUReplicatedInput"
#   input_arg {
#     name: "inputs"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "TPUReplicatedOutput"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "outputs"
#     type_attr: "T"
#     number_attr: "num_replicas"
#   }
#   attr {
#     name: "num_replicas"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "WorkerHeartbeat"
#   input_arg {
#     name: "request"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "response"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\212\001\n\027ConfigureDistributedTPU\032\014\n\010topology\030\007\"\036\n\020embedding_config\022\006string\032\002\022\000\"\"\n\024tpu_embedding_config\022\006string\032\002\022\000\"\032\n\016is_global_init\022\004bool\032\002(\000\210\001\001\n`\n\017CrossReplicaSum\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\016\001\"!\n\020group_assignment\022\tlist(int)\032\002\n\000\nB\n\rInfeedDequeue\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\210\001\001\n[\n\022InfeedDequeueTuple\032\021\n\007outputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\025\n\006shapes\022\013list(shape)\210\001\001\ni\n\rInfeedEnqueue\022\016\n\005input\"\005dtype\"\r\n\005dtype\022\004type\"\022\n\005shape\022\005shape\032\002:\000\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n~\n\022InfeedEnqueueTuple\022\020\n\006inputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\025\n\006shapes\022\013list(shape)\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\ng\n\016OutfeedDequeue\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\200\001\n\023OutfeedDequeueTuple\032\021\n\007outputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\025\n\006shapes\022\013list(shape)\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n2\n\016OutfeedEnqueue\022\016\n\005input\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\nD\n\023OutfeedEnqueueTuple\022\020\n\006inputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\210\001\001\n\033\n\026ShutdownDistributedTPU\210\001\001\n\"\n\024TPUCompilationResult\032\n\n\006output\030\007\n|\n\027TPUEmbeddingActivations\022\026\n\022embedding_variable\030\001\022\026\n\022sliced_activations\030\001\032\n\n\006output\030\001\"\021\n\010table_id\022\003int(\001\"\022\n\tlookup_id\022\003int(\001\n\306\001\n\036TPUEmbeddingEnqueueSparseBatch\022\036\n\016sample_indices\030\003*\nnum_tables\022!\n\021embedding_indices\030\003*\nnum_tables\022#\n\023aggregation_weights\030\001*\nnum_tables\"\025\n\nnum_tables\022\003int(\0010\001\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\243\001\n!TPUEmbeddingLoadAdagradParameters\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\"\036\n\024tpu_embedding_config\022\006string\"\021\n\010table_id\022\003int(\001\"\024\n\tnum_hosts\022\003int(\0010\001\"\020\n\007host_id\022\003int(\001\210\001\001\n\231\001\n)TPUEmbeddingLoadGradientDescentParameters\022\016\n\nparameters\030\001\"\036\n\024tpu_embedding_config\022\006string\"\021\n\010table_id\022\003int(\001\"\024\n\tnum_hosts\022\003int(\0010\001\"\020\n\007host_id\022\003int(\001\210\001\001\ns\n\036TPUEmbeddingReceiveActivations\032\027\n\007outputs\030\001*\nnum_tables\"\025\n\nnum_tables\022\003int(\0010\001\"\036\n\024tpu_embedding_config\022\006string\210\001\001\n\247\001\n%TPUEmbeddingRetrieveAdagradParameters\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\"\036\n\024tpu_embedding_config\022\006string\"\021\n\010table_id\022\003int(\001\"\024\n\tnum_hosts\022\003int(\0010\001\"\020\n\007host_id\022\003int(\001\210\001\001\n\225\001\n-TPUEmbeddingRetrieveGradientDescentParameters\032\016\n\nparameters\030\001\"\036\n\024tpu_embedding_config\022\006string\"\017\n\010table_id\022\003int\"\020\n\tnum_hosts\022\003int\"\016\n\007host_id\022\003int\210\001\001\np\n\031TPUEmbeddingSendGradients\022\031\n\tgradients\030\001*\nnum_tables\"\025\n\nnum_tables\022\003int(\0010\001\"\036\n\024tpu_embedding_config\022\006string\210\001\001\n\222\004\n\014TPUReplicate\022\021\n\006inputs2\007Tinputs\022%\n\020broadcast_inputs2\021Tbroadcast_inputs\022\033\n\tvariables\030\024*\014NumVariables\022-\n\024guaranteed_constants2\025Tguaranteed_constants\032\027\n\007outputs2\014output_types\"\023\n\013computation\022\004func\"\027\n\014num_replicas\022\003int(\0010\001\"\026\n\010topology\022\006string\032\002\022\000\"\023\n\007use_tpu\022\004bool\032\002(\001\"\"\n\021device_assignment\022\tlist(int)\032\002\n\000\"%\n\021host_compute_core\022\014list(string)\032\002\n\000\"\"\n\021computation_shape\022\tlist(int)\032\002\n\000\"\027\n\007Tinputs\022\nlist(type)(\001\"!\n\021Tbroadcast_inputs\022\nlist(type)(\001\"\025\n\014NumVariables\022\003int(\001\"%\n\025Tguaranteed_constants\022\nlist(type)(\001\"\034\n\014output_types\022\nlist(type)(\001\210\001\001\n\311\001\n\024TPUReplicateMetadata\"\025\n\014num_replicas\022\003int(\001\"\026\n\010topology\022\006string\032\002\022\000\"\023\n\007use_tpu\022\004bool\032\002(\001\"\"\n\021device_assignment\022\tlist(int)\032\002\n\000\"\"\n\021computation_shape\022\tlist(int)\032\002\n\000\"%\n\021host_compute_core\022\014list(string)\032\002\n\000\nJ\n\022TPUReplicatedInput\022\016\n\006inputs\"\001T*\001N\032\013\n\006output\"\001T\"\014\n\001N\022\003int(\0010\001\"\t\n\001T\022\004type\na\n\023TPUReplicatedOutput\022\n\n\005input\"\001T\032\032\n\007outputs\"\001T*\014num_replicas\"\027\n\014num_replicas\022\003int(\0010\001\"\t\n\001T\022\004type\n/\n\017WorkerHeartbeat\022\013\n\007request\030\007\032\014\n\010response\030\007\210\001\001")
