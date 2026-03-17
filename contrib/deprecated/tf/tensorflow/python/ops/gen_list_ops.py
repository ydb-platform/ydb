"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: list_ops.cc
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


@tf_export('empty_tensor_list')
def empty_tensor_list(element_shape, element_dtype, name=None):
  r"""Creates and returns an empty tensor list.

  All list elements must be tensors of dtype element_dtype and shape compatible
  with element_shape.

  handle: an empty tensor list.
  element_dtype: the type of elements in the list.
  element_shape: a shape compatible with that of elements in the list.

  Args:
    element_shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    element_dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    element_dtype = _execute.make_type(element_dtype, "element_dtype")
    _, _, _op = _op_def_lib._apply_op_helper(
        "EmptyTensorList", element_shape=element_shape,
        element_dtype=element_dtype, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"), "shape_type",
              _op.get_attr("shape_type"))
    _execute.record_gradient(
      "EmptyTensorList", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "EmptyTensorList", name, _ctx._post_execution_callbacks,
        element_shape, "element_dtype", element_dtype)
      return _result
    except _core._FallbackException:
      return empty_tensor_list_eager_fallback(
          element_shape, element_dtype=element_dtype, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def empty_tensor_list_eager_fallback(element_shape, element_dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function empty_tensor_list
  """
  _ctx = ctx if ctx else _context.context()
  element_dtype = _execute.make_type(element_dtype, "element_dtype")
  _attr_shape_type, (element_shape,) = _execute.args_to_matching_eager([element_shape], _ctx)
  _inputs_flat = [element_shape]
  _attrs = ("element_dtype", element_dtype, "shape_type", _attr_shape_type)
  _result = _execute.execute(b"EmptyTensorList", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EmptyTensorList", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_concat_lists')
def tensor_list_concat_lists(input_a, input_b, element_dtype, name=None):
  r"""TODO: add doc.

  Args:
    input_a: A `Tensor` of type `variant`.
    input_b: A `Tensor` of type `variant`.
    element_dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    element_dtype = _execute.make_type(element_dtype, "element_dtype")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListConcatLists", input_a=input_a, input_b=input_b,
        element_dtype=element_dtype, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"))
    _execute.record_gradient(
      "TensorListConcatLists", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListConcatLists", name, _ctx._post_execution_callbacks,
        input_a, input_b, "element_dtype", element_dtype)
      return _result
    except _core._FallbackException:
      return tensor_list_concat_lists_eager_fallback(
          input_a, input_b, element_dtype=element_dtype, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_concat_lists_eager_fallback(input_a, input_b, element_dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_concat_lists
  """
  _ctx = ctx if ctx else _context.context()
  element_dtype = _execute.make_type(element_dtype, "element_dtype")
  input_a = _ops.convert_to_tensor(input_a, _dtypes.variant)
  input_b = _ops.convert_to_tensor(input_b, _dtypes.variant)
  _inputs_flat = [input_a, input_b]
  _attrs = ("element_dtype", element_dtype)
  _result = _execute.execute(b"TensorListConcatLists", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListConcatLists", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_element_shape')
def tensor_list_element_shape(input_handle, shape_type, name=None):
  r"""The shape of the elements of the given list, as a tensor.

    input_handle: the list
    element_shape: the shape of elements of the list

  Args:
    input_handle: A `Tensor` of type `variant`.
    shape_type: A `tf.DType` from: `tf.int32, tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `shape_type`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    shape_type = _execute.make_type(shape_type, "shape_type")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListElementShape", input_handle=input_handle,
        shape_type=shape_type, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("shape_type", _op.get_attr("shape_type"))
    _execute.record_gradient(
      "TensorListElementShape", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListElementShape", name, _ctx._post_execution_callbacks,
        input_handle, "shape_type", shape_type)
      return _result
    except _core._FallbackException:
      return tensor_list_element_shape_eager_fallback(
          input_handle, shape_type=shape_type, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_element_shape_eager_fallback(input_handle, shape_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_element_shape
  """
  _ctx = ctx if ctx else _context.context()
  shape_type = _execute.make_type(shape_type, "shape_type")
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  _inputs_flat = [input_handle]
  _attrs = ("shape_type", shape_type)
  _result = _execute.execute(b"TensorListElementShape", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TensorListElementShape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_from_tensor')
def tensor_list_from_tensor(tensor, element_shape, name=None):
  r"""Creates a TensorList which, when stacked, has the value of `tensor`.

  Each tensor in the result list corresponds to one row of the input tensor.

  tensor: The input tensor.
  output_handle: The list.

  Args:
    tensor: A `Tensor`.
    element_shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListFromTensor", tensor=tensor, element_shape=element_shape,
        name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"), "shape_type",
              _op.get_attr("shape_type"))
    _execute.record_gradient(
      "TensorListFromTensor", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListFromTensor", name, _ctx._post_execution_callbacks, tensor,
        element_shape)
      return _result
    except _core._FallbackException:
      return tensor_list_from_tensor_eager_fallback(
          tensor, element_shape, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_from_tensor_eager_fallback(tensor, element_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_from_tensor
  """
  _ctx = ctx if ctx else _context.context()
  _attr_element_dtype, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  _attr_shape_type, (element_shape,) = _execute.args_to_matching_eager([element_shape], _ctx)
  _inputs_flat = [tensor, element_shape]
  _attrs = ("element_dtype", _attr_element_dtype, "shape_type",
  _attr_shape_type)
  _result = _execute.execute(b"TensorListFromTensor", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListFromTensor", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_get_item')
def tensor_list_get_item(input_handle, index, element_dtype, name=None):
  r"""Returns the item in the list with the given index.

  input_handle: the list
  index: the position in the list from which an element will be retrieved
  item: the element at that position

  Args:
    input_handle: A `Tensor` of type `variant`.
    index: A `Tensor` of type `int32`.
    element_dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `element_dtype`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    element_dtype = _execute.make_type(element_dtype, "element_dtype")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListGetItem", input_handle=input_handle, index=index,
        element_dtype=element_dtype, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"))
    _execute.record_gradient(
      "TensorListGetItem", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListGetItem", name, _ctx._post_execution_callbacks,
        input_handle, index, "element_dtype", element_dtype)
      return _result
    except _core._FallbackException:
      return tensor_list_get_item_eager_fallback(
          input_handle, index, element_dtype=element_dtype, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_get_item_eager_fallback(input_handle, index, element_dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_get_item
  """
  _ctx = ctx if ctx else _context.context()
  element_dtype = _execute.make_type(element_dtype, "element_dtype")
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  index = _ops.convert_to_tensor(index, _dtypes.int32)
  _inputs_flat = [input_handle, index]
  _attrs = ("element_dtype", element_dtype)
  _result = _execute.execute(b"TensorListGetItem", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListGetItem", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_length')
def tensor_list_length(input_handle, name=None):
  r"""Returns the number of tensors in the input tensor list.

  input_handle: the input list
  length: the number of tensors in the list

  Args:
    input_handle: A `Tensor` of type `variant`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListLength", input_handle=input_handle, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = None
    _execute.record_gradient(
      "TensorListLength", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListLength", name, _ctx._post_execution_callbacks,
        input_handle)
      return _result
    except _core._FallbackException:
      return tensor_list_length_eager_fallback(
          input_handle, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_length_eager_fallback(input_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_length
  """
  _ctx = ctx if ctx else _context.context()
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  _inputs_flat = [input_handle]
  _attrs = None
  _result = _execute.execute(b"TensorListLength", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListLength", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_tensor_list_pop_back_outputs = ["output_handle", "tensor"]
_TensorListPopBackOutput = _collections.namedtuple(
    "TensorListPopBack", _tensor_list_pop_back_outputs)


@tf_export('tensor_list_pop_back')
def tensor_list_pop_back(input_handle, element_dtype, name=None):
  r"""Returns the last element of the input list as well as a list with all but that element.

  Fails if the list is empty.

  input_handle: the input list
  tensor: the withdrawn last element of the list
  element_dtype: the type of elements in the list
  element_shape: the shape of the output tensor

  Args:
    input_handle: A `Tensor` of type `variant`.
    element_dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_handle, tensor).

    output_handle: A `Tensor` of type `variant`.
    tensor: A `Tensor` of type `element_dtype`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    element_dtype = _execute.make_type(element_dtype, "element_dtype")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListPopBack", input_handle=input_handle,
        element_dtype=element_dtype, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"))
    _execute.record_gradient(
      "TensorListPopBack", _inputs_flat, _attrs, _result, name)
    _result = _TensorListPopBackOutput._make(_result)
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListPopBack", name, _ctx._post_execution_callbacks,
        input_handle, "element_dtype", element_dtype)
      _result = _TensorListPopBackOutput._make(_result)
      return _result
    except _core._FallbackException:
      return tensor_list_pop_back_eager_fallback(
          input_handle, element_dtype=element_dtype, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_pop_back_eager_fallback(input_handle, element_dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_pop_back
  """
  _ctx = ctx if ctx else _context.context()
  element_dtype = _execute.make_type(element_dtype, "element_dtype")
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  _inputs_flat = [input_handle]
  _attrs = ("element_dtype", element_dtype)
  _result = _execute.execute(b"TensorListPopBack", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListPopBack", _inputs_flat, _attrs, _result, name)
  _result = _TensorListPopBackOutput._make(_result)
  return _result


@tf_export('tensor_list_push_back')
def tensor_list_push_back(input_handle, tensor, name=None):
  r"""Returns a list list which has the passed-in `Tensor` as last element and the other elements of the given list in `input_handle`.

  tensor: The tensor to put on the list.
  input_handle: The old list.
  output_handle: A list with the elements of the old list followed by tensor.
  element_dtype: the type of elements in the list.
  element_shape: a shape compatible with that of elements in the list.

  Args:
    input_handle: A `Tensor` of type `variant`.
    tensor: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListPushBack", input_handle=input_handle, tensor=tensor,
        name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"))
    _execute.record_gradient(
      "TensorListPushBack", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListPushBack", name, _ctx._post_execution_callbacks,
        input_handle, tensor)
      return _result
    except _core._FallbackException:
      return tensor_list_push_back_eager_fallback(
          input_handle, tensor, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_push_back_eager_fallback(input_handle, tensor, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_push_back
  """
  _ctx = ctx if ctx else _context.context()
  _attr_element_dtype, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  _inputs_flat = [input_handle, tensor]
  _attrs = ("element_dtype", _attr_element_dtype)
  _result = _execute.execute(b"TensorListPushBack", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListPushBack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_push_back_batch')
def tensor_list_push_back_batch(input_handles, tensor, name=None):
  r"""TODO: add doc.

  Args:
    input_handles: A `Tensor` of type `variant`.
    tensor: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListPushBackBatch", input_handles=input_handles, tensor=tensor,
        name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"))
    _execute.record_gradient(
      "TensorListPushBackBatch", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListPushBackBatch", name, _ctx._post_execution_callbacks,
        input_handles, tensor)
      return _result
    except _core._FallbackException:
      return tensor_list_push_back_batch_eager_fallback(
          input_handles, tensor, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_push_back_batch_eager_fallback(input_handles, tensor, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_push_back_batch
  """
  _ctx = ctx if ctx else _context.context()
  _attr_element_dtype, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  input_handles = _ops.convert_to_tensor(input_handles, _dtypes.variant)
  _inputs_flat = [input_handles, tensor]
  _attrs = ("element_dtype", _attr_element_dtype)
  _result = _execute.execute(b"TensorListPushBackBatch", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TensorListPushBackBatch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_reserve')
def tensor_list_reserve(element_shape, num_elements, element_dtype, name=None):
  r"""List of the given size with empty elements.

  element_shape: the shape of the future elements of the list
  num_elements: the number of elements to reserve
  handle: the output list
  element_dtype: the desired type of elements in the list.

  Args:
    element_shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    num_elements: A `Tensor` of type `int32`.
    element_dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    element_dtype = _execute.make_type(element_dtype, "element_dtype")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListReserve", element_shape=element_shape,
        num_elements=num_elements, element_dtype=element_dtype, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"), "shape_type",
              _op.get_attr("shape_type"))
    _execute.record_gradient(
      "TensorListReserve", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListReserve", name, _ctx._post_execution_callbacks,
        element_shape, num_elements, "element_dtype", element_dtype)
      return _result
    except _core._FallbackException:
      return tensor_list_reserve_eager_fallback(
          element_shape, num_elements, element_dtype=element_dtype, name=name,
          ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_reserve_eager_fallback(element_shape, num_elements, element_dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_reserve
  """
  _ctx = ctx if ctx else _context.context()
  element_dtype = _execute.make_type(element_dtype, "element_dtype")
  _attr_shape_type, (element_shape,) = _execute.args_to_matching_eager([element_shape], _ctx)
  num_elements = _ops.convert_to_tensor(num_elements, _dtypes.int32)
  _inputs_flat = [element_shape, num_elements]
  _attrs = ("element_dtype", element_dtype, "shape_type", _attr_shape_type)
  _result = _execute.execute(b"TensorListReserve", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListReserve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_set_item')
def tensor_list_set_item(input_handle, index, item, name=None):
  r"""Sets the index-th position of the list to contain the given tensor.

  input_handle: the list
  index: the position in the list to which the tensor will be assigned
  item: the element to be assigned to that position
  output_handle: the new list, with the element in the proper position

  Args:
    input_handle: A `Tensor` of type `variant`.
    index: A `Tensor` of type `int32`.
    item: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListSetItem", input_handle=input_handle, index=index,
        item=item, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"))
    _execute.record_gradient(
      "TensorListSetItem", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListSetItem", name, _ctx._post_execution_callbacks,
        input_handle, index, item)
      return _result
    except _core._FallbackException:
      return tensor_list_set_item_eager_fallback(
          input_handle, index, item, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_set_item_eager_fallback(input_handle, index, item, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_set_item
  """
  _ctx = ctx if ctx else _context.context()
  _attr_element_dtype, (item,) = _execute.args_to_matching_eager([item], _ctx)
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  index = _ops.convert_to_tensor(index, _dtypes.int32)
  _inputs_flat = [input_handle, index, item]
  _attrs = ("element_dtype", _attr_element_dtype)
  _result = _execute.execute(b"TensorListSetItem", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListSetItem", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@tf_export('tensor_list_stack')
def tensor_list_stack(input_handle, element_dtype, num_elements=-1, name=None):
  r"""Stacks all tensors in the list.

  Requires that all tensors have the same shape.

  input_handle: the input list
  tensor: the gathered result
  num_elements: optional. If not -1, the number of elements in the list.

  Args:
    input_handle: A `Tensor` of type `variant`.
    element_dtype: A `tf.DType`.
    num_elements: An optional `int`. Defaults to `-1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `element_dtype`.
  """
  _ctx = _context._context
  if _ctx is None or not _ctx._eager_context.is_eager:
    element_dtype = _execute.make_type(element_dtype, "element_dtype")
    if num_elements is None:
      num_elements = -1
    num_elements = _execute.make_int(num_elements, "num_elements")
    _, _, _op = _op_def_lib._apply_op_helper(
        "TensorListStack", input_handle=input_handle,
        element_dtype=element_dtype, num_elements=num_elements, name=name)
    _result = _op.outputs[:]
    _inputs_flat = _op.inputs
    _attrs = ("element_dtype", _op.get_attr("element_dtype"), "num_elements",
              _op.get_attr("num_elements"))
    _execute.record_gradient(
      "TensorListStack", _inputs_flat, _attrs, _result, name)
    _result, = _result
    return _result

  else:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorListStack", name, _ctx._post_execution_callbacks, input_handle,
        "element_dtype", element_dtype, "num_elements", num_elements)
      return _result
    except _core._FallbackException:
      return tensor_list_stack_eager_fallback(
          input_handle, element_dtype=element_dtype,
          num_elements=num_elements, name=name, ctx=_ctx)
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)


def tensor_list_stack_eager_fallback(input_handle, element_dtype, num_elements=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_list_stack
  """
  _ctx = ctx if ctx else _context.context()
  element_dtype = _execute.make_type(element_dtype, "element_dtype")
  if num_elements is None:
    num_elements = -1
  num_elements = _execute.make_int(num_elements, "num_elements")
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  _inputs_flat = [input_handle]
  _attrs = ("element_dtype", element_dtype, "num_elements", num_elements)
  _result = _execute.execute(b"TensorListStack", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorListStack", _inputs_flat, _attrs, _result, name)
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
#   name: "EmptyTensorList"
#   input_arg {
#     name: "element_shape"
#     type_attr: "shape_type"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TensorListConcatLists"
#   input_arg {
#     name: "input_a"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "input_b"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "output"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
# }
# op {
#   name: "TensorListElementShape"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "element_shape"
#     type_attr: "shape_type"
#   }
#   attr {
#     name: "shape_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TensorListFromTensor"
#   input_arg {
#     name: "tensor"
#     type_attr: "element_dtype"
#   }
#   input_arg {
#     name: "element_shape"
#     type_attr: "shape_type"
#   }
#   output_arg {
#     name: "output_handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TensorListGetItem"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "item"
#     type_attr: "element_dtype"
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
# }
# op {
#   name: "TensorListLength"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "length"
#     type: DT_INT32
#   }
# }
# op {
#   name: "TensorListPopBack"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "output_handle"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "tensor"
#     type_attr: "element_dtype"
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
# }
# op {
#   name: "TensorListPushBack"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "tensor"
#     type_attr: "element_dtype"
#   }
#   output_arg {
#     name: "output_handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
# }
# op {
#   name: "TensorListPushBackBatch"
#   input_arg {
#     name: "input_handles"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "tensor"
#     type_attr: "element_dtype"
#   }
#   output_arg {
#     name: "output_handles"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
# }
# op {
#   name: "TensorListReserve"
#   input_arg {
#     name: "element_shape"
#     type_attr: "shape_type"
#   }
#   input_arg {
#     name: "num_elements"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TensorListSetItem"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "item"
#     type_attr: "element_dtype"
#   }
#   output_arg {
#     name: "output_handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
# }
# op {
#   name: "TensorListStack"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "tensor"
#     type_attr: "element_dtype"
#   }
#   attr {
#     name: "element_dtype"
#     type: "type"
#   }
#   attr {
#     name: "num_elements"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nm\n\017EmptyTensorList\022\033\n\relement_shape\"\nshape_type\032\n\n\006handle\030\025\"\025\n\relement_dtype\022\004type\"\032\n\nshape_type\022\004type:\006\n\0042\002\003\t\nT\n\025TensorListConcatLists\022\013\n\007input_a\030\025\022\013\n\007input_b\030\025\032\n\n\006output\030\025\"\025\n\relement_dtype\022\004type\nc\n\026TensorListElementShape\022\020\n\014input_handle\030\025\032\033\n\relement_shape\"\nshape_type\"\032\n\nshape_type\022\004type:\006\n\0042\002\003\t\n\222\001\n\024TensorListFromTensor\022\027\n\006tensor\"\relement_dtype\022\033\n\relement_shape\"\nshape_type\032\021\n\routput_handle\030\025\"\025\n\relement_dtype\022\004type\"\032\n\nshape_type\022\004type:\006\n\0042\002\003\t\n^\n\021TensorListGetItem\022\020\n\014input_handle\030\025\022\t\n\005index\030\003\032\025\n\004item\"\relement_dtype\"\025\n\relement_dtype\022\004type\n0\n\020TensorListLength\022\020\n\014input_handle\030\025\032\n\n\006length\030\003\nh\n\021TensorListPopBack\022\020\n\014input_handle\030\025\032\021\n\routput_handle\030\025\032\027\n\006tensor\"\relement_dtype\"\025\n\relement_dtype\022\004type\ni\n\022TensorListPushBack\022\020\n\014input_handle\030\025\022\027\n\006tensor\"\relement_dtype\032\021\n\routput_handle\030\025\"\025\n\relement_dtype\022\004type\np\n\027TensorListPushBackBatch\022\021\n\rinput_handles\030\025\022\027\n\006tensor\"\relement_dtype\032\022\n\016output_handles\030\025\"\025\n\relement_dtype\022\004type\n\201\001\n\021TensorListReserve\022\033\n\relement_shape\"\nshape_type\022\020\n\014num_elements\030\003\032\n\n\006handle\030\025\"\025\n\relement_dtype\022\004type\"\032\n\nshape_type\022\004type:\006\n\0042\002\003\t\nq\n\021TensorListSetItem\022\020\n\014input_handle\030\025\022\t\n\005index\030\003\022\025\n\004item\"\relement_dtype\032\021\n\routput_handle\030\025\"\025\n\relement_dtype\022\004type\nu\n\017TensorListStack\022\020\n\014input_handle\030\025\032\027\n\006tensor\"\relement_dtype\"\025\n\relement_dtype\022\004type\" \n\014num_elements\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001")
