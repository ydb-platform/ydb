// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/sparse_ops.h"

namespace tensorflow {
namespace ops {

AddManySparseToTensorsMap::AddManySparseToTensorsMap(const ::tensorflow::Scope&
                                                     scope, ::tensorflow::Input
                                                     sparse_indices,
                                                     ::tensorflow::Input
                                                     sparse_values,
                                                     ::tensorflow::Input
                                                     sparse_shape, const
                                                     AddManySparseToTensorsMap::Attrs&
                                                     attrs) {
  if (!scope.ok()) return;
  auto _sparse_indices = ::tensorflow::ops::AsNodeOut(scope, sparse_indices);
  if (!scope.ok()) return;
  auto _sparse_values = ::tensorflow::ops::AsNodeOut(scope, sparse_values);
  if (!scope.ok()) return;
  auto _sparse_shape = ::tensorflow::ops::AsNodeOut(scope, sparse_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AddManySparseToTensorsMap");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AddManySparseToTensorsMap")
                     .Input(_sparse_indices)
                     .Input(_sparse_values)
                     .Input(_sparse_shape)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->sparse_handles = Output(ret, 0);
}

AddManySparseToTensorsMap::AddManySparseToTensorsMap(const ::tensorflow::Scope&
                                                     scope, ::tensorflow::Input
                                                     sparse_indices,
                                                     ::tensorflow::Input
                                                     sparse_values,
                                                     ::tensorflow::Input
                                                     sparse_shape)
  : AddManySparseToTensorsMap(scope, sparse_indices, sparse_values, sparse_shape, AddManySparseToTensorsMap::Attrs()) {}

AddSparseToTensorsMap::AddSparseToTensorsMap(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input
                                             sparse_indices,
                                             ::tensorflow::Input sparse_values,
                                             ::tensorflow::Input sparse_shape,
                                             const
                                             AddSparseToTensorsMap::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _sparse_indices = ::tensorflow::ops::AsNodeOut(scope, sparse_indices);
  if (!scope.ok()) return;
  auto _sparse_values = ::tensorflow::ops::AsNodeOut(scope, sparse_values);
  if (!scope.ok()) return;
  auto _sparse_shape = ::tensorflow::ops::AsNodeOut(scope, sparse_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AddSparseToTensorsMap");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AddSparseToTensorsMap")
                     .Input(_sparse_indices)
                     .Input(_sparse_values)
                     .Input(_sparse_shape)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->sparse_handle = Output(ret, 0);
}

AddSparseToTensorsMap::AddSparseToTensorsMap(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input
                                             sparse_indices,
                                             ::tensorflow::Input sparse_values,
                                             ::tensorflow::Input sparse_shape)
  : AddSparseToTensorsMap(scope, sparse_indices, sparse_values, sparse_shape, AddSparseToTensorsMap::Attrs()) {}

DeserializeManySparse::DeserializeManySparse(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input
                                             serialized_sparse, DataType dtype) {
  if (!scope.ok()) return;
  auto _serialized_sparse = ::tensorflow::ops::AsNodeOut(scope, serialized_sparse);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DeserializeManySparse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DeserializeManySparse")
                     .Input(_serialized_sparse)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sparse_indices = Output(ret, _outputs_range["sparse_indices"].first);
  this->sparse_values = Output(ret, _outputs_range["sparse_values"].first);
  this->sparse_shape = Output(ret, _outputs_range["sparse_shape"].first);
}

DeserializeSparse::DeserializeSparse(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input serialized_sparse,
                                     DataType dtype) {
  if (!scope.ok()) return;
  auto _serialized_sparse = ::tensorflow::ops::AsNodeOut(scope, serialized_sparse);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DeserializeSparse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DeserializeSparse")
                     .Input(_serialized_sparse)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sparse_indices = Output(ret, _outputs_range["sparse_indices"].first);
  this->sparse_values = Output(ret, _outputs_range["sparse_values"].first);
  this->sparse_shape = Output(ret, _outputs_range["sparse_shape"].first);
}

SerializeManySparse::SerializeManySparse(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input sparse_indices,
                                         ::tensorflow::Input sparse_values,
                                         ::tensorflow::Input sparse_shape,
                                         const SerializeManySparse::Attrs&
                                         attrs) {
  if (!scope.ok()) return;
  auto _sparse_indices = ::tensorflow::ops::AsNodeOut(scope, sparse_indices);
  if (!scope.ok()) return;
  auto _sparse_values = ::tensorflow::ops::AsNodeOut(scope, sparse_values);
  if (!scope.ok()) return;
  auto _sparse_shape = ::tensorflow::ops::AsNodeOut(scope, sparse_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SerializeManySparse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SerializeManySparse")
                     .Input(_sparse_indices)
                     .Input(_sparse_values)
                     .Input(_sparse_shape)
                     .Attr("out_type", attrs.out_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->serialized_sparse = Output(ret, 0);
}

SerializeManySparse::SerializeManySparse(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input sparse_indices,
                                         ::tensorflow::Input sparse_values,
                                         ::tensorflow::Input sparse_shape)
  : SerializeManySparse(scope, sparse_indices, sparse_values, sparse_shape, SerializeManySparse::Attrs()) {}

SerializeSparse::SerializeSparse(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input sparse_indices,
                                 ::tensorflow::Input sparse_values,
                                 ::tensorflow::Input sparse_shape, const
                                 SerializeSparse::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _sparse_indices = ::tensorflow::ops::AsNodeOut(scope, sparse_indices);
  if (!scope.ok()) return;
  auto _sparse_values = ::tensorflow::ops::AsNodeOut(scope, sparse_values);
  if (!scope.ok()) return;
  auto _sparse_shape = ::tensorflow::ops::AsNodeOut(scope, sparse_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SerializeSparse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SerializeSparse")
                     .Input(_sparse_indices)
                     .Input(_sparse_values)
                     .Input(_sparse_shape)
                     .Attr("out_type", attrs.out_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->serialized_sparse = Output(ret, 0);
}

SerializeSparse::SerializeSparse(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input sparse_indices,
                                 ::tensorflow::Input sparse_values,
                                 ::tensorflow::Input sparse_shape)
  : SerializeSparse(scope, sparse_indices, sparse_values, sparse_shape, SerializeSparse::Attrs()) {}

SparseAdd::SparseAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     a_indices, ::tensorflow::Input a_values,
                     ::tensorflow::Input a_shape, ::tensorflow::Input
                     b_indices, ::tensorflow::Input b_values,
                     ::tensorflow::Input b_shape, ::tensorflow::Input thresh) {
  if (!scope.ok()) return;
  auto _a_indices = ::tensorflow::ops::AsNodeOut(scope, a_indices);
  if (!scope.ok()) return;
  auto _a_values = ::tensorflow::ops::AsNodeOut(scope, a_values);
  if (!scope.ok()) return;
  auto _a_shape = ::tensorflow::ops::AsNodeOut(scope, a_shape);
  if (!scope.ok()) return;
  auto _b_indices = ::tensorflow::ops::AsNodeOut(scope, b_indices);
  if (!scope.ok()) return;
  auto _b_values = ::tensorflow::ops::AsNodeOut(scope, b_values);
  if (!scope.ok()) return;
  auto _b_shape = ::tensorflow::ops::AsNodeOut(scope, b_shape);
  if (!scope.ok()) return;
  auto _thresh = ::tensorflow::ops::AsNodeOut(scope, thresh);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseAdd")
                     .Input(_a_indices)
                     .Input(_a_values)
                     .Input(_a_shape)
                     .Input(_b_indices)
                     .Input(_b_values)
                     .Input(_b_shape)
                     .Input(_thresh)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sum_indices = Output(ret, _outputs_range["sum_indices"].first);
  this->sum_values = Output(ret, _outputs_range["sum_values"].first);
  this->sum_shape = Output(ret, _outputs_range["sum_shape"].first);
}

SparseAddGrad::SparseAddGrad(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input backprop_val_grad,
                             ::tensorflow::Input a_indices, ::tensorflow::Input
                             b_indices, ::tensorflow::Input sum_indices) {
  if (!scope.ok()) return;
  auto _backprop_val_grad = ::tensorflow::ops::AsNodeOut(scope, backprop_val_grad);
  if (!scope.ok()) return;
  auto _a_indices = ::tensorflow::ops::AsNodeOut(scope, a_indices);
  if (!scope.ok()) return;
  auto _b_indices = ::tensorflow::ops::AsNodeOut(scope, b_indices);
  if (!scope.ok()) return;
  auto _sum_indices = ::tensorflow::ops::AsNodeOut(scope, sum_indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseAddGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseAddGrad")
                     .Input(_backprop_val_grad)
                     .Input(_a_indices)
                     .Input(_b_indices)
                     .Input(_sum_indices)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->a_val_grad = Output(ret, _outputs_range["a_val_grad"].first);
  this->b_val_grad = Output(ret, _outputs_range["b_val_grad"].first);
}

SparseConcat::SparseConcat(const ::tensorflow::Scope& scope,
                           ::tensorflow::InputList indices,
                           ::tensorflow::InputList values,
                           ::tensorflow::InputList shapes, int64 concat_dim) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOutList(scope, indices);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  auto _shapes = ::tensorflow::ops::AsNodeOutList(scope, shapes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseConcat");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseConcat")
                     .Input(_indices)
                     .Input(_values)
                     .Input(_shapes)
                     .Attr("concat_dim", concat_dim)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
  this->output_shape = Output(ret, _outputs_range["output_shape"].first);
}

SparseCross::SparseCross(const ::tensorflow::Scope& scope,
                         ::tensorflow::InputList indices,
                         ::tensorflow::InputList values,
                         ::tensorflow::InputList shapes,
                         ::tensorflow::InputList dense_inputs, bool
                         hashed_output, int64 num_buckets, int64 hash_key,
                         DataType out_type, DataType internal_type) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOutList(scope, indices);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  auto _shapes = ::tensorflow::ops::AsNodeOutList(scope, shapes);
  if (!scope.ok()) return;
  auto _dense_inputs = ::tensorflow::ops::AsNodeOutList(scope, dense_inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseCross");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseCross")
                     .Input(_indices)
                     .Input(_values)
                     .Input(_shapes)
                     .Input(_dense_inputs)
                     .Attr("hashed_output", hashed_output)
                     .Attr("num_buckets", num_buckets)
                     .Attr("hash_key", hash_key)
                     .Attr("out_type", out_type)
                     .Attr("internal_type", internal_type)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
  this->output_shape = Output(ret, _outputs_range["output_shape"].first);
}

SparseDenseCwiseAdd::SparseDenseCwiseAdd(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input sp_indices,
                                         ::tensorflow::Input sp_values,
                                         ::tensorflow::Input sp_shape,
                                         ::tensorflow::Input dense) {
  if (!scope.ok()) return;
  auto _sp_indices = ::tensorflow::ops::AsNodeOut(scope, sp_indices);
  if (!scope.ok()) return;
  auto _sp_values = ::tensorflow::ops::AsNodeOut(scope, sp_values);
  if (!scope.ok()) return;
  auto _sp_shape = ::tensorflow::ops::AsNodeOut(scope, sp_shape);
  if (!scope.ok()) return;
  auto _dense = ::tensorflow::ops::AsNodeOut(scope, dense);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseDenseCwiseAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseDenseCwiseAdd")
                     .Input(_sp_indices)
                     .Input(_sp_values)
                     .Input(_sp_shape)
                     .Input(_dense)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseDenseCwiseDiv::SparseDenseCwiseDiv(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input sp_indices,
                                         ::tensorflow::Input sp_values,
                                         ::tensorflow::Input sp_shape,
                                         ::tensorflow::Input dense) {
  if (!scope.ok()) return;
  auto _sp_indices = ::tensorflow::ops::AsNodeOut(scope, sp_indices);
  if (!scope.ok()) return;
  auto _sp_values = ::tensorflow::ops::AsNodeOut(scope, sp_values);
  if (!scope.ok()) return;
  auto _sp_shape = ::tensorflow::ops::AsNodeOut(scope, sp_shape);
  if (!scope.ok()) return;
  auto _dense = ::tensorflow::ops::AsNodeOut(scope, dense);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseDenseCwiseDiv");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseDenseCwiseDiv")
                     .Input(_sp_indices)
                     .Input(_sp_values)
                     .Input(_sp_shape)
                     .Input(_dense)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseDenseCwiseMul::SparseDenseCwiseMul(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input sp_indices,
                                         ::tensorflow::Input sp_values,
                                         ::tensorflow::Input sp_shape,
                                         ::tensorflow::Input dense) {
  if (!scope.ok()) return;
  auto _sp_indices = ::tensorflow::ops::AsNodeOut(scope, sp_indices);
  if (!scope.ok()) return;
  auto _sp_values = ::tensorflow::ops::AsNodeOut(scope, sp_values);
  if (!scope.ok()) return;
  auto _sp_shape = ::tensorflow::ops::AsNodeOut(scope, sp_shape);
  if (!scope.ok()) return;
  auto _dense = ::tensorflow::ops::AsNodeOut(scope, dense);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseDenseCwiseMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseDenseCwiseMul")
                     .Input(_sp_indices)
                     .Input(_sp_values)
                     .Input(_sp_shape)
                     .Input(_dense)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseFillEmptyRows::SparseFillEmptyRows(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input indices,
                                         ::tensorflow::Input values,
                                         ::tensorflow::Input dense_shape,
                                         ::tensorflow::Input default_value) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  auto _dense_shape = ::tensorflow::ops::AsNodeOut(scope, dense_shape);
  if (!scope.ok()) return;
  auto _default_value = ::tensorflow::ops::AsNodeOut(scope, default_value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseFillEmptyRows");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseFillEmptyRows")
                     .Input(_indices)
                     .Input(_values)
                     .Input(_dense_shape)
                     .Input(_default_value)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
  this->empty_row_indicator = Output(ret, _outputs_range["empty_row_indicator"].first);
  this->reverse_index_map = Output(ret, _outputs_range["reverse_index_map"].first);
}

SparseFillEmptyRowsGrad::SparseFillEmptyRowsGrad(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 reverse_index_map,
                                                 ::tensorflow::Input
                                                 grad_values) {
  if (!scope.ok()) return;
  auto _reverse_index_map = ::tensorflow::ops::AsNodeOut(scope, reverse_index_map);
  if (!scope.ok()) return;
  auto _grad_values = ::tensorflow::ops::AsNodeOut(scope, grad_values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseFillEmptyRowsGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseFillEmptyRowsGrad")
                     .Input(_reverse_index_map)
                     .Input(_grad_values)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->d_values = Output(ret, _outputs_range["d_values"].first);
  this->d_default_value = Output(ret, _outputs_range["d_default_value"].first);
}

SparseReduceMax::SparseReduceMax(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input_indices,
                                 ::tensorflow::Input input_values,
                                 ::tensorflow::Input input_shape,
                                 ::tensorflow::Input reduction_axes, const
                                 SparseReduceMax::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input_indices = ::tensorflow::ops::AsNodeOut(scope, input_indices);
  if (!scope.ok()) return;
  auto _input_values = ::tensorflow::ops::AsNodeOut(scope, input_values);
  if (!scope.ok()) return;
  auto _input_shape = ::tensorflow::ops::AsNodeOut(scope, input_shape);
  if (!scope.ok()) return;
  auto _reduction_axes = ::tensorflow::ops::AsNodeOut(scope, reduction_axes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseReduceMax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseReduceMax")
                     .Input(_input_indices)
                     .Input(_input_values)
                     .Input(_input_shape)
                     .Input(_reduction_axes)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseReduceMax::SparseReduceMax(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input_indices,
                                 ::tensorflow::Input input_values,
                                 ::tensorflow::Input input_shape,
                                 ::tensorflow::Input reduction_axes)
  : SparseReduceMax(scope, input_indices, input_values, input_shape, reduction_axes, SparseReduceMax::Attrs()) {}

SparseReduceMaxSparse::SparseReduceMaxSparse(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input_indices,
                                             ::tensorflow::Input input_values,
                                             ::tensorflow::Input input_shape,
                                             ::tensorflow::Input
                                             reduction_axes, const
                                             SparseReduceMaxSparse::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _input_indices = ::tensorflow::ops::AsNodeOut(scope, input_indices);
  if (!scope.ok()) return;
  auto _input_values = ::tensorflow::ops::AsNodeOut(scope, input_values);
  if (!scope.ok()) return;
  auto _input_shape = ::tensorflow::ops::AsNodeOut(scope, input_shape);
  if (!scope.ok()) return;
  auto _reduction_axes = ::tensorflow::ops::AsNodeOut(scope, reduction_axes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseReduceMaxSparse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseReduceMaxSparse")
                     .Input(_input_indices)
                     .Input(_input_values)
                     .Input(_input_shape)
                     .Input(_reduction_axes)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
  this->output_shape = Output(ret, _outputs_range["output_shape"].first);
}

SparseReduceMaxSparse::SparseReduceMaxSparse(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input_indices,
                                             ::tensorflow::Input input_values,
                                             ::tensorflow::Input input_shape,
                                             ::tensorflow::Input
                                             reduction_axes)
  : SparseReduceMaxSparse(scope, input_indices, input_values, input_shape, reduction_axes, SparseReduceMaxSparse::Attrs()) {}

SparseReduceSum::SparseReduceSum(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input_indices,
                                 ::tensorflow::Input input_values,
                                 ::tensorflow::Input input_shape,
                                 ::tensorflow::Input reduction_axes, const
                                 SparseReduceSum::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input_indices = ::tensorflow::ops::AsNodeOut(scope, input_indices);
  if (!scope.ok()) return;
  auto _input_values = ::tensorflow::ops::AsNodeOut(scope, input_values);
  if (!scope.ok()) return;
  auto _input_shape = ::tensorflow::ops::AsNodeOut(scope, input_shape);
  if (!scope.ok()) return;
  auto _reduction_axes = ::tensorflow::ops::AsNodeOut(scope, reduction_axes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseReduceSum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseReduceSum")
                     .Input(_input_indices)
                     .Input(_input_values)
                     .Input(_input_shape)
                     .Input(_reduction_axes)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseReduceSum::SparseReduceSum(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input_indices,
                                 ::tensorflow::Input input_values,
                                 ::tensorflow::Input input_shape,
                                 ::tensorflow::Input reduction_axes)
  : SparseReduceSum(scope, input_indices, input_values, input_shape, reduction_axes, SparseReduceSum::Attrs()) {}

SparseReduceSumSparse::SparseReduceSumSparse(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input_indices,
                                             ::tensorflow::Input input_values,
                                             ::tensorflow::Input input_shape,
                                             ::tensorflow::Input
                                             reduction_axes, const
                                             SparseReduceSumSparse::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _input_indices = ::tensorflow::ops::AsNodeOut(scope, input_indices);
  if (!scope.ok()) return;
  auto _input_values = ::tensorflow::ops::AsNodeOut(scope, input_values);
  if (!scope.ok()) return;
  auto _input_shape = ::tensorflow::ops::AsNodeOut(scope, input_shape);
  if (!scope.ok()) return;
  auto _reduction_axes = ::tensorflow::ops::AsNodeOut(scope, reduction_axes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseReduceSumSparse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseReduceSumSparse")
                     .Input(_input_indices)
                     .Input(_input_values)
                     .Input(_input_shape)
                     .Input(_reduction_axes)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
  this->output_shape = Output(ret, _outputs_range["output_shape"].first);
}

SparseReduceSumSparse::SparseReduceSumSparse(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input_indices,
                                             ::tensorflow::Input input_values,
                                             ::tensorflow::Input input_shape,
                                             ::tensorflow::Input
                                             reduction_axes)
  : SparseReduceSumSparse(scope, input_indices, input_values, input_shape, reduction_axes, SparseReduceSumSparse::Attrs()) {}

SparseReorder::SparseReorder(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input input_indices,
                             ::tensorflow::Input input_values,
                             ::tensorflow::Input input_shape) {
  if (!scope.ok()) return;
  auto _input_indices = ::tensorflow::ops::AsNodeOut(scope, input_indices);
  if (!scope.ok()) return;
  auto _input_values = ::tensorflow::ops::AsNodeOut(scope, input_values);
  if (!scope.ok()) return;
  auto _input_shape = ::tensorflow::ops::AsNodeOut(scope, input_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseReorder");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseReorder")
                     .Input(_input_indices)
                     .Input(_input_values)
                     .Input(_input_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
}

SparseReshape::SparseReshape(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input input_indices,
                             ::tensorflow::Input input_shape,
                             ::tensorflow::Input new_shape) {
  if (!scope.ok()) return;
  auto _input_indices = ::tensorflow::ops::AsNodeOut(scope, input_indices);
  if (!scope.ok()) return;
  auto _input_shape = ::tensorflow::ops::AsNodeOut(scope, input_shape);
  if (!scope.ok()) return;
  auto _new_shape = ::tensorflow::ops::AsNodeOut(scope, new_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseReshape");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseReshape")
                     .Input(_input_indices)
                     .Input(_input_shape)
                     .Input(_new_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_shape = Output(ret, _outputs_range["output_shape"].first);
}

SparseSlice::SparseSlice(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         indices, ::tensorflow::Input values,
                         ::tensorflow::Input shape, ::tensorflow::Input start,
                         ::tensorflow::Input size) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  auto _start = ::tensorflow::ops::AsNodeOut(scope, start);
  if (!scope.ok()) return;
  auto _size = ::tensorflow::ops::AsNodeOut(scope, size);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSlice");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSlice")
                     .Input(_indices)
                     .Input(_values)
                     .Input(_shape)
                     .Input(_start)
                     .Input(_size)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
  this->output_shape = Output(ret, _outputs_range["output_shape"].first);
}

SparseSliceGrad::SparseSliceGrad(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input backprop_val_grad,
                                 ::tensorflow::Input input_indices,
                                 ::tensorflow::Input input_start,
                                 ::tensorflow::Input output_indices) {
  if (!scope.ok()) return;
  auto _backprop_val_grad = ::tensorflow::ops::AsNodeOut(scope, backprop_val_grad);
  if (!scope.ok()) return;
  auto _input_indices = ::tensorflow::ops::AsNodeOut(scope, input_indices);
  if (!scope.ok()) return;
  auto _input_start = ::tensorflow::ops::AsNodeOut(scope, input_start);
  if (!scope.ok()) return;
  auto _output_indices = ::tensorflow::ops::AsNodeOut(scope, output_indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSliceGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSliceGrad")
                     .Input(_backprop_val_grad)
                     .Input(_input_indices)
                     .Input(_input_start)
                     .Input(_output_indices)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->val_grad = Output(ret, 0);
}

SparseSoftmax::SparseSoftmax(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input sp_indices,
                             ::tensorflow::Input sp_values, ::tensorflow::Input
                             sp_shape) {
  if (!scope.ok()) return;
  auto _sp_indices = ::tensorflow::ops::AsNodeOut(scope, sp_indices);
  if (!scope.ok()) return;
  auto _sp_values = ::tensorflow::ops::AsNodeOut(scope, sp_values);
  if (!scope.ok()) return;
  auto _sp_shape = ::tensorflow::ops::AsNodeOut(scope, sp_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSoftmax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSoftmax")
                     .Input(_sp_indices)
                     .Input(_sp_values)
                     .Input(_sp_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSparseMaximum::SparseSparseMaximum(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input a_indices,
                                         ::tensorflow::Input a_values,
                                         ::tensorflow::Input a_shape,
                                         ::tensorflow::Input b_indices,
                                         ::tensorflow::Input b_values,
                                         ::tensorflow::Input b_shape) {
  if (!scope.ok()) return;
  auto _a_indices = ::tensorflow::ops::AsNodeOut(scope, a_indices);
  if (!scope.ok()) return;
  auto _a_values = ::tensorflow::ops::AsNodeOut(scope, a_values);
  if (!scope.ok()) return;
  auto _a_shape = ::tensorflow::ops::AsNodeOut(scope, a_shape);
  if (!scope.ok()) return;
  auto _b_indices = ::tensorflow::ops::AsNodeOut(scope, b_indices);
  if (!scope.ok()) return;
  auto _b_values = ::tensorflow::ops::AsNodeOut(scope, b_values);
  if (!scope.ok()) return;
  auto _b_shape = ::tensorflow::ops::AsNodeOut(scope, b_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSparseMaximum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSparseMaximum")
                     .Input(_a_indices)
                     .Input(_a_values)
                     .Input(_a_shape)
                     .Input(_b_indices)
                     .Input(_b_values)
                     .Input(_b_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
}

SparseSparseMinimum::SparseSparseMinimum(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input a_indices,
                                         ::tensorflow::Input a_values,
                                         ::tensorflow::Input a_shape,
                                         ::tensorflow::Input b_indices,
                                         ::tensorflow::Input b_values,
                                         ::tensorflow::Input b_shape) {
  if (!scope.ok()) return;
  auto _a_indices = ::tensorflow::ops::AsNodeOut(scope, a_indices);
  if (!scope.ok()) return;
  auto _a_values = ::tensorflow::ops::AsNodeOut(scope, a_values);
  if (!scope.ok()) return;
  auto _a_shape = ::tensorflow::ops::AsNodeOut(scope, a_shape);
  if (!scope.ok()) return;
  auto _b_indices = ::tensorflow::ops::AsNodeOut(scope, b_indices);
  if (!scope.ok()) return;
  auto _b_values = ::tensorflow::ops::AsNodeOut(scope, b_values);
  if (!scope.ok()) return;
  auto _b_shape = ::tensorflow::ops::AsNodeOut(scope, b_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSparseMinimum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSparseMinimum")
                     .Input(_a_indices)
                     .Input(_a_values)
                     .Input(_a_shape)
                     .Input(_b_indices)
                     .Input(_b_values)
                     .Input(_b_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->output_indices = Output(ret, _outputs_range["output_indices"].first);
  this->output_values = Output(ret, _outputs_range["output_values"].first);
}

SparseSplit::SparseSplit(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         split_dim, ::tensorflow::Input indices,
                         ::tensorflow::Input values, ::tensorflow::Input shape,
                         int64 num_split) {
  if (!scope.ok()) return;
  auto _split_dim = ::tensorflow::ops::AsNodeOut(scope, split_dim);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSplit");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSplit")
                     .Input(_split_dim)
                     .Input(_indices)
                     .Input(_values)
                     .Input(_shape)
                     .Attr("num_split", num_split)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  for (int32 i = _outputs_range["output_indices"].first; i < _outputs_range["output_indices"].second; ++i)
    this->output_indices.push_back(Output(ret, i));
  for (int32 i = _outputs_range["output_values"].first; i < _outputs_range["output_values"].second; ++i)
    this->output_values.push_back(Output(ret, i));
  for (int32 i = _outputs_range["output_shape"].first; i < _outputs_range["output_shape"].second; ++i)
    this->output_shape.push_back(Output(ret, i));
}

SparseTensorDenseAdd::SparseTensorDenseAdd(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input a_indices,
                                           ::tensorflow::Input a_values,
                                           ::tensorflow::Input a_shape,
                                           ::tensorflow::Input b) {
  if (!scope.ok()) return;
  auto _a_indices = ::tensorflow::ops::AsNodeOut(scope, a_indices);
  if (!scope.ok()) return;
  auto _a_values = ::tensorflow::ops::AsNodeOut(scope, a_values);
  if (!scope.ok()) return;
  auto _a_shape = ::tensorflow::ops::AsNodeOut(scope, a_shape);
  if (!scope.ok()) return;
  auto _b = ::tensorflow::ops::AsNodeOut(scope, b);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseTensorDenseAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseTensorDenseAdd")
                     .Input(_a_indices)
                     .Input(_a_values)
                     .Input(_a_shape)
                     .Input(_b)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseTensorDenseMatMul::SparseTensorDenseMatMul(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 a_indices, ::tensorflow::Input
                                                 a_values, ::tensorflow::Input
                                                 a_shape, ::tensorflow::Input
                                                 b, const
                                                 SparseTensorDenseMatMul::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _a_indices = ::tensorflow::ops::AsNodeOut(scope, a_indices);
  if (!scope.ok()) return;
  auto _a_values = ::tensorflow::ops::AsNodeOut(scope, a_values);
  if (!scope.ok()) return;
  auto _a_shape = ::tensorflow::ops::AsNodeOut(scope, a_shape);
  if (!scope.ok()) return;
  auto _b = ::tensorflow::ops::AsNodeOut(scope, b);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseTensorDenseMatMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseTensorDenseMatMul")
                     .Input(_a_indices)
                     .Input(_a_values)
                     .Input(_a_shape)
                     .Input(_b)
                     .Attr("adjoint_a", attrs.adjoint_a_)
                     .Attr("adjoint_b", attrs.adjoint_b_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->product = Output(ret, 0);
}

SparseTensorDenseMatMul::SparseTensorDenseMatMul(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 a_indices, ::tensorflow::Input
                                                 a_values, ::tensorflow::Input
                                                 a_shape, ::tensorflow::Input
                                                 b)
  : SparseTensorDenseMatMul(scope, a_indices, a_values, a_shape, b, SparseTensorDenseMatMul::Attrs()) {}

SparseToDense::SparseToDense(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input sparse_indices,
                             ::tensorflow::Input output_shape,
                             ::tensorflow::Input sparse_values,
                             ::tensorflow::Input default_value, const
                             SparseToDense::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _sparse_indices = ::tensorflow::ops::AsNodeOut(scope, sparse_indices);
  if (!scope.ok()) return;
  auto _output_shape = ::tensorflow::ops::AsNodeOut(scope, output_shape);
  if (!scope.ok()) return;
  auto _sparse_values = ::tensorflow::ops::AsNodeOut(scope, sparse_values);
  if (!scope.ok()) return;
  auto _default_value = ::tensorflow::ops::AsNodeOut(scope, default_value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseToDense");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseToDense")
                     .Input(_sparse_indices)
                     .Input(_output_shape)
                     .Input(_sparse_values)
                     .Input(_default_value)
                     .Attr("validate_indices", attrs.validate_indices_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->dense = Output(ret, 0);
}

SparseToDense::SparseToDense(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input sparse_indices,
                             ::tensorflow::Input output_shape,
                             ::tensorflow::Input sparse_values,
                             ::tensorflow::Input default_value)
  : SparseToDense(scope, sparse_indices, output_shape, sparse_values, default_value, SparseToDense::Attrs()) {}

TakeManySparseFromTensorsMap::TakeManySparseFromTensorsMap(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           sparse_handles,
                                                           DataType dtype,
                                                           const
                                                           TakeManySparseFromTensorsMap::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _sparse_handles = ::tensorflow::ops::AsNodeOut(scope, sparse_handles);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TakeManySparseFromTensorsMap");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TakeManySparseFromTensorsMap")
                     .Input(_sparse_handles)
                     .Attr("dtype", dtype)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sparse_indices = Output(ret, _outputs_range["sparse_indices"].first);
  this->sparse_values = Output(ret, _outputs_range["sparse_values"].first);
  this->sparse_shape = Output(ret, _outputs_range["sparse_shape"].first);
}

TakeManySparseFromTensorsMap::TakeManySparseFromTensorsMap(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           sparse_handles,
                                                           DataType dtype)
  : TakeManySparseFromTensorsMap(scope, sparse_handles, dtype, TakeManySparseFromTensorsMap::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
