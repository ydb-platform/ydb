// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/data_flow_ops.h"

namespace tensorflow {
namespace ops {

AccumulatorApplyGradient::AccumulatorApplyGradient(const ::tensorflow::Scope&
                                                   scope, ::tensorflow::Input
                                                   handle, ::tensorflow::Input
                                                   local_step,
                                                   ::tensorflow::Input
                                                   gradient) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _local_step = ::tensorflow::ops::AsNodeOut(scope, local_step);
  if (!scope.ok()) return;
  auto _gradient = ::tensorflow::ops::AsNodeOut(scope, gradient);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AccumulatorApplyGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AccumulatorApplyGradient")
                     .Input(_handle)
                     .Input(_local_step)
                     .Input(_gradient)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

AccumulatorNumAccumulated::AccumulatorNumAccumulated(const ::tensorflow::Scope&
                                                     scope, ::tensorflow::Input
                                                     handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AccumulatorNumAccumulated");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AccumulatorNumAccumulated")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->num_accumulated = Output(ret, 0);
}

AccumulatorSetGlobalStep::AccumulatorSetGlobalStep(const ::tensorflow::Scope&
                                                   scope, ::tensorflow::Input
                                                   handle, ::tensorflow::Input
                                                   new_global_step) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _new_global_step = ::tensorflow::ops::AsNodeOut(scope, new_global_step);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AccumulatorSetGlobalStep");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AccumulatorSetGlobalStep")
                     .Input(_handle)
                     .Input(_new_global_step)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

AccumulatorTakeGradient::AccumulatorTakeGradient(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 handle, ::tensorflow::Input
                                                 num_required, DataType dtype) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _num_required = ::tensorflow::ops::AsNodeOut(scope, num_required);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AccumulatorTakeGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AccumulatorTakeGradient")
                     .Input(_handle)
                     .Input(_num_required)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->average = Output(ret, 0);
}

Barrier::Barrier(const ::tensorflow::Scope& scope, const DataTypeSlice&
                 component_types, const Barrier::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Barrier");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Barrier")
                     .Attr("component_types", component_types)
                     .Attr("shapes", attrs.shapes_)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

Barrier::Barrier(const ::tensorflow::Scope& scope, const DataTypeSlice&
                 component_types)
  : Barrier(scope, component_types, Barrier::Attrs()) {}

BarrierClose::BarrierClose(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input handle, const
                           BarrierClose::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BarrierClose");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BarrierClose")
                     .Input(_handle)
                     .Attr("cancel_pending_enqueues", attrs.cancel_pending_enqueues_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

BarrierClose::BarrierClose(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input handle)
  : BarrierClose(scope, handle, BarrierClose::Attrs()) {}

BarrierIncompleteSize::BarrierIncompleteSize(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BarrierIncompleteSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BarrierIncompleteSize")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

BarrierInsertMany::BarrierInsertMany(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input handle,
                                     ::tensorflow::Input keys,
                                     ::tensorflow::Input values, int64
                                     component_index) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _keys = ::tensorflow::ops::AsNodeOut(scope, keys);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BarrierInsertMany");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BarrierInsertMany")
                     .Input(_handle)
                     .Input(_keys)
                     .Input(_values)
                     .Attr("component_index", component_index)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

BarrierReadySize::BarrierReadySize(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BarrierReadySize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BarrierReadySize")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

BarrierTakeMany::BarrierTakeMany(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input handle,
                                 ::tensorflow::Input num_elements, const
                                 DataTypeSlice& component_types, const
                                 BarrierTakeMany::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _num_elements = ::tensorflow::ops::AsNodeOut(scope, num_elements);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BarrierTakeMany");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BarrierTakeMany")
                     .Input(_handle)
                     .Input(_num_elements)
                     .Attr("component_types", component_types)
                     .Attr("allow_small_batch", attrs.allow_small_batch_)
                     .Attr("wait_for_incomplete", attrs.wait_for_incomplete_)
                     .Attr("timeout_ms", attrs.timeout_ms_)
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

  this->indices = Output(ret, _outputs_range["indices"].first);
  this->keys = Output(ret, _outputs_range["keys"].first);
  for (int32 i = _outputs_range["values"].first; i < _outputs_range["values"].second; ++i)
    this->values.push_back(Output(ret, i));
}

BarrierTakeMany::BarrierTakeMany(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input handle,
                                 ::tensorflow::Input num_elements, const
                                 DataTypeSlice& component_types)
  : BarrierTakeMany(scope, handle, num_elements, component_types, BarrierTakeMany::Attrs()) {}

ConditionalAccumulator::ConditionalAccumulator(const ::tensorflow::Scope&
                                               scope, DataType dtype,
                                               PartialTensorShape shape, const
                                               ConditionalAccumulator::Attrs&
                                               attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ConditionalAccumulator");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ConditionalAccumulator")
                     .Attr("dtype", dtype)
                     .Attr("shape", shape)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

ConditionalAccumulator::ConditionalAccumulator(const ::tensorflow::Scope&
                                               scope, DataType dtype,
                                               PartialTensorShape shape)
  : ConditionalAccumulator(scope, dtype, shape, ConditionalAccumulator::Attrs()) {}

DeleteSessionTensor::DeleteSessionTensor(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DeleteSessionTensor");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DeleteSessionTensor")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

DynamicPartition::DynamicPartition(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input data,
                                   ::tensorflow::Input partitions, int64
                                   num_partitions) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _partitions = ::tensorflow::ops::AsNodeOut(scope, partitions);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DynamicPartition");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DynamicPartition")
                     .Input(_data)
                     .Input(_partitions)
                     .Attr("num_partitions", num_partitions)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->outputs.push_back(Output(ret, i));
}

DynamicStitch::DynamicStitch(const ::tensorflow::Scope& scope,
                             ::tensorflow::InputList indices,
                             ::tensorflow::InputList data) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOutList(scope, indices);
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOutList(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DynamicStitch");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DynamicStitch")
                     .Input(_indices)
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->merged = Output(ret, 0);
}

FIFOQueue::FIFOQueue(const ::tensorflow::Scope& scope, const DataTypeSlice&
                     component_types, const FIFOQueue::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FIFOQueue");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FIFOQueueV2")
                     .Attr("component_types", component_types)
                     .Attr("shapes", attrs.shapes_)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

FIFOQueue::FIFOQueue(const ::tensorflow::Scope& scope, const DataTypeSlice&
                     component_types)
  : FIFOQueue(scope, component_types, FIFOQueue::Attrs()) {}

GetSessionHandle::GetSessionHandle(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input value) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("GetSessionHandle");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "GetSessionHandle")
                     .Input(_value)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

GetSessionHandleV2::GetSessionHandleV2(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input value) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("GetSessionHandleV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "GetSessionHandleV2")
                     .Input(_value)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

GetSessionTensor::GetSessionTensor(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle, DataType dtype) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("GetSessionTensor");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "GetSessionTensor")
                     .Input(_handle)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->value = Output(ret, 0);
}

MapClear::MapClear(const ::tensorflow::Scope& scope, const DataTypeSlice&
                   dtypes, const MapClear::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MapClear");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MapClear")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

MapClear::MapClear(const ::tensorflow::Scope& scope, const DataTypeSlice&
                   dtypes)
  : MapClear(scope, dtypes, MapClear::Attrs()) {}

MapIncompleteSize::MapIncompleteSize(const ::tensorflow::Scope& scope, const
                                     DataTypeSlice& dtypes, const
                                     MapIncompleteSize::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MapIncompleteSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MapIncompleteSize")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

MapIncompleteSize::MapIncompleteSize(const ::tensorflow::Scope& scope, const
                                     DataTypeSlice& dtypes)
  : MapIncompleteSize(scope, dtypes, MapIncompleteSize::Attrs()) {}

MapPeek::MapPeek(const ::tensorflow::Scope& scope, ::tensorflow::Input key,
                 ::tensorflow::Input indices, const DataTypeSlice& dtypes,
                 const MapPeek::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _key = ::tensorflow::ops::AsNodeOut(scope, key);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MapPeek");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MapPeek")
                     .Input(_key)
                     .Input(_indices)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->values.push_back(Output(ret, i));
}

MapPeek::MapPeek(const ::tensorflow::Scope& scope, ::tensorflow::Input key,
                 ::tensorflow::Input indices, const DataTypeSlice& dtypes)
  : MapPeek(scope, key, indices, dtypes, MapPeek::Attrs()) {}

MapSize::MapSize(const ::tensorflow::Scope& scope, const DataTypeSlice& dtypes,
                 const MapSize::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MapSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MapSize")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

MapSize::MapSize(const ::tensorflow::Scope& scope, const DataTypeSlice& dtypes)
  : MapSize(scope, dtypes, MapSize::Attrs()) {}

MapStage::MapStage(const ::tensorflow::Scope& scope, ::tensorflow::Input key,
                   ::tensorflow::Input indices, ::tensorflow::InputList values,
                   const DataTypeSlice& dtypes, const MapStage::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _key = ::tensorflow::ops::AsNodeOut(scope, key);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MapStage");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MapStage")
                     .Input(_key)
                     .Input(_indices)
                     .Input(_values)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

MapStage::MapStage(const ::tensorflow::Scope& scope, ::tensorflow::Input key,
                   ::tensorflow::Input indices, ::tensorflow::InputList values,
                   const DataTypeSlice& dtypes)
  : MapStage(scope, key, indices, values, dtypes, MapStage::Attrs()) {}

MapUnstage::MapUnstage(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       key, ::tensorflow::Input indices, const DataTypeSlice&
                       dtypes, const MapUnstage::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _key = ::tensorflow::ops::AsNodeOut(scope, key);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MapUnstage");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MapUnstage")
                     .Input(_key)
                     .Input(_indices)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->values.push_back(Output(ret, i));
}

MapUnstage::MapUnstage(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       key, ::tensorflow::Input indices, const DataTypeSlice&
                       dtypes)
  : MapUnstage(scope, key, indices, dtypes, MapUnstage::Attrs()) {}

MapUnstageNoKey::MapUnstageNoKey(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input indices, const
                                 DataTypeSlice& dtypes, const
                                 MapUnstageNoKey::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MapUnstageNoKey");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MapUnstageNoKey")
                     .Input(_indices)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
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

  this->key = Output(ret, _outputs_range["key"].first);
  for (int32 i = _outputs_range["values"].first; i < _outputs_range["values"].second; ++i)
    this->values.push_back(Output(ret, i));
}

MapUnstageNoKey::MapUnstageNoKey(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input indices, const
                                 DataTypeSlice& dtypes)
  : MapUnstageNoKey(scope, indices, dtypes, MapUnstageNoKey::Attrs()) {}

OrderedMapClear::OrderedMapClear(const ::tensorflow::Scope& scope, const
                                 DataTypeSlice& dtypes, const
                                 OrderedMapClear::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OrderedMapClear");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OrderedMapClear")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

OrderedMapClear::OrderedMapClear(const ::tensorflow::Scope& scope, const
                                 DataTypeSlice& dtypes)
  : OrderedMapClear(scope, dtypes, OrderedMapClear::Attrs()) {}

OrderedMapIncompleteSize::OrderedMapIncompleteSize(const ::tensorflow::Scope&
                                                   scope, const DataTypeSlice&
                                                   dtypes, const
                                                   OrderedMapIncompleteSize::Attrs&
                                                   attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OrderedMapIncompleteSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OrderedMapIncompleteSize")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

OrderedMapIncompleteSize::OrderedMapIncompleteSize(const ::tensorflow::Scope&
                                                   scope, const DataTypeSlice&
                                                   dtypes)
  : OrderedMapIncompleteSize(scope, dtypes, OrderedMapIncompleteSize::Attrs()) {}

OrderedMapPeek::OrderedMapPeek(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input key, ::tensorflow::Input
                               indices, const DataTypeSlice& dtypes, const
                               OrderedMapPeek::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _key = ::tensorflow::ops::AsNodeOut(scope, key);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OrderedMapPeek");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OrderedMapPeek")
                     .Input(_key)
                     .Input(_indices)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->values.push_back(Output(ret, i));
}

OrderedMapPeek::OrderedMapPeek(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input key, ::tensorflow::Input
                               indices, const DataTypeSlice& dtypes)
  : OrderedMapPeek(scope, key, indices, dtypes, OrderedMapPeek::Attrs()) {}

OrderedMapSize::OrderedMapSize(const ::tensorflow::Scope& scope, const
                               DataTypeSlice& dtypes, const
                               OrderedMapSize::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OrderedMapSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OrderedMapSize")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

OrderedMapSize::OrderedMapSize(const ::tensorflow::Scope& scope, const
                               DataTypeSlice& dtypes)
  : OrderedMapSize(scope, dtypes, OrderedMapSize::Attrs()) {}

OrderedMapStage::OrderedMapStage(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input key, ::tensorflow::Input
                                 indices, ::tensorflow::InputList values, const
                                 DataTypeSlice& dtypes, const
                                 OrderedMapStage::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _key = ::tensorflow::ops::AsNodeOut(scope, key);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OrderedMapStage");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OrderedMapStage")
                     .Input(_key)
                     .Input(_indices)
                     .Input(_values)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

OrderedMapStage::OrderedMapStage(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input key, ::tensorflow::Input
                                 indices, ::tensorflow::InputList values, const
                                 DataTypeSlice& dtypes)
  : OrderedMapStage(scope, key, indices, values, dtypes, OrderedMapStage::Attrs()) {}

OrderedMapUnstage::OrderedMapUnstage(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input key,
                                     ::tensorflow::Input indices, const
                                     DataTypeSlice& dtypes, const
                                     OrderedMapUnstage::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _key = ::tensorflow::ops::AsNodeOut(scope, key);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OrderedMapUnstage");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OrderedMapUnstage")
                     .Input(_key)
                     .Input(_indices)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->values.push_back(Output(ret, i));
}

OrderedMapUnstage::OrderedMapUnstage(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input key,
                                     ::tensorflow::Input indices, const
                                     DataTypeSlice& dtypes)
  : OrderedMapUnstage(scope, key, indices, dtypes, OrderedMapUnstage::Attrs()) {}

OrderedMapUnstageNoKey::OrderedMapUnstageNoKey(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input
                                               indices, const DataTypeSlice&
                                               dtypes, const
                                               OrderedMapUnstageNoKey::Attrs&
                                               attrs) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OrderedMapUnstageNoKey");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OrderedMapUnstageNoKey")
                     .Input(_indices)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
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

  this->key = Output(ret, _outputs_range["key"].first);
  for (int32 i = _outputs_range["values"].first; i < _outputs_range["values"].second; ++i)
    this->values.push_back(Output(ret, i));
}

OrderedMapUnstageNoKey::OrderedMapUnstageNoKey(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input
                                               indices, const DataTypeSlice&
                                               dtypes)
  : OrderedMapUnstageNoKey(scope, indices, dtypes, OrderedMapUnstageNoKey::Attrs()) {}

PaddingFIFOQueue::PaddingFIFOQueue(const ::tensorflow::Scope& scope, const
                                   DataTypeSlice& component_types, const
                                   PaddingFIFOQueue::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("PaddingFIFOQueue");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "PaddingFIFOQueueV2")
                     .Attr("component_types", component_types)
                     .Attr("shapes", attrs.shapes_)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

PaddingFIFOQueue::PaddingFIFOQueue(const ::tensorflow::Scope& scope, const
                                   DataTypeSlice& component_types)
  : PaddingFIFOQueue(scope, component_types, PaddingFIFOQueue::Attrs()) {}

ParallelDynamicStitch::ParallelDynamicStitch(const ::tensorflow::Scope& scope,
                                             ::tensorflow::InputList indices,
                                             ::tensorflow::InputList data) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOutList(scope, indices);
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOutList(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ParallelDynamicStitch");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ParallelDynamicStitch")
                     .Input(_indices)
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->merged = Output(ret, 0);
}

PriorityQueue::PriorityQueue(const ::tensorflow::Scope& scope, const
                             gtl::ArraySlice<PartialTensorShape>& shapes, const
                             PriorityQueue::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("PriorityQueue");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "PriorityQueueV2")
                     .Attr("component_types", attrs.component_types_)
                     .Attr("shapes", shapes)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

PriorityQueue::PriorityQueue(const ::tensorflow::Scope& scope, const
                             gtl::ArraySlice<PartialTensorShape>& shapes)
  : PriorityQueue(scope, shapes, PriorityQueue::Attrs()) {}

QueueClose::QueueClose(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       handle, const QueueClose::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueClose");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueCloseV2")
                     .Input(_handle)
                     .Attr("cancel_pending_enqueues", attrs.cancel_pending_enqueues_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

QueueClose::QueueClose(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       handle)
  : QueueClose(scope, handle, QueueClose::Attrs()) {}

QueueDequeueMany::QueueDequeueMany(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::Input n, const DataTypeSlice&
                                   component_types, const
                                   QueueDequeueMany::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _n = ::tensorflow::ops::AsNodeOut(scope, n);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueDequeueMany");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueDequeueManyV2")
                     .Input(_handle)
                     .Input(_n)
                     .Attr("component_types", component_types)
                     .Attr("timeout_ms", attrs.timeout_ms_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->components.push_back(Output(ret, i));
}

QueueDequeueMany::QueueDequeueMany(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::Input n, const DataTypeSlice&
                                   component_types)
  : QueueDequeueMany(scope, handle, n, component_types, QueueDequeueMany::Attrs()) {}

QueueDequeueUpTo::QueueDequeueUpTo(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::Input n, const DataTypeSlice&
                                   component_types, const
                                   QueueDequeueUpTo::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _n = ::tensorflow::ops::AsNodeOut(scope, n);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueDequeueUpTo");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueDequeueUpToV2")
                     .Input(_handle)
                     .Input(_n)
                     .Attr("component_types", component_types)
                     .Attr("timeout_ms", attrs.timeout_ms_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->components.push_back(Output(ret, i));
}

QueueDequeueUpTo::QueueDequeueUpTo(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::Input n, const DataTypeSlice&
                                   component_types)
  : QueueDequeueUpTo(scope, handle, n, component_types, QueueDequeueUpTo::Attrs()) {}

QueueDequeue::QueueDequeue(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input handle, const DataTypeSlice&
                           component_types, const QueueDequeue::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueDequeue");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueDequeueV2")
                     .Input(_handle)
                     .Attr("component_types", component_types)
                     .Attr("timeout_ms", attrs.timeout_ms_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->components.push_back(Output(ret, i));
}

QueueDequeue::QueueDequeue(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input handle, const DataTypeSlice&
                           component_types)
  : QueueDequeue(scope, handle, component_types, QueueDequeue::Attrs()) {}

QueueEnqueueMany::QueueEnqueueMany(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::InputList components, const
                                   QueueEnqueueMany::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _components = ::tensorflow::ops::AsNodeOutList(scope, components);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueEnqueueMany");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueEnqueueManyV2")
                     .Input(_handle)
                     .Input(_components)
                     .Attr("timeout_ms", attrs.timeout_ms_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

QueueEnqueueMany::QueueEnqueueMany(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::InputList components)
  : QueueEnqueueMany(scope, handle, components, QueueEnqueueMany::Attrs()) {}

QueueEnqueue::QueueEnqueue(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input handle, ::tensorflow::InputList
                           components, const QueueEnqueue::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _components = ::tensorflow::ops::AsNodeOutList(scope, components);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueEnqueue");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueEnqueueV2")
                     .Input(_handle)
                     .Input(_components)
                     .Attr("timeout_ms", attrs.timeout_ms_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

QueueEnqueue::QueueEnqueue(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input handle, ::tensorflow::InputList
                           components)
  : QueueEnqueue(scope, handle, components, QueueEnqueue::Attrs()) {}

QueueIsClosed::QueueIsClosed(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueIsClosed");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueIsClosed")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->is_closed = Output(ret, 0);
}

QueueIsClosedV2::QueueIsClosedV2(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueIsClosedV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueIsClosedV2")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->is_closed = Output(ret, 0);
}

QueueSize::QueueSize(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QueueSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QueueSizeV2")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

RandomShuffleQueue::RandomShuffleQueue(const ::tensorflow::Scope& scope, const
                                       DataTypeSlice& component_types, const
                                       RandomShuffleQueue::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomShuffleQueue");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomShuffleQueueV2")
                     .Attr("component_types", component_types)
                     .Attr("shapes", attrs.shapes_)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("min_after_dequeue", attrs.min_after_dequeue_)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

RandomShuffleQueue::RandomShuffleQueue(const ::tensorflow::Scope& scope, const
                                       DataTypeSlice& component_types)
  : RandomShuffleQueue(scope, component_types, RandomShuffleQueue::Attrs()) {}

RecordInput::RecordInput(const ::tensorflow::Scope& scope, StringPiece
                         file_pattern, const RecordInput::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RecordInput");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RecordInput")
                     .Attr("file_pattern", file_pattern)
                     .Attr("file_random_seed", attrs.file_random_seed_)
                     .Attr("file_shuffle_shift_ratio", attrs.file_shuffle_shift_ratio_)
                     .Attr("file_buffer_size", attrs.file_buffer_size_)
                     .Attr("file_parallelism", attrs.file_parallelism_)
                     .Attr("batch_size", attrs.batch_size_)
                     .Attr("compression_type", attrs.compression_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->records = Output(ret, 0);
}

RecordInput::RecordInput(const ::tensorflow::Scope& scope, StringPiece
                         file_pattern)
  : RecordInput(scope, file_pattern, RecordInput::Attrs()) {}

SparseAccumulatorApplyGradient::SparseAccumulatorApplyGradient(const
                                                               ::tensorflow::Scope&
                                                               scope,
                                                               ::tensorflow::Input
                                                               handle,
                                                               ::tensorflow::Input
                                                               local_step,
                                                               ::tensorflow::Input
                                                               gradient_indices,
                                                               ::tensorflow::Input
                                                               gradient_values,
                                                               ::tensorflow::Input
                                                               gradient_shape,
                                                               bool
                                                               has_known_shape) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _local_step = ::tensorflow::ops::AsNodeOut(scope, local_step);
  if (!scope.ok()) return;
  auto _gradient_indices = ::tensorflow::ops::AsNodeOut(scope, gradient_indices);
  if (!scope.ok()) return;
  auto _gradient_values = ::tensorflow::ops::AsNodeOut(scope, gradient_values);
  if (!scope.ok()) return;
  auto _gradient_shape = ::tensorflow::ops::AsNodeOut(scope, gradient_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseAccumulatorApplyGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseAccumulatorApplyGradient")
                     .Input(_handle)
                     .Input(_local_step)
                     .Input(_gradient_indices)
                     .Input(_gradient_values)
                     .Input(_gradient_shape)
                     .Attr("has_known_shape", has_known_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

SparseAccumulatorTakeGradient::SparseAccumulatorTakeGradient(const
                                                             ::tensorflow::Scope&
                                                             scope,
                                                             ::tensorflow::Input
                                                             handle,
                                                             ::tensorflow::Input
                                                             num_required,
                                                             DataType dtype) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _num_required = ::tensorflow::ops::AsNodeOut(scope, num_required);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseAccumulatorTakeGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseAccumulatorTakeGradient")
                     .Input(_handle)
                     .Input(_num_required)
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

  this->indices = Output(ret, _outputs_range["indices"].first);
  this->values = Output(ret, _outputs_range["values"].first);
  this->shape = Output(ret, _outputs_range["shape"].first);
}

SparseConditionalAccumulator::SparseConditionalAccumulator(const
                                                           ::tensorflow::Scope&
                                                           scope, DataType
                                                           dtype,
                                                           PartialTensorShape
                                                           shape, const
                                                           SparseConditionalAccumulator::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseConditionalAccumulator");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseConditionalAccumulator")
                     .Attr("dtype", dtype)
                     .Attr("shape", shape)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->handle = Output(ret, 0);
}

SparseConditionalAccumulator::SparseConditionalAccumulator(const
                                                           ::tensorflow::Scope&
                                                           scope, DataType
                                                           dtype,
                                                           PartialTensorShape
                                                           shape)
  : SparseConditionalAccumulator(scope, dtype, shape, SparseConditionalAccumulator::Attrs()) {}

Stage::Stage(const ::tensorflow::Scope& scope, ::tensorflow::InputList values,
             const Stage::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Stage");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Stage")
                     .Input(_values)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

Stage::Stage(const ::tensorflow::Scope& scope, ::tensorflow::InputList values)
  : Stage(scope, values, Stage::Attrs()) {}

StageClear::StageClear(const ::tensorflow::Scope& scope, const DataTypeSlice&
                       dtypes, const StageClear::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StageClear");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StageClear")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

StageClear::StageClear(const ::tensorflow::Scope& scope, const DataTypeSlice&
                       dtypes)
  : StageClear(scope, dtypes, StageClear::Attrs()) {}

StagePeek::StagePeek(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     index, const DataTypeSlice& dtypes, const
                     StagePeek::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _index = ::tensorflow::ops::AsNodeOut(scope, index);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StagePeek");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StagePeek")
                     .Input(_index)
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->values.push_back(Output(ret, i));
}

StagePeek::StagePeek(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     index, const DataTypeSlice& dtypes)
  : StagePeek(scope, index, dtypes, StagePeek::Attrs()) {}

StageSize::StageSize(const ::tensorflow::Scope& scope, const DataTypeSlice&
                     dtypes, const StageSize::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StageSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StageSize")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

StageSize::StageSize(const ::tensorflow::Scope& scope, const DataTypeSlice&
                     dtypes)
  : StageSize(scope, dtypes, StageSize::Attrs()) {}

TensorArrayClose::TensorArrayClose(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayClose");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayCloseV3")
                     .Input(_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

TensorArrayConcat::TensorArrayConcat(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input handle,
                                     ::tensorflow::Input flow_in, DataType
                                     dtype, const TensorArrayConcat::Attrs&
                                     attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayConcat");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayConcatV3")
                     .Input(_handle)
                     .Input(_flow_in)
                     .Attr("dtype", dtype)
                     .Attr("element_shape_except0", attrs.element_shape_except0_)
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

  this->value = Output(ret, _outputs_range["value"].first);
  this->lengths = Output(ret, _outputs_range["lengths"].first);
}

TensorArrayConcat::TensorArrayConcat(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input handle,
                                     ::tensorflow::Input flow_in, DataType
                                     dtype)
  : TensorArrayConcat(scope, handle, flow_in, dtype, TensorArrayConcat::Attrs()) {}

TensorArrayGather::TensorArrayGather(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input handle,
                                     ::tensorflow::Input indices,
                                     ::tensorflow::Input flow_in, DataType
                                     dtype, const TensorArrayGather::Attrs&
                                     attrs) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayGather");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayGatherV3")
                     .Input(_handle)
                     .Input(_indices)
                     .Input(_flow_in)
                     .Attr("dtype", dtype)
                     .Attr("element_shape", attrs.element_shape_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->value = Output(ret, 0);
}

TensorArrayGather::TensorArrayGather(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input handle,
                                     ::tensorflow::Input indices,
                                     ::tensorflow::Input flow_in, DataType
                                     dtype)
  : TensorArrayGather(scope, handle, indices, flow_in, dtype, TensorArrayGather::Attrs()) {}

TensorArrayGrad::TensorArrayGrad(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input handle,
                                 ::tensorflow::Input flow_in, StringPiece
                                 source) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayGradV3")
                     .Input(_handle)
                     .Input(_flow_in)
                     .Attr("source", source)
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

  this->grad_handle = Output(ret, _outputs_range["grad_handle"].first);
  this->flow_out = Output(ret, _outputs_range["flow_out"].first);
}

TensorArrayGradWithShape::TensorArrayGradWithShape(const ::tensorflow::Scope&
                                                   scope, ::tensorflow::Input
                                                   handle, ::tensorflow::Input
                                                   flow_in, ::tensorflow::Input
                                                   shape_to_prepend,
                                                   StringPiece source) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  auto _shape_to_prepend = ::tensorflow::ops::AsNodeOut(scope, shape_to_prepend);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayGradWithShape");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayGradWithShape")
                     .Input(_handle)
                     .Input(_flow_in)
                     .Input(_shape_to_prepend)
                     .Attr("source", source)
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

  this->grad_handle = Output(ret, _outputs_range["grad_handle"].first);
  this->flow_out = Output(ret, _outputs_range["flow_out"].first);
}

TensorArrayRead::TensorArrayRead(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input handle,
                                 ::tensorflow::Input index, ::tensorflow::Input
                                 flow_in, DataType dtype) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _index = ::tensorflow::ops::AsNodeOut(scope, index);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayRead");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayReadV3")
                     .Input(_handle)
                     .Input(_index)
                     .Input(_flow_in)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->value = Output(ret, 0);
}

TensorArrayScatter::TensorArrayScatter(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input handle,
                                       ::tensorflow::Input indices,
                                       ::tensorflow::Input value,
                                       ::tensorflow::Input flow_in) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayScatter");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayScatterV3")
                     .Input(_handle)
                     .Input(_indices)
                     .Input(_value)
                     .Input(_flow_in)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->flow_out = Output(ret, 0);
}

TensorArraySize::TensorArraySize(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input handle,
                                 ::tensorflow::Input flow_in) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArraySize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArraySizeV3")
                     .Input(_handle)
                     .Input(_flow_in)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

TensorArraySplit::TensorArraySplit(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::Input value,
                                   ::tensorflow::Input lengths,
                                   ::tensorflow::Input flow_in) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  auto _lengths = ::tensorflow::ops::AsNodeOut(scope, lengths);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArraySplit");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArraySplitV3")
                     .Input(_handle)
                     .Input(_value)
                     .Input(_lengths)
                     .Input(_flow_in)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->flow_out = Output(ret, 0);
}

TensorArray::TensorArray(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         size, DataType dtype, const TensorArray::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _size = ::tensorflow::ops::AsNodeOut(scope, size);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArray");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayV3")
                     .Input(_size)
                     .Attr("dtype", dtype)
                     .Attr("element_shape", attrs.element_shape_)
                     .Attr("dynamic_size", attrs.dynamic_size_)
                     .Attr("clear_after_read", attrs.clear_after_read_)
                     .Attr("identical_element_shapes", attrs.identical_element_shapes_)
                     .Attr("tensor_array_name", attrs.tensor_array_name_)
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

  this->handle = Output(ret, _outputs_range["handle"].first);
  this->flow = Output(ret, _outputs_range["flow"].first);
}

TensorArray::TensorArray(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         size, DataType dtype)
  : TensorArray(scope, size, dtype, TensorArray::Attrs()) {}

TensorArrayWrite::TensorArrayWrite(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input handle,
                                   ::tensorflow::Input index,
                                   ::tensorflow::Input value,
                                   ::tensorflow::Input flow_in) {
  if (!scope.ok()) return;
  auto _handle = ::tensorflow::ops::AsNodeOut(scope, handle);
  if (!scope.ok()) return;
  auto _index = ::tensorflow::ops::AsNodeOut(scope, index);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  auto _flow_in = ::tensorflow::ops::AsNodeOut(scope, flow_in);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TensorArrayWrite");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TensorArrayWriteV3")
                     .Input(_handle)
                     .Input(_index)
                     .Input(_value)
                     .Input(_flow_in)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->flow_out = Output(ret, 0);
}

Unstage::Unstage(const ::tensorflow::Scope& scope, const DataTypeSlice& dtypes,
                 const Unstage::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Unstage");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Unstage")
                     .Attr("capacity", attrs.capacity_)
                     .Attr("memory_limit", attrs.memory_limit_)
                     .Attr("dtypes", dtypes)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->values.push_back(Output(ret, i));
}

Unstage::Unstage(const ::tensorflow::Scope& scope, const DataTypeSlice& dtypes)
  : Unstage(scope, dtypes, Unstage::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
