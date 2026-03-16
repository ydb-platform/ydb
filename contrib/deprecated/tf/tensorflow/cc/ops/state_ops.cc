// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/state_ops.h"

namespace tensorflow {
namespace ops {

Assign::Assign(const ::tensorflow::Scope& scope, ::tensorflow::Input ref,
               ::tensorflow::Input value, const Assign::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Assign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Assign")
                     .Input(_ref)
                     .Input(_value)
                     .Attr("validate_shape", attrs.validate_shape_)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

Assign::Assign(const ::tensorflow::Scope& scope, ::tensorflow::Input ref,
               ::tensorflow::Input value)
  : Assign(scope, ref, value, Assign::Attrs()) {}

AssignAdd::AssignAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input ref,
                     ::tensorflow::Input value, const AssignAdd::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AssignAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AssignAdd")
                     .Input(_ref)
                     .Input(_value)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

AssignAdd::AssignAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input ref,
                     ::tensorflow::Input value)
  : AssignAdd(scope, ref, value, AssignAdd::Attrs()) {}

AssignSub::AssignSub(const ::tensorflow::Scope& scope, ::tensorflow::Input ref,
                     ::tensorflow::Input value, const AssignSub::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AssignSub");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AssignSub")
                     .Input(_ref)
                     .Input(_value)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

AssignSub::AssignSub(const ::tensorflow::Scope& scope, ::tensorflow::Input ref,
                     ::tensorflow::Input value)
  : AssignSub(scope, ref, value, AssignSub::Attrs()) {}

CountUpTo::CountUpTo(const ::tensorflow::Scope& scope, ::tensorflow::Input ref,
                     int64 limit) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("CountUpTo");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "CountUpTo")
                     .Input(_ref)
                     .Attr("limit", limit)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DestroyTemporaryVariable::DestroyTemporaryVariable(const ::tensorflow::Scope&
                                                   scope, ::tensorflow::Input
                                                   ref, StringPiece var_name) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DestroyTemporaryVariable");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DestroyTemporaryVariable")
                     .Input(_ref)
                     .Attr("var_name", var_name)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->value = Output(ret, 0);
}

IsVariableInitialized::IsVariableInitialized(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input ref) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("IsVariableInitialized");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "IsVariableInitialized")
                     .Input(_ref)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->is_initialized = Output(ret, 0);
}

ResourceCountUpTo::ResourceCountUpTo(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input resource, int64 limit,
                                     DataType T) {
  if (!scope.ok()) return;
  auto _resource = ::tensorflow::ops::AsNodeOut(scope, resource);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceCountUpTo");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceCountUpTo")
                     .Input(_resource)
                     .Attr("limit", limit)
                     .Attr("T", T)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ResourceScatterNdAdd::ResourceScatterNdAdd(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input ref,
                                           ::tensorflow::Input indices,
                                           ::tensorflow::Input updates, const
                                           ResourceScatterNdAdd::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceScatterNdAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceScatterNdAdd")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceScatterNdAdd::ResourceScatterNdAdd(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input ref,
                                           ::tensorflow::Input indices,
                                           ::tensorflow::Input updates)
  : ResourceScatterNdAdd(scope, ref, indices, updates, ResourceScatterNdAdd::Attrs()) {}

ResourceScatterNdUpdate::ResourceScatterNdUpdate(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 ref, ::tensorflow::Input
                                                 indices, ::tensorflow::Input
                                                 updates, const
                                                 ResourceScatterNdUpdate::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceScatterNdUpdate");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceScatterNdUpdate")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceScatterNdUpdate::ResourceScatterNdUpdate(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 ref, ::tensorflow::Input
                                                 indices, ::tensorflow::Input
                                                 updates)
  : ResourceScatterNdUpdate(scope, ref, indices, updates, ResourceScatterNdUpdate::Attrs()) {}

ScatterAdd::ScatterAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates, const ScatterAdd::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterAdd")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterAdd::ScatterAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates)
  : ScatterAdd(scope, ref, indices, updates, ScatterAdd::Attrs()) {}

ScatterDiv::ScatterDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates, const ScatterDiv::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterDiv");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterDiv")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterDiv::ScatterDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates)
  : ScatterDiv(scope, ref, indices, updates, ScatterDiv::Attrs()) {}

ScatterMax::ScatterMax(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates, const ScatterMax::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterMax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterMax")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterMax::ScatterMax(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates)
  : ScatterMax(scope, ref, indices, updates, ScatterMax::Attrs()) {}

ScatterMin::ScatterMin(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates, const ScatterMin::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterMin");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterMin")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterMin::ScatterMin(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates)
  : ScatterMin(scope, ref, indices, updates, ScatterMin::Attrs()) {}

ScatterMul::ScatterMul(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates, const ScatterMul::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterMul")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterMul::ScatterMul(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates)
  : ScatterMul(scope, ref, indices, updates, ScatterMul::Attrs()) {}

ScatterNdAdd::ScatterNdAdd(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input ref, ::tensorflow::Input
                           indices, ::tensorflow::Input updates, const
                           ScatterNdAdd::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterNdAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterNdAdd")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterNdAdd::ScatterNdAdd(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input ref, ::tensorflow::Input
                           indices, ::tensorflow::Input updates)
  : ScatterNdAdd(scope, ref, indices, updates, ScatterNdAdd::Attrs()) {}

ScatterNdSub::ScatterNdSub(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input ref, ::tensorflow::Input
                           indices, ::tensorflow::Input updates, const
                           ScatterNdSub::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterNdSub");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterNdSub")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterNdSub::ScatterNdSub(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input ref, ::tensorflow::Input
                           indices, ::tensorflow::Input updates)
  : ScatterNdSub(scope, ref, indices, updates, ScatterNdSub::Attrs()) {}

ScatterNdUpdate::ScatterNdUpdate(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input ref, ::tensorflow::Input
                                 indices, ::tensorflow::Input updates, const
                                 ScatterNdUpdate::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterNdUpdate");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterNdUpdate")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterNdUpdate::ScatterNdUpdate(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input ref, ::tensorflow::Input
                                 indices, ::tensorflow::Input updates)
  : ScatterNdUpdate(scope, ref, indices, updates, ScatterNdUpdate::Attrs()) {}

ScatterSub::ScatterSub(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates, const ScatterSub::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterSub");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterSub")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterSub::ScatterSub(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       ref, ::tensorflow::Input indices, ::tensorflow::Input
                       updates)
  : ScatterSub(scope, ref, indices, updates, ScatterSub::Attrs()) {}

ScatterUpdate::ScatterUpdate(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input ref, ::tensorflow::Input
                             indices, ::tensorflow::Input updates, const
                             ScatterUpdate::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterUpdate");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterUpdate")
                     .Input(_ref)
                     .Input(_indices)
                     .Input(_updates)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

ScatterUpdate::ScatterUpdate(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input ref, ::tensorflow::Input
                             indices, ::tensorflow::Input updates)
  : ScatterUpdate(scope, ref, indices, updates, ScatterUpdate::Attrs()) {}

TemporaryVariable::TemporaryVariable(const ::tensorflow::Scope& scope,
                                     PartialTensorShape shape, DataType dtype,
                                     const TemporaryVariable::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TemporaryVariable");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TemporaryVariable")
                     .Attr("shape", shape)
                     .Attr("dtype", dtype)
                     .Attr("var_name", attrs.var_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->ref = Output(ret, 0);
}

TemporaryVariable::TemporaryVariable(const ::tensorflow::Scope& scope,
                                     PartialTensorShape shape, DataType dtype)
  : TemporaryVariable(scope, shape, dtype, TemporaryVariable::Attrs()) {}

Variable::Variable(const ::tensorflow::Scope& scope, PartialTensorShape shape,
                   DataType dtype, const Variable::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Variable");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "VariableV2")
                     .Attr("shape", shape)
                     .Attr("dtype", dtype)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->ref = Output(ret, 0);
}

Variable::Variable(const ::tensorflow::Scope& scope, PartialTensorShape shape,
                   DataType dtype)
  : Variable(scope, shape, dtype, Variable::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
