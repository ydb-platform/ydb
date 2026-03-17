// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/control_flow_ops.h"

namespace tensorflow {
namespace ops {

Abort::Abort(const ::tensorflow::Scope& scope, const Abort::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Abort");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Abort")
                     .Attr("error_msg", attrs.error_msg_)
                     .Attr("exit_without_error", attrs.exit_without_error_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

Abort::Abort(const ::tensorflow::Scope& scope)
  : Abort(scope, Abort::Attrs()) {}

ControlTrigger::ControlTrigger(const ::tensorflow::Scope& scope) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ControlTrigger");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ControlTrigger")
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

LoopCond::LoopCond(const ::tensorflow::Scope& scope, ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LoopCond");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LoopCond")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Merge::Merge(const ::tensorflow::Scope& scope, ::tensorflow::InputList inputs) {
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOutList(scope, inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Merge");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Merge")
                     .Input(_inputs)
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

  this->output = Output(ret, _outputs_range["output"].first);
  this->value_index = Output(ret, _outputs_range["value_index"].first);
}

NextIteration::NextIteration(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input data) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("NextIteration");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "NextIteration")
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RefNextIteration::RefNextIteration(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input data) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RefNextIteration");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RefNextIteration")
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RefSelect::RefSelect(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     index, ::tensorflow::InputList inputs) {
  if (!scope.ok()) return;
  auto _index = ::tensorflow::ops::AsNodeOut(scope, index);
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOutList(scope, inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RefSelect");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RefSelect")
                     .Input(_index)
                     .Input(_inputs)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RefSwitch::RefSwitch(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     data, ::tensorflow::Input pred) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _pred = ::tensorflow::ops::AsNodeOut(scope, pred);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RefSwitch");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RefSwitch")
                     .Input(_data)
                     .Input(_pred)
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

  this->output_false = Output(ret, _outputs_range["output_false"].first);
  this->output_true = Output(ret, _outputs_range["output_true"].first);
}

Switch::Switch(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
               ::tensorflow::Input pred) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _pred = ::tensorflow::ops::AsNodeOut(scope, pred);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Switch");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Switch")
                     .Input(_data)
                     .Input(_pred)
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

  this->output_false = Output(ret, _outputs_range["output_false"].first);
  this->output_true = Output(ret, _outputs_range["output_true"].first);
}

/// @}

}  // namespace ops
}  // namespace tensorflow
