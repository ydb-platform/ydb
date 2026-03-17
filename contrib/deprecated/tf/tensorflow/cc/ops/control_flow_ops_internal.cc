// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/control_flow_ops_internal.h"

namespace tensorflow {
namespace ops {
namespace internal {
// NOTE: This namespace has internal TensorFlow details that
// are not part of TensorFlow's public API.

Enter::Enter(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
             StringPiece frame_name, const Enter::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Enter");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Enter")
                     .Input(_data)
                     .Attr("frame_name", frame_name)
                     .Attr("is_constant", attrs.is_constant_)
                     .Attr("parallel_iterations", attrs.parallel_iterations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Enter::Enter(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
             StringPiece frame_name)
  : Enter(scope, data, frame_name, Enter::Attrs()) {}

Exit::Exit(const ::tensorflow::Scope& scope, ::tensorflow::Input data) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Exit");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Exit")
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RefEnter::RefEnter(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                   StringPiece frame_name, const RefEnter::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RefEnter");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RefEnter")
                     .Input(_data)
                     .Attr("frame_name", frame_name)
                     .Attr("is_constant", attrs.is_constant_)
                     .Attr("parallel_iterations", attrs.parallel_iterations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RefEnter::RefEnter(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                   StringPiece frame_name)
  : RefEnter(scope, data, frame_name, RefEnter::Attrs()) {}

RefExit::RefExit(const ::tensorflow::Scope& scope, ::tensorflow::Input data) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RefExit");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RefExit")
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RefMerge::RefMerge(const ::tensorflow::Scope& scope, ::tensorflow::InputList
                   inputs) {
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOutList(scope, inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RefMerge");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RefMerge")
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

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow
