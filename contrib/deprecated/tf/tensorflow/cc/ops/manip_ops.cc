// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/manip_ops.h"

namespace tensorflow {
namespace ops {

Roll::Roll(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input shift, ::tensorflow::Input axis) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _shift = ::tensorflow::ops::AsNodeOut(scope, shift);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Roll");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Roll")
                     .Input(_input)
                     .Input(_shift)
                     .Input(_axis)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

/// @}

}  // namespace ops
}  // namespace tensorflow
