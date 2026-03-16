// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/no_op.h"

namespace tensorflow {
namespace ops {

NoOp::NoOp(const ::tensorflow::Scope& scope) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("NoOp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "NoOp")
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

/// @}

}  // namespace ops
}  // namespace tensorflow
