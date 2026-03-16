// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/user_ops.h"

namespace tensorflow {
namespace ops {

Fact::Fact(const ::tensorflow::Scope& scope) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Fact");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Fact")
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->fact = Output(ret, 0);
}

/// @}

}  // namespace ops
}  // namespace tensorflow
