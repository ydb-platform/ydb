// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/random_ops_internal.h"

namespace tensorflow {
namespace ops {
namespace internal {
// NOTE: This namespace has internal TensorFlow details that
// are not part of TensorFlow's public API.

RandomGammaGrad::RandomGammaGrad(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input alpha, ::tensorflow::Input
                                 sample) {
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _sample = ::tensorflow::ops::AsNodeOut(scope, sample);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomGammaGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomGammaGrad")
                     .Input(_alpha)
                     .Input(_sample)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow
