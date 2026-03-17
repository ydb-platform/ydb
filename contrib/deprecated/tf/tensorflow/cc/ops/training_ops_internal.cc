// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/training_ops_internal.h"

namespace tensorflow {
namespace ops {
namespace internal {
// NOTE: This namespace has internal TensorFlow details that
// are not part of TensorFlow's public API.

ApplyAdaMax::ApplyAdaMax(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         var, ::tensorflow::Input m, ::tensorflow::Input v,
                         ::tensorflow::Input beta1_power, ::tensorflow::Input
                         lr, ::tensorflow::Input beta1, ::tensorflow::Input
                         beta2, ::tensorflow::Input epsilon,
                         ::tensorflow::Input grad, const ApplyAdaMax::Attrs&
                         attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _v = ::tensorflow::ops::AsNodeOut(scope, v);
  if (!scope.ok()) return;
  auto _beta1_power = ::tensorflow::ops::AsNodeOut(scope, beta1_power);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _beta1 = ::tensorflow::ops::AsNodeOut(scope, beta1);
  if (!scope.ok()) return;
  auto _beta2 = ::tensorflow::ops::AsNodeOut(scope, beta2);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyAdaMax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyAdaMax")
                     .Input(_var)
                     .Input(_m)
                     .Input(_v)
                     .Input(_beta1_power)
                     .Input(_lr)
                     .Input(_beta1)
                     .Input(_beta2)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyAdaMax::ApplyAdaMax(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         var, ::tensorflow::Input m, ::tensorflow::Input v,
                         ::tensorflow::Input beta1_power, ::tensorflow::Input
                         lr, ::tensorflow::Input beta1, ::tensorflow::Input
                         beta2, ::tensorflow::Input epsilon,
                         ::tensorflow::Input grad)
  : ApplyAdaMax(scope, var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad, ApplyAdaMax::Attrs()) {}

ResourceApplyAdaMax::ResourceApplyAdaMax(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input m,
                                         ::tensorflow::Input v,
                                         ::tensorflow::Input beta1_power,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input beta1,
                                         ::tensorflow::Input beta2,
                                         ::tensorflow::Input epsilon,
                                         ::tensorflow::Input grad, const
                                         ResourceApplyAdaMax::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _v = ::tensorflow::ops::AsNodeOut(scope, v);
  if (!scope.ok()) return;
  auto _beta1_power = ::tensorflow::ops::AsNodeOut(scope, beta1_power);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _beta1 = ::tensorflow::ops::AsNodeOut(scope, beta1);
  if (!scope.ok()) return;
  auto _beta2 = ::tensorflow::ops::AsNodeOut(scope, beta2);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyAdaMax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyAdaMax")
                     .Input(_var)
                     .Input(_m)
                     .Input(_v)
                     .Input(_beta1_power)
                     .Input(_lr)
                     .Input(_beta1)
                     .Input(_beta2)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyAdaMax::ResourceApplyAdaMax(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input m,
                                         ::tensorflow::Input v,
                                         ::tensorflow::Input beta1_power,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input beta1,
                                         ::tensorflow::Input beta2,
                                         ::tensorflow::Input epsilon,
                                         ::tensorflow::Input grad)
  : ResourceApplyAdaMax(scope, var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad, ResourceApplyAdaMax::Attrs()) {}

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow
