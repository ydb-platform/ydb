// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/training_ops.h"

namespace tensorflow {
namespace ops {

ApplyAdadelta::ApplyAdadelta(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input var, ::tensorflow::Input
                             accum, ::tensorflow::Input accum_update,
                             ::tensorflow::Input lr, ::tensorflow::Input rho,
                             ::tensorflow::Input epsilon, ::tensorflow::Input
                             grad, const ApplyAdadelta::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _accum_update = ::tensorflow::ops::AsNodeOut(scope, accum_update);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyAdadelta");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyAdadelta")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_accum_update)
                     .Input(_lr)
                     .Input(_rho)
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

ApplyAdadelta::ApplyAdadelta(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input var, ::tensorflow::Input
                             accum, ::tensorflow::Input accum_update,
                             ::tensorflow::Input lr, ::tensorflow::Input rho,
                             ::tensorflow::Input epsilon, ::tensorflow::Input
                             grad)
  : ApplyAdadelta(scope, var, accum, accum_update, lr, rho, epsilon, grad, ApplyAdadelta::Attrs()) {}

ApplyAdagrad::ApplyAdagrad(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input var, ::tensorflow::Input accum,
                           ::tensorflow::Input lr, ::tensorflow::Input grad,
                           const ApplyAdagrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("update_slots", attrs.update_slots_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyAdagrad::ApplyAdagrad(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input var, ::tensorflow::Input accum,
                           ::tensorflow::Input lr, ::tensorflow::Input grad)
  : ApplyAdagrad(scope, var, accum, lr, grad, ApplyAdagrad::Attrs()) {}

ApplyAdagradDA::ApplyAdagradDA(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input var, ::tensorflow::Input
                               gradient_accumulator, ::tensorflow::Input
                               gradient_squared_accumulator,
                               ::tensorflow::Input grad, ::tensorflow::Input
                               lr, ::tensorflow::Input l1, ::tensorflow::Input
                               l2, ::tensorflow::Input global_step, const
                               ApplyAdagradDA::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _gradient_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_accumulator);
  if (!scope.ok()) return;
  auto _gradient_squared_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_squared_accumulator);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _global_step = ::tensorflow::ops::AsNodeOut(scope, global_step);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyAdagradDA");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyAdagradDA")
                     .Input(_var)
                     .Input(_gradient_accumulator)
                     .Input(_gradient_squared_accumulator)
                     .Input(_grad)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_global_step)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyAdagradDA::ApplyAdagradDA(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input var, ::tensorflow::Input
                               gradient_accumulator, ::tensorflow::Input
                               gradient_squared_accumulator,
                               ::tensorflow::Input grad, ::tensorflow::Input
                               lr, ::tensorflow::Input l1, ::tensorflow::Input
                               l2, ::tensorflow::Input global_step)
  : ApplyAdagradDA(scope, var, gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2, global_step, ApplyAdagradDA::Attrs()) {}

ApplyAdam::ApplyAdam(const ::tensorflow::Scope& scope, ::tensorflow::Input var,
                     ::tensorflow::Input m, ::tensorflow::Input v,
                     ::tensorflow::Input beta1_power, ::tensorflow::Input
                     beta2_power, ::tensorflow::Input lr, ::tensorflow::Input
                     beta1, ::tensorflow::Input beta2, ::tensorflow::Input
                     epsilon, ::tensorflow::Input grad, const ApplyAdam::Attrs&
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
  auto _beta2_power = ::tensorflow::ops::AsNodeOut(scope, beta2_power);
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
  const auto unique_name = scope.GetUniqueNameForOp("ApplyAdam");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyAdam")
                     .Input(_var)
                     .Input(_m)
                     .Input(_v)
                     .Input(_beta1_power)
                     .Input(_beta2_power)
                     .Input(_lr)
                     .Input(_beta1)
                     .Input(_beta2)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("use_nesterov", attrs.use_nesterov_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyAdam::ApplyAdam(const ::tensorflow::Scope& scope, ::tensorflow::Input var,
                     ::tensorflow::Input m, ::tensorflow::Input v,
                     ::tensorflow::Input beta1_power, ::tensorflow::Input
                     beta2_power, ::tensorflow::Input lr, ::tensorflow::Input
                     beta1, ::tensorflow::Input beta2, ::tensorflow::Input
                     epsilon, ::tensorflow::Input grad)
  : ApplyAdam(scope, var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, ApplyAdam::Attrs()) {}

ApplyAddSign::ApplyAddSign(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input var, ::tensorflow::Input m,
                           ::tensorflow::Input lr, ::tensorflow::Input alpha,
                           ::tensorflow::Input sign_decay, ::tensorflow::Input
                           beta, ::tensorflow::Input grad, const
                           ApplyAddSign::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _sign_decay = ::tensorflow::ops::AsNodeOut(scope, sign_decay);
  if (!scope.ok()) return;
  auto _beta = ::tensorflow::ops::AsNodeOut(scope, beta);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyAddSign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyAddSign")
                     .Input(_var)
                     .Input(_m)
                     .Input(_lr)
                     .Input(_alpha)
                     .Input(_sign_decay)
                     .Input(_beta)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyAddSign::ApplyAddSign(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input var, ::tensorflow::Input m,
                           ::tensorflow::Input lr, ::tensorflow::Input alpha,
                           ::tensorflow::Input sign_decay, ::tensorflow::Input
                           beta, ::tensorflow::Input grad)
  : ApplyAddSign(scope, var, m, lr, alpha, sign_decay, beta, grad, ApplyAddSign::Attrs()) {}

ApplyCenteredRMSProp::ApplyCenteredRMSProp(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input mg,
                                           ::tensorflow::Input ms,
                                           ::tensorflow::Input mom,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input rho,
                                           ::tensorflow::Input momentum,
                                           ::tensorflow::Input epsilon,
                                           ::tensorflow::Input grad, const
                                           ApplyCenteredRMSProp::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _mg = ::tensorflow::ops::AsNodeOut(scope, mg);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyCenteredRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyCenteredRMSProp")
                     .Input(_var)
                     .Input(_mg)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
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

ApplyCenteredRMSProp::ApplyCenteredRMSProp(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input mg,
                                           ::tensorflow::Input ms,
                                           ::tensorflow::Input mom,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input rho,
                                           ::tensorflow::Input momentum,
                                           ::tensorflow::Input epsilon,
                                           ::tensorflow::Input grad)
  : ApplyCenteredRMSProp(scope, var, mg, ms, mom, lr, rho, momentum, epsilon, grad, ApplyCenteredRMSProp::Attrs()) {}

ApplyFtrl::ApplyFtrl(const ::tensorflow::Scope& scope, ::tensorflow::Input var,
                     ::tensorflow::Input accum, ::tensorflow::Input linear,
                     ::tensorflow::Input grad, ::tensorflow::Input lr,
                     ::tensorflow::Input l1, ::tensorflow::Input l2,
                     ::tensorflow::Input lr_power, const ApplyFtrl::Attrs&
                     attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyFtrl");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyFtrl")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyFtrl::ApplyFtrl(const ::tensorflow::Scope& scope, ::tensorflow::Input var,
                     ::tensorflow::Input accum, ::tensorflow::Input linear,
                     ::tensorflow::Input grad, ::tensorflow::Input lr,
                     ::tensorflow::Input l1, ::tensorflow::Input l2,
                     ::tensorflow::Input lr_power)
  : ApplyFtrl(scope, var, accum, linear, grad, lr, l1, l2, lr_power, ApplyFtrl::Attrs()) {}

ApplyFtrlV2::ApplyFtrlV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         var, ::tensorflow::Input accum, ::tensorflow::Input
                         linear, ::tensorflow::Input grad, ::tensorflow::Input
                         lr, ::tensorflow::Input l1, ::tensorflow::Input l2,
                         ::tensorflow::Input l2_shrinkage, ::tensorflow::Input
                         lr_power, const ApplyFtrlV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _l2_shrinkage = ::tensorflow::ops::AsNodeOut(scope, l2_shrinkage);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyFtrlV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyFtrlV2")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_l2_shrinkage)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyFtrlV2::ApplyFtrlV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         var, ::tensorflow::Input accum, ::tensorflow::Input
                         linear, ::tensorflow::Input grad, ::tensorflow::Input
                         lr, ::tensorflow::Input l1, ::tensorflow::Input l2,
                         ::tensorflow::Input l2_shrinkage, ::tensorflow::Input
                         lr_power)
  : ApplyFtrlV2(scope, var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power, ApplyFtrlV2::Attrs()) {}

ApplyGradientDescent::ApplyGradientDescent(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input alpha,
                                           ::tensorflow::Input delta, const
                                           ApplyGradientDescent::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _delta = ::tensorflow::ops::AsNodeOut(scope, delta);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyGradientDescent");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyGradientDescent")
                     .Input(_var)
                     .Input(_alpha)
                     .Input(_delta)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyGradientDescent::ApplyGradientDescent(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input alpha,
                                           ::tensorflow::Input delta)
  : ApplyGradientDescent(scope, var, alpha, delta, ApplyGradientDescent::Attrs()) {}

ApplyMomentum::ApplyMomentum(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input var, ::tensorflow::Input
                             accum, ::tensorflow::Input lr, ::tensorflow::Input
                             grad, ::tensorflow::Input momentum, const
                             ApplyMomentum::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyMomentum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyMomentum")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Input(_momentum)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("use_nesterov", attrs.use_nesterov_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyMomentum::ApplyMomentum(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input var, ::tensorflow::Input
                             accum, ::tensorflow::Input lr, ::tensorflow::Input
                             grad, ::tensorflow::Input momentum)
  : ApplyMomentum(scope, var, accum, lr, grad, momentum, ApplyMomentum::Attrs()) {}

ApplyPowerSign::ApplyPowerSign(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input var, ::tensorflow::Input m,
                               ::tensorflow::Input lr, ::tensorflow::Input
                               logbase, ::tensorflow::Input sign_decay,
                               ::tensorflow::Input beta, ::tensorflow::Input
                               grad, const ApplyPowerSign::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _logbase = ::tensorflow::ops::AsNodeOut(scope, logbase);
  if (!scope.ok()) return;
  auto _sign_decay = ::tensorflow::ops::AsNodeOut(scope, sign_decay);
  if (!scope.ok()) return;
  auto _beta = ::tensorflow::ops::AsNodeOut(scope, beta);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyPowerSign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyPowerSign")
                     .Input(_var)
                     .Input(_m)
                     .Input(_lr)
                     .Input(_logbase)
                     .Input(_sign_decay)
                     .Input(_beta)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyPowerSign::ApplyPowerSign(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input var, ::tensorflow::Input m,
                               ::tensorflow::Input lr, ::tensorflow::Input
                               logbase, ::tensorflow::Input sign_decay,
                               ::tensorflow::Input beta, ::tensorflow::Input
                               grad)
  : ApplyPowerSign(scope, var, m, lr, logbase, sign_decay, beta, grad, ApplyPowerSign::Attrs()) {}

ApplyProximalAdagrad::ApplyProximalAdagrad(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input accum,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input l1,
                                           ::tensorflow::Input l2,
                                           ::tensorflow::Input grad, const
                                           ApplyProximalAdagrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyProximalAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyProximalAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyProximalAdagrad::ApplyProximalAdagrad(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input accum,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input l1,
                                           ::tensorflow::Input l2,
                                           ::tensorflow::Input grad)
  : ApplyProximalAdagrad(scope, var, accum, lr, l1, l2, grad, ApplyProximalAdagrad::Attrs()) {}

ApplyProximalGradientDescent::ApplyProximalGradientDescent(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           alpha,
                                                           ::tensorflow::Input
                                                           l1,
                                                           ::tensorflow::Input
                                                           l2,
                                                           ::tensorflow::Input
                                                           delta, const
                                                           ApplyProximalGradientDescent::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _delta = ::tensorflow::ops::AsNodeOut(scope, delta);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyProximalGradientDescent");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyProximalGradientDescent")
                     .Input(_var)
                     .Input(_alpha)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_delta)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

ApplyProximalGradientDescent::ApplyProximalGradientDescent(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           alpha,
                                                           ::tensorflow::Input
                                                           l1,
                                                           ::tensorflow::Input
                                                           l2,
                                                           ::tensorflow::Input
                                                           delta)
  : ApplyProximalGradientDescent(scope, var, alpha, l1, l2, delta, ApplyProximalGradientDescent::Attrs()) {}

ApplyRMSProp::ApplyRMSProp(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input var, ::tensorflow::Input ms,
                           ::tensorflow::Input mom, ::tensorflow::Input lr,
                           ::tensorflow::Input rho, ::tensorflow::Input
                           momentum, ::tensorflow::Input epsilon,
                           ::tensorflow::Input grad, const ApplyRMSProp::Attrs&
                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApplyRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApplyRMSProp")
                     .Input(_var)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
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

ApplyRMSProp::ApplyRMSProp(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input var, ::tensorflow::Input ms,
                           ::tensorflow::Input mom, ::tensorflow::Input lr,
                           ::tensorflow::Input rho, ::tensorflow::Input
                           momentum, ::tensorflow::Input epsilon,
                           ::tensorflow::Input grad)
  : ApplyRMSProp(scope, var, ms, mom, lr, rho, momentum, epsilon, grad, ApplyRMSProp::Attrs()) {}

ResourceApplyAdadelta::ResourceApplyAdadelta(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input var,
                                             ::tensorflow::Input accum,
                                             ::tensorflow::Input accum_update,
                                             ::tensorflow::Input lr,
                                             ::tensorflow::Input rho,
                                             ::tensorflow::Input epsilon,
                                             ::tensorflow::Input grad, const
                                             ResourceApplyAdadelta::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _accum_update = ::tensorflow::ops::AsNodeOut(scope, accum_update);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyAdadelta");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyAdadelta")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_accum_update)
                     .Input(_lr)
                     .Input(_rho)
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

ResourceApplyAdadelta::ResourceApplyAdadelta(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input var,
                                             ::tensorflow::Input accum,
                                             ::tensorflow::Input accum_update,
                                             ::tensorflow::Input lr,
                                             ::tensorflow::Input rho,
                                             ::tensorflow::Input epsilon,
                                             ::tensorflow::Input grad)
  : ResourceApplyAdadelta(scope, var, accum, accum_update, lr, rho, epsilon, grad, ResourceApplyAdadelta::Attrs()) {}

ResourceApplyAdagrad::ResourceApplyAdagrad(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input accum,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input grad, const
                                           ResourceApplyAdagrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("update_slots", attrs.update_slots_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyAdagrad::ResourceApplyAdagrad(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input accum,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input grad)
  : ResourceApplyAdagrad(scope, var, accum, lr, grad, ResourceApplyAdagrad::Attrs()) {}

ResourceApplyAdagradDA::ResourceApplyAdagradDA(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input var,
                                               ::tensorflow::Input
                                               gradient_accumulator,
                                               ::tensorflow::Input
                                               gradient_squared_accumulator,
                                               ::tensorflow::Input grad,
                                               ::tensorflow::Input lr,
                                               ::tensorflow::Input l1,
                                               ::tensorflow::Input l2,
                                               ::tensorflow::Input global_step,
                                               const
                                               ResourceApplyAdagradDA::Attrs&
                                               attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _gradient_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_accumulator);
  if (!scope.ok()) return;
  auto _gradient_squared_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_squared_accumulator);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _global_step = ::tensorflow::ops::AsNodeOut(scope, global_step);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyAdagradDA");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyAdagradDA")
                     .Input(_var)
                     .Input(_gradient_accumulator)
                     .Input(_gradient_squared_accumulator)
                     .Input(_grad)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_global_step)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyAdagradDA::ResourceApplyAdagradDA(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input var,
                                               ::tensorflow::Input
                                               gradient_accumulator,
                                               ::tensorflow::Input
                                               gradient_squared_accumulator,
                                               ::tensorflow::Input grad,
                                               ::tensorflow::Input lr,
                                               ::tensorflow::Input l1,
                                               ::tensorflow::Input l2,
                                               ::tensorflow::Input global_step)
  : ResourceApplyAdagradDA(scope, var, gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2, global_step, ResourceApplyAdagradDA::Attrs()) {}

ResourceApplyAdam::ResourceApplyAdam(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input var,
                                     ::tensorflow::Input m, ::tensorflow::Input
                                     v, ::tensorflow::Input beta1_power,
                                     ::tensorflow::Input beta2_power,
                                     ::tensorflow::Input lr,
                                     ::tensorflow::Input beta1,
                                     ::tensorflow::Input beta2,
                                     ::tensorflow::Input epsilon,
                                     ::tensorflow::Input grad, const
                                     ResourceApplyAdam::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _v = ::tensorflow::ops::AsNodeOut(scope, v);
  if (!scope.ok()) return;
  auto _beta1_power = ::tensorflow::ops::AsNodeOut(scope, beta1_power);
  if (!scope.ok()) return;
  auto _beta2_power = ::tensorflow::ops::AsNodeOut(scope, beta2_power);
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
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyAdam");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyAdam")
                     .Input(_var)
                     .Input(_m)
                     .Input(_v)
                     .Input(_beta1_power)
                     .Input(_beta2_power)
                     .Input(_lr)
                     .Input(_beta1)
                     .Input(_beta2)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("use_nesterov", attrs.use_nesterov_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyAdam::ResourceApplyAdam(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input var,
                                     ::tensorflow::Input m, ::tensorflow::Input
                                     v, ::tensorflow::Input beta1_power,
                                     ::tensorflow::Input beta2_power,
                                     ::tensorflow::Input lr,
                                     ::tensorflow::Input beta1,
                                     ::tensorflow::Input beta2,
                                     ::tensorflow::Input epsilon,
                                     ::tensorflow::Input grad)
  : ResourceApplyAdam(scope, var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, ResourceApplyAdam::Attrs()) {}

ResourceApplyAddSign::ResourceApplyAddSign(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input m,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input alpha,
                                           ::tensorflow::Input sign_decay,
                                           ::tensorflow::Input beta,
                                           ::tensorflow::Input grad, const
                                           ResourceApplyAddSign::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _sign_decay = ::tensorflow::ops::AsNodeOut(scope, sign_decay);
  if (!scope.ok()) return;
  auto _beta = ::tensorflow::ops::AsNodeOut(scope, beta);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyAddSign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyAddSign")
                     .Input(_var)
                     .Input(_m)
                     .Input(_lr)
                     .Input(_alpha)
                     .Input(_sign_decay)
                     .Input(_beta)
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

ResourceApplyAddSign::ResourceApplyAddSign(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input m,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input alpha,
                                           ::tensorflow::Input sign_decay,
                                           ::tensorflow::Input beta,
                                           ::tensorflow::Input grad)
  : ResourceApplyAddSign(scope, var, m, lr, alpha, sign_decay, beta, grad, ResourceApplyAddSign::Attrs()) {}

ResourceApplyCenteredRMSProp::ResourceApplyCenteredRMSProp(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           mg,
                                                           ::tensorflow::Input
                                                           ms,
                                                           ::tensorflow::Input
                                                           mom,
                                                           ::tensorflow::Input
                                                           lr,
                                                           ::tensorflow::Input
                                                           rho,
                                                           ::tensorflow::Input
                                                           momentum,
                                                           ::tensorflow::Input
                                                           epsilon,
                                                           ::tensorflow::Input
                                                           grad, const
                                                           ResourceApplyCenteredRMSProp::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _mg = ::tensorflow::ops::AsNodeOut(scope, mg);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyCenteredRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyCenteredRMSProp")
                     .Input(_var)
                     .Input(_mg)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
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

ResourceApplyCenteredRMSProp::ResourceApplyCenteredRMSProp(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           mg,
                                                           ::tensorflow::Input
                                                           ms,
                                                           ::tensorflow::Input
                                                           mom,
                                                           ::tensorflow::Input
                                                           lr,
                                                           ::tensorflow::Input
                                                           rho,
                                                           ::tensorflow::Input
                                                           momentum,
                                                           ::tensorflow::Input
                                                           epsilon,
                                                           ::tensorflow::Input
                                                           grad)
  : ResourceApplyCenteredRMSProp(scope, var, mg, ms, mom, lr, rho, momentum, epsilon, grad, ResourceApplyCenteredRMSProp::Attrs()) {}

ResourceApplyFtrl::ResourceApplyFtrl(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input var,
                                     ::tensorflow::Input accum,
                                     ::tensorflow::Input linear,
                                     ::tensorflow::Input grad,
                                     ::tensorflow::Input lr,
                                     ::tensorflow::Input l1,
                                     ::tensorflow::Input l2,
                                     ::tensorflow::Input lr_power, const
                                     ResourceApplyFtrl::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyFtrl");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyFtrl")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyFtrl::ResourceApplyFtrl(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input var,
                                     ::tensorflow::Input accum,
                                     ::tensorflow::Input linear,
                                     ::tensorflow::Input grad,
                                     ::tensorflow::Input lr,
                                     ::tensorflow::Input l1,
                                     ::tensorflow::Input l2,
                                     ::tensorflow::Input lr_power)
  : ResourceApplyFtrl(scope, var, accum, linear, grad, lr, l1, l2, lr_power, ResourceApplyFtrl::Attrs()) {}

ResourceApplyFtrlV2::ResourceApplyFtrlV2(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input accum,
                                         ::tensorflow::Input linear,
                                         ::tensorflow::Input grad,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input l1,
                                         ::tensorflow::Input l2,
                                         ::tensorflow::Input l2_shrinkage,
                                         ::tensorflow::Input lr_power, const
                                         ResourceApplyFtrlV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _l2_shrinkage = ::tensorflow::ops::AsNodeOut(scope, l2_shrinkage);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyFtrlV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyFtrlV2")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_l2_shrinkage)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyFtrlV2::ResourceApplyFtrlV2(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input accum,
                                         ::tensorflow::Input linear,
                                         ::tensorflow::Input grad,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input l1,
                                         ::tensorflow::Input l2,
                                         ::tensorflow::Input l2_shrinkage,
                                         ::tensorflow::Input lr_power)
  : ResourceApplyFtrlV2(scope, var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power, ResourceApplyFtrlV2::Attrs()) {}

ResourceApplyGradientDescent::ResourceApplyGradientDescent(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           alpha,
                                                           ::tensorflow::Input
                                                           delta, const
                                                           ResourceApplyGradientDescent::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _delta = ::tensorflow::ops::AsNodeOut(scope, delta);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyGradientDescent");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyGradientDescent")
                     .Input(_var)
                     .Input(_alpha)
                     .Input(_delta)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyGradientDescent::ResourceApplyGradientDescent(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           alpha,
                                                           ::tensorflow::Input
                                                           delta)
  : ResourceApplyGradientDescent(scope, var, alpha, delta, ResourceApplyGradientDescent::Attrs()) {}

ResourceApplyMomentum::ResourceApplyMomentum(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input var,
                                             ::tensorflow::Input accum,
                                             ::tensorflow::Input lr,
                                             ::tensorflow::Input grad,
                                             ::tensorflow::Input momentum,
                                             const
                                             ResourceApplyMomentum::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyMomentum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyMomentum")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Input(_momentum)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("use_nesterov", attrs.use_nesterov_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyMomentum::ResourceApplyMomentum(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input var,
                                             ::tensorflow::Input accum,
                                             ::tensorflow::Input lr,
                                             ::tensorflow::Input grad,
                                             ::tensorflow::Input momentum)
  : ResourceApplyMomentum(scope, var, accum, lr, grad, momentum, ResourceApplyMomentum::Attrs()) {}

ResourceApplyPowerSign::ResourceApplyPowerSign(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input var,
                                               ::tensorflow::Input m,
                                               ::tensorflow::Input lr,
                                               ::tensorflow::Input logbase,
                                               ::tensorflow::Input sign_decay,
                                               ::tensorflow::Input beta,
                                               ::tensorflow::Input grad, const
                                               ResourceApplyPowerSign::Attrs&
                                               attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _logbase = ::tensorflow::ops::AsNodeOut(scope, logbase);
  if (!scope.ok()) return;
  auto _sign_decay = ::tensorflow::ops::AsNodeOut(scope, sign_decay);
  if (!scope.ok()) return;
  auto _beta = ::tensorflow::ops::AsNodeOut(scope, beta);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyPowerSign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyPowerSign")
                     .Input(_var)
                     .Input(_m)
                     .Input(_lr)
                     .Input(_logbase)
                     .Input(_sign_decay)
                     .Input(_beta)
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

ResourceApplyPowerSign::ResourceApplyPowerSign(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input var,
                                               ::tensorflow::Input m,
                                               ::tensorflow::Input lr,
                                               ::tensorflow::Input logbase,
                                               ::tensorflow::Input sign_decay,
                                               ::tensorflow::Input beta,
                                               ::tensorflow::Input grad)
  : ResourceApplyPowerSign(scope, var, m, lr, logbase, sign_decay, beta, grad, ResourceApplyPowerSign::Attrs()) {}

ResourceApplyProximalAdagrad::ResourceApplyProximalAdagrad(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           accum,
                                                           ::tensorflow::Input
                                                           lr,
                                                           ::tensorflow::Input
                                                           l1,
                                                           ::tensorflow::Input
                                                           l2,
                                                           ::tensorflow::Input
                                                           grad, const
                                                           ResourceApplyProximalAdagrad::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyProximalAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyProximalAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
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

ResourceApplyProximalAdagrad::ResourceApplyProximalAdagrad(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           accum,
                                                           ::tensorflow::Input
                                                           lr,
                                                           ::tensorflow::Input
                                                           l1,
                                                           ::tensorflow::Input
                                                           l2,
                                                           ::tensorflow::Input
                                                           grad)
  : ResourceApplyProximalAdagrad(scope, var, accum, lr, l1, l2, grad, ResourceApplyProximalAdagrad::Attrs()) {}

ResourceApplyProximalGradientDescent::ResourceApplyProximalGradientDescent(const
                                                                           ::tensorflow::Scope&
                                                                           scope,
                                                                           ::tensorflow::Input
                                                                           var,
                                                                           ::tensorflow::Input
                                                                           alpha,
                                                                           ::tensorflow::Input
                                                                           l1,
                                                                           ::tensorflow::Input
                                                                           l2,
                                                                           ::tensorflow::Input
                                                                           delta,
                                                                           const
                                                                           ResourceApplyProximalGradientDescent::Attrs&
                                                                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _delta = ::tensorflow::ops::AsNodeOut(scope, delta);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyProximalGradientDescent");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyProximalGradientDescent")
                     .Input(_var)
                     .Input(_alpha)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_delta)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceApplyProximalGradientDescent::ResourceApplyProximalGradientDescent(const
                                                                           ::tensorflow::Scope&
                                                                           scope,
                                                                           ::tensorflow::Input
                                                                           var,
                                                                           ::tensorflow::Input
                                                                           alpha,
                                                                           ::tensorflow::Input
                                                                           l1,
                                                                           ::tensorflow::Input
                                                                           l2,
                                                                           ::tensorflow::Input
                                                                           delta)
  : ResourceApplyProximalGradientDescent(scope, var, alpha, l1, l2, delta, ResourceApplyProximalGradientDescent::Attrs()) {}

ResourceApplyRMSProp::ResourceApplyRMSProp(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input ms,
                                           ::tensorflow::Input mom,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input rho,
                                           ::tensorflow::Input momentum,
                                           ::tensorflow::Input epsilon,
                                           ::tensorflow::Input grad, const
                                           ResourceApplyRMSProp::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceApplyRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceApplyRMSProp")
                     .Input(_var)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
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

ResourceApplyRMSProp::ResourceApplyRMSProp(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input ms,
                                           ::tensorflow::Input mom,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input rho,
                                           ::tensorflow::Input momentum,
                                           ::tensorflow::Input epsilon,
                                           ::tensorflow::Input grad)
  : ResourceApplyRMSProp(scope, var, ms, mom, lr, rho, momentum, epsilon, grad, ResourceApplyRMSProp::Attrs()) {}

ResourceSparseApplyAdadelta::ResourceSparseApplyAdadelta(const
                                                         ::tensorflow::Scope&
                                                         scope,
                                                         ::tensorflow::Input
                                                         var,
                                                         ::tensorflow::Input
                                                         accum,
                                                         ::tensorflow::Input
                                                         accum_update,
                                                         ::tensorflow::Input
                                                         lr,
                                                         ::tensorflow::Input
                                                         rho,
                                                         ::tensorflow::Input
                                                         epsilon,
                                                         ::tensorflow::Input
                                                         grad,
                                                         ::tensorflow::Input
                                                         indices, const
                                                         ResourceSparseApplyAdadelta::Attrs&
                                                         attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _accum_update = ::tensorflow::ops::AsNodeOut(scope, accum_update);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyAdadelta");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyAdadelta")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_accum_update)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyAdadelta::ResourceSparseApplyAdadelta(const
                                                         ::tensorflow::Scope&
                                                         scope,
                                                         ::tensorflow::Input
                                                         var,
                                                         ::tensorflow::Input
                                                         accum,
                                                         ::tensorflow::Input
                                                         accum_update,
                                                         ::tensorflow::Input
                                                         lr,
                                                         ::tensorflow::Input
                                                         rho,
                                                         ::tensorflow::Input
                                                         epsilon,
                                                         ::tensorflow::Input
                                                         grad,
                                                         ::tensorflow::Input
                                                         indices)
  : ResourceSparseApplyAdadelta(scope, var, accum, accum_update, lr, rho, epsilon, grad, indices, ResourceSparseApplyAdadelta::Attrs()) {}

ResourceSparseApplyAdagrad::ResourceSparseApplyAdagrad(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input
                                                       accum,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices, const
                                                       ResourceSparseApplyAdagrad::Attrs&
                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("update_slots", attrs.update_slots_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyAdagrad::ResourceSparseApplyAdagrad(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input
                                                       accum,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices)
  : ResourceSparseApplyAdagrad(scope, var, accum, lr, grad, indices, ResourceSparseApplyAdagrad::Attrs()) {}

ResourceSparseApplyAdagradDA::ResourceSparseApplyAdagradDA(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           gradient_accumulator,
                                                           ::tensorflow::Input
                                                           gradient_squared_accumulator,
                                                           ::tensorflow::Input
                                                           grad,
                                                           ::tensorflow::Input
                                                           indices,
                                                           ::tensorflow::Input
                                                           lr,
                                                           ::tensorflow::Input
                                                           l1,
                                                           ::tensorflow::Input
                                                           l2,
                                                           ::tensorflow::Input
                                                           global_step, const
                                                           ResourceSparseApplyAdagradDA::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _gradient_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_accumulator);
  if (!scope.ok()) return;
  auto _gradient_squared_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_squared_accumulator);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _global_step = ::tensorflow::ops::AsNodeOut(scope, global_step);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyAdagradDA");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyAdagradDA")
                     .Input(_var)
                     .Input(_gradient_accumulator)
                     .Input(_gradient_squared_accumulator)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_global_step)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyAdagradDA::ResourceSparseApplyAdagradDA(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           var,
                                                           ::tensorflow::Input
                                                           gradient_accumulator,
                                                           ::tensorflow::Input
                                                           gradient_squared_accumulator,
                                                           ::tensorflow::Input
                                                           grad,
                                                           ::tensorflow::Input
                                                           indices,
                                                           ::tensorflow::Input
                                                           lr,
                                                           ::tensorflow::Input
                                                           l1,
                                                           ::tensorflow::Input
                                                           l2,
                                                           ::tensorflow::Input
                                                           global_step)
  : ResourceSparseApplyAdagradDA(scope, var, gradient_accumulator, gradient_squared_accumulator, grad, indices, lr, l1, l2, global_step, ResourceSparseApplyAdagradDA::Attrs()) {}

ResourceSparseApplyCenteredRMSProp::ResourceSparseApplyCenteredRMSProp(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       var,
                                                                       ::tensorflow::Input
                                                                       mg,
                                                                       ::tensorflow::Input
                                                                       ms,
                                                                       ::tensorflow::Input
                                                                       mom,
                                                                       ::tensorflow::Input
                                                                       lr,
                                                                       ::tensorflow::Input
                                                                       rho,
                                                                       ::tensorflow::Input
                                                                       momentum,
                                                                       ::tensorflow::Input
                                                                       epsilon,
                                                                       ::tensorflow::Input
                                                                       grad,
                                                                       ::tensorflow::Input
                                                                       indices,
                                                                       const
                                                                       ResourceSparseApplyCenteredRMSProp::Attrs&
                                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _mg = ::tensorflow::ops::AsNodeOut(scope, mg);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyCenteredRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyCenteredRMSProp")
                     .Input(_var)
                     .Input(_mg)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyCenteredRMSProp::ResourceSparseApplyCenteredRMSProp(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       var,
                                                                       ::tensorflow::Input
                                                                       mg,
                                                                       ::tensorflow::Input
                                                                       ms,
                                                                       ::tensorflow::Input
                                                                       mom,
                                                                       ::tensorflow::Input
                                                                       lr,
                                                                       ::tensorflow::Input
                                                                       rho,
                                                                       ::tensorflow::Input
                                                                       momentum,
                                                                       ::tensorflow::Input
                                                                       epsilon,
                                                                       ::tensorflow::Input
                                                                       grad,
                                                                       ::tensorflow::Input
                                                                       indices)
  : ResourceSparseApplyCenteredRMSProp(scope, var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices, ResourceSparseApplyCenteredRMSProp::Attrs()) {}

ResourceSparseApplyFtrl::ResourceSparseApplyFtrl(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 var, ::tensorflow::Input
                                                 accum, ::tensorflow::Input
                                                 linear, ::tensorflow::Input
                                                 grad, ::tensorflow::Input
                                                 indices, ::tensorflow::Input
                                                 lr, ::tensorflow::Input l1,
                                                 ::tensorflow::Input l2,
                                                 ::tensorflow::Input lr_power,
                                                 const
                                                 ResourceSparseApplyFtrl::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyFtrl");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyFtrl")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyFtrl::ResourceSparseApplyFtrl(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 var, ::tensorflow::Input
                                                 accum, ::tensorflow::Input
                                                 linear, ::tensorflow::Input
                                                 grad, ::tensorflow::Input
                                                 indices, ::tensorflow::Input
                                                 lr, ::tensorflow::Input l1,
                                                 ::tensorflow::Input l2,
                                                 ::tensorflow::Input lr_power)
  : ResourceSparseApplyFtrl(scope, var, accum, linear, grad, indices, lr, l1, l2, lr_power, ResourceSparseApplyFtrl::Attrs()) {}

ResourceSparseApplyFtrlV2::ResourceSparseApplyFtrlV2(const ::tensorflow::Scope&
                                                     scope, ::tensorflow::Input
                                                     var, ::tensorflow::Input
                                                     accum, ::tensorflow::Input
                                                     linear,
                                                     ::tensorflow::Input grad,
                                                     ::tensorflow::Input
                                                     indices,
                                                     ::tensorflow::Input lr,
                                                     ::tensorflow::Input l1,
                                                     ::tensorflow::Input l2,
                                                     ::tensorflow::Input
                                                     l2_shrinkage,
                                                     ::tensorflow::Input
                                                     lr_power, const
                                                     ResourceSparseApplyFtrlV2::Attrs&
                                                     attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _l2_shrinkage = ::tensorflow::ops::AsNodeOut(scope, l2_shrinkage);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyFtrlV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyFtrlV2")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_l2_shrinkage)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyFtrlV2::ResourceSparseApplyFtrlV2(const ::tensorflow::Scope&
                                                     scope, ::tensorflow::Input
                                                     var, ::tensorflow::Input
                                                     accum, ::tensorflow::Input
                                                     linear,
                                                     ::tensorflow::Input grad,
                                                     ::tensorflow::Input
                                                     indices,
                                                     ::tensorflow::Input lr,
                                                     ::tensorflow::Input l1,
                                                     ::tensorflow::Input l2,
                                                     ::tensorflow::Input
                                                     l2_shrinkage,
                                                     ::tensorflow::Input
                                                     lr_power)
  : ResourceSparseApplyFtrlV2(scope, var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power, ResourceSparseApplyFtrlV2::Attrs()) {}

ResourceSparseApplyMomentum::ResourceSparseApplyMomentum(const
                                                         ::tensorflow::Scope&
                                                         scope,
                                                         ::tensorflow::Input
                                                         var,
                                                         ::tensorflow::Input
                                                         accum,
                                                         ::tensorflow::Input
                                                         lr,
                                                         ::tensorflow::Input
                                                         grad,
                                                         ::tensorflow::Input
                                                         indices,
                                                         ::tensorflow::Input
                                                         momentum, const
                                                         ResourceSparseApplyMomentum::Attrs&
                                                         attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyMomentum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyMomentum")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_momentum)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("use_nesterov", attrs.use_nesterov_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyMomentum::ResourceSparseApplyMomentum(const
                                                         ::tensorflow::Scope&
                                                         scope,
                                                         ::tensorflow::Input
                                                         var,
                                                         ::tensorflow::Input
                                                         accum,
                                                         ::tensorflow::Input
                                                         lr,
                                                         ::tensorflow::Input
                                                         grad,
                                                         ::tensorflow::Input
                                                         indices,
                                                         ::tensorflow::Input
                                                         momentum)
  : ResourceSparseApplyMomentum(scope, var, accum, lr, grad, indices, momentum, ResourceSparseApplyMomentum::Attrs()) {}

ResourceSparseApplyProximalAdagrad::ResourceSparseApplyProximalAdagrad(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       var,
                                                                       ::tensorflow::Input
                                                                       accum,
                                                                       ::tensorflow::Input
                                                                       lr,
                                                                       ::tensorflow::Input
                                                                       l1,
                                                                       ::tensorflow::Input
                                                                       l2,
                                                                       ::tensorflow::Input
                                                                       grad,
                                                                       ::tensorflow::Input
                                                                       indices,
                                                                       const
                                                                       ResourceSparseApplyProximalAdagrad::Attrs&
                                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyProximalAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyProximalAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyProximalAdagrad::ResourceSparseApplyProximalAdagrad(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       var,
                                                                       ::tensorflow::Input
                                                                       accum,
                                                                       ::tensorflow::Input
                                                                       lr,
                                                                       ::tensorflow::Input
                                                                       l1,
                                                                       ::tensorflow::Input
                                                                       l2,
                                                                       ::tensorflow::Input
                                                                       grad,
                                                                       ::tensorflow::Input
                                                                       indices)
  : ResourceSparseApplyProximalAdagrad(scope, var, accum, lr, l1, l2, grad, indices, ResourceSparseApplyProximalAdagrad::Attrs()) {}

ResourceSparseApplyProximalGradientDescent::ResourceSparseApplyProximalGradientDescent(const ::tensorflow::Scope& scope, ::tensorflow::Input var, ::tensorflow::Input alpha, ::tensorflow::Input l1, ::tensorflow::Input l2, ::tensorflow::Input grad, ::tensorflow::Input indices, const ResourceSparseApplyProximalGradientDescent::Attrs&
                                                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyProximalGradientDescent");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyProximalGradientDescent")
                     .Input(_var)
                     .Input(_alpha)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyProximalGradientDescent::ResourceSparseApplyProximalGradientDescent(const ::tensorflow::Scope& scope, ::tensorflow::Input var, ::tensorflow::Input alpha, ::tensorflow::Input l1, ::tensorflow::Input l2, ::tensorflow::Input grad, ::tensorflow::Input
                                                                                       indices)
  : ResourceSparseApplyProximalGradientDescent(scope, var, alpha, l1, l2, grad, indices, ResourceSparseApplyProximalGradientDescent::Attrs()) {}

ResourceSparseApplyRMSProp::ResourceSparseApplyRMSProp(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input ms,
                                                       ::tensorflow::Input mom,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input rho,
                                                       ::tensorflow::Input
                                                       momentum,
                                                       ::tensorflow::Input
                                                       epsilon,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices, const
                                                       ResourceSparseApplyRMSProp::Attrs&
                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceSparseApplyRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceSparseApplyRMSProp")
                     .Input(_var)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceSparseApplyRMSProp::ResourceSparseApplyRMSProp(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input ms,
                                                       ::tensorflow::Input mom,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input rho,
                                                       ::tensorflow::Input
                                                       momentum,
                                                       ::tensorflow::Input
                                                       epsilon,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices)
  : ResourceSparseApplyRMSProp(scope, var, ms, mom, lr, rho, momentum, epsilon, grad, indices, ResourceSparseApplyRMSProp::Attrs()) {}

SparseApplyAdadelta::SparseApplyAdadelta(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input accum,
                                         ::tensorflow::Input accum_update,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input rho,
                                         ::tensorflow::Input epsilon,
                                         ::tensorflow::Input grad,
                                         ::tensorflow::Input indices, const
                                         SparseApplyAdadelta::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _accum_update = ::tensorflow::ops::AsNodeOut(scope, accum_update);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyAdadelta");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyAdadelta")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_accum_update)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyAdadelta::SparseApplyAdadelta(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input accum,
                                         ::tensorflow::Input accum_update,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input rho,
                                         ::tensorflow::Input epsilon,
                                         ::tensorflow::Input grad,
                                         ::tensorflow::Input indices)
  : SparseApplyAdadelta(scope, var, accum, accum_update, lr, rho, epsilon, grad, indices, SparseApplyAdadelta::Attrs()) {}

SparseApplyAdagrad::SparseApplyAdagrad(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input var,
                                       ::tensorflow::Input accum,
                                       ::tensorflow::Input lr,
                                       ::tensorflow::Input grad,
                                       ::tensorflow::Input indices, const
                                       SparseApplyAdagrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("update_slots", attrs.update_slots_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyAdagrad::SparseApplyAdagrad(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input var,
                                       ::tensorflow::Input accum,
                                       ::tensorflow::Input lr,
                                       ::tensorflow::Input grad,
                                       ::tensorflow::Input indices)
  : SparseApplyAdagrad(scope, var, accum, lr, grad, indices, SparseApplyAdagrad::Attrs()) {}

SparseApplyAdagradDA::SparseApplyAdagradDA(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input
                                           gradient_accumulator,
                                           ::tensorflow::Input
                                           gradient_squared_accumulator,
                                           ::tensorflow::Input grad,
                                           ::tensorflow::Input indices,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input l1,
                                           ::tensorflow::Input l2,
                                           ::tensorflow::Input global_step,
                                           const SparseApplyAdagradDA::Attrs&
                                           attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _gradient_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_accumulator);
  if (!scope.ok()) return;
  auto _gradient_squared_accumulator = ::tensorflow::ops::AsNodeOut(scope, gradient_squared_accumulator);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _global_step = ::tensorflow::ops::AsNodeOut(scope, global_step);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyAdagradDA");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyAdagradDA")
                     .Input(_var)
                     .Input(_gradient_accumulator)
                     .Input(_gradient_squared_accumulator)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_global_step)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyAdagradDA::SparseApplyAdagradDA(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input var,
                                           ::tensorflow::Input
                                           gradient_accumulator,
                                           ::tensorflow::Input
                                           gradient_squared_accumulator,
                                           ::tensorflow::Input grad,
                                           ::tensorflow::Input indices,
                                           ::tensorflow::Input lr,
                                           ::tensorflow::Input l1,
                                           ::tensorflow::Input l2,
                                           ::tensorflow::Input global_step)
  : SparseApplyAdagradDA(scope, var, gradient_accumulator, gradient_squared_accumulator, grad, indices, lr, l1, l2, global_step, SparseApplyAdagradDA::Attrs()) {}

SparseApplyCenteredRMSProp::SparseApplyCenteredRMSProp(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input mg,
                                                       ::tensorflow::Input ms,
                                                       ::tensorflow::Input mom,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input rho,
                                                       ::tensorflow::Input
                                                       momentum,
                                                       ::tensorflow::Input
                                                       epsilon,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices, const
                                                       SparseApplyCenteredRMSProp::Attrs&
                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _mg = ::tensorflow::ops::AsNodeOut(scope, mg);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyCenteredRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyCenteredRMSProp")
                     .Input(_var)
                     .Input(_mg)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyCenteredRMSProp::SparseApplyCenteredRMSProp(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input mg,
                                                       ::tensorflow::Input ms,
                                                       ::tensorflow::Input mom,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input rho,
                                                       ::tensorflow::Input
                                                       momentum,
                                                       ::tensorflow::Input
                                                       epsilon,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices)
  : SparseApplyCenteredRMSProp(scope, var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices, SparseApplyCenteredRMSProp::Attrs()) {}

SparseApplyFtrl::SparseApplyFtrl(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input var, ::tensorflow::Input
                                 accum, ::tensorflow::Input linear,
                                 ::tensorflow::Input grad, ::tensorflow::Input
                                 indices, ::tensorflow::Input lr,
                                 ::tensorflow::Input l1, ::tensorflow::Input
                                 l2, ::tensorflow::Input lr_power, const
                                 SparseApplyFtrl::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyFtrl");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyFtrl")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyFtrl::SparseApplyFtrl(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input var, ::tensorflow::Input
                                 accum, ::tensorflow::Input linear,
                                 ::tensorflow::Input grad, ::tensorflow::Input
                                 indices, ::tensorflow::Input lr,
                                 ::tensorflow::Input l1, ::tensorflow::Input
                                 l2, ::tensorflow::Input lr_power)
  : SparseApplyFtrl(scope, var, accum, linear, grad, indices, lr, l1, l2, lr_power, SparseApplyFtrl::Attrs()) {}

SparseApplyFtrlV2::SparseApplyFtrlV2(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input var,
                                     ::tensorflow::Input accum,
                                     ::tensorflow::Input linear,
                                     ::tensorflow::Input grad,
                                     ::tensorflow::Input indices,
                                     ::tensorflow::Input lr,
                                     ::tensorflow::Input l1,
                                     ::tensorflow::Input l2,
                                     ::tensorflow::Input l2_shrinkage,
                                     ::tensorflow::Input lr_power, const
                                     SparseApplyFtrlV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _linear = ::tensorflow::ops::AsNodeOut(scope, linear);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _l2_shrinkage = ::tensorflow::ops::AsNodeOut(scope, l2_shrinkage);
  if (!scope.ok()) return;
  auto _lr_power = ::tensorflow::ops::AsNodeOut(scope, lr_power);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyFtrlV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyFtrlV2")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_linear)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_l2_shrinkage)
                     .Input(_lr_power)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyFtrlV2::SparseApplyFtrlV2(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input var,
                                     ::tensorflow::Input accum,
                                     ::tensorflow::Input linear,
                                     ::tensorflow::Input grad,
                                     ::tensorflow::Input indices,
                                     ::tensorflow::Input lr,
                                     ::tensorflow::Input l1,
                                     ::tensorflow::Input l2,
                                     ::tensorflow::Input l2_shrinkage,
                                     ::tensorflow::Input lr_power)
  : SparseApplyFtrlV2(scope, var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power, SparseApplyFtrlV2::Attrs()) {}

SparseApplyMomentum::SparseApplyMomentum(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input accum,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input grad,
                                         ::tensorflow::Input indices,
                                         ::tensorflow::Input momentum, const
                                         SparseApplyMomentum::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyMomentum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyMomentum")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_momentum)
                     .Attr("use_locking", attrs.use_locking_)
                     .Attr("use_nesterov", attrs.use_nesterov_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyMomentum::SparseApplyMomentum(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input var,
                                         ::tensorflow::Input accum,
                                         ::tensorflow::Input lr,
                                         ::tensorflow::Input grad,
                                         ::tensorflow::Input indices,
                                         ::tensorflow::Input momentum)
  : SparseApplyMomentum(scope, var, accum, lr, grad, indices, momentum, SparseApplyMomentum::Attrs()) {}

SparseApplyProximalAdagrad::SparseApplyProximalAdagrad(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input
                                                       accum,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input l1,
                                                       ::tensorflow::Input l2,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices, const
                                                       SparseApplyProximalAdagrad::Attrs&
                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _accum = ::tensorflow::ops::AsNodeOut(scope, accum);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyProximalAdagrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyProximalAdagrad")
                     .Input(_var)
                     .Input(_accum)
                     .Input(_lr)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyProximalAdagrad::SparseApplyProximalAdagrad(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input var,
                                                       ::tensorflow::Input
                                                       accum,
                                                       ::tensorflow::Input lr,
                                                       ::tensorflow::Input l1,
                                                       ::tensorflow::Input l2,
                                                       ::tensorflow::Input
                                                       grad,
                                                       ::tensorflow::Input
                                                       indices)
  : SparseApplyProximalAdagrad(scope, var, accum, lr, l1, l2, grad, indices, SparseApplyProximalAdagrad::Attrs()) {}

SparseApplyProximalGradientDescent::SparseApplyProximalGradientDescent(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       var,
                                                                       ::tensorflow::Input
                                                                       alpha,
                                                                       ::tensorflow::Input
                                                                       l1,
                                                                       ::tensorflow::Input
                                                                       l2,
                                                                       ::tensorflow::Input
                                                                       grad,
                                                                       ::tensorflow::Input
                                                                       indices,
                                                                       const
                                                                       SparseApplyProximalGradientDescent::Attrs&
                                                                       attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  auto _l1 = ::tensorflow::ops::AsNodeOut(scope, l1);
  if (!scope.ok()) return;
  auto _l2 = ::tensorflow::ops::AsNodeOut(scope, l2);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyProximalGradientDescent");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyProximalGradientDescent")
                     .Input(_var)
                     .Input(_alpha)
                     .Input(_l1)
                     .Input(_l2)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyProximalGradientDescent::SparseApplyProximalGradientDescent(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       var,
                                                                       ::tensorflow::Input
                                                                       alpha,
                                                                       ::tensorflow::Input
                                                                       l1,
                                                                       ::tensorflow::Input
                                                                       l2,
                                                                       ::tensorflow::Input
                                                                       grad,
                                                                       ::tensorflow::Input
                                                                       indices)
  : SparseApplyProximalGradientDescent(scope, var, alpha, l1, l2, grad, indices, SparseApplyProximalGradientDescent::Attrs()) {}

SparseApplyRMSProp::SparseApplyRMSProp(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input var,
                                       ::tensorflow::Input ms,
                                       ::tensorflow::Input mom,
                                       ::tensorflow::Input lr,
                                       ::tensorflow::Input rho,
                                       ::tensorflow::Input momentum,
                                       ::tensorflow::Input epsilon,
                                       ::tensorflow::Input grad,
                                       ::tensorflow::Input indices, const
                                       SparseApplyRMSProp::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _var = ::tensorflow::ops::AsNodeOut(scope, var);
  if (!scope.ok()) return;
  auto _ms = ::tensorflow::ops::AsNodeOut(scope, ms);
  if (!scope.ok()) return;
  auto _mom = ::tensorflow::ops::AsNodeOut(scope, mom);
  if (!scope.ok()) return;
  auto _lr = ::tensorflow::ops::AsNodeOut(scope, lr);
  if (!scope.ok()) return;
  auto _rho = ::tensorflow::ops::AsNodeOut(scope, rho);
  if (!scope.ok()) return;
  auto _momentum = ::tensorflow::ops::AsNodeOut(scope, momentum);
  if (!scope.ok()) return;
  auto _epsilon = ::tensorflow::ops::AsNodeOut(scope, epsilon);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseApplyRMSProp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseApplyRMSProp")
                     .Input(_var)
                     .Input(_ms)
                     .Input(_mom)
                     .Input(_lr)
                     .Input(_rho)
                     .Input(_momentum)
                     .Input(_epsilon)
                     .Input(_grad)
                     .Input(_indices)
                     .Attr("use_locking", attrs.use_locking_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

SparseApplyRMSProp::SparseApplyRMSProp(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input var,
                                       ::tensorflow::Input ms,
                                       ::tensorflow::Input mom,
                                       ::tensorflow::Input lr,
                                       ::tensorflow::Input rho,
                                       ::tensorflow::Input momentum,
                                       ::tensorflow::Input epsilon,
                                       ::tensorflow::Input grad,
                                       ::tensorflow::Input indices)
  : SparseApplyRMSProp(scope, var, ms, mom, lr, rho, momentum, epsilon, grad, indices, SparseApplyRMSProp::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
