// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/random_ops.h"

namespace tensorflow {
namespace ops {

Multinomial::Multinomial(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         logits, ::tensorflow::Input num_samples, const
                         Multinomial::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _logits = ::tensorflow::ops::AsNodeOut(scope, logits);
  if (!scope.ok()) return;
  auto _num_samples = ::tensorflow::ops::AsNodeOut(scope, num_samples);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Multinomial");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Multinomial")
                     .Input(_logits)
                     .Input(_num_samples)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
                     .Attr("output_dtype", attrs.output_dtype_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Multinomial::Multinomial(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         logits, ::tensorflow::Input num_samples)
  : Multinomial(scope, logits, num_samples, Multinomial::Attrs()) {}

ParameterizedTruncatedNormal::ParameterizedTruncatedNormal(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           shape,
                                                           ::tensorflow::Input
                                                           means,
                                                           ::tensorflow::Input
                                                           stdevs,
                                                           ::tensorflow::Input
                                                           minvals,
                                                           ::tensorflow::Input
                                                           maxvals, const
                                                           ParameterizedTruncatedNormal::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  auto _means = ::tensorflow::ops::AsNodeOut(scope, means);
  if (!scope.ok()) return;
  auto _stdevs = ::tensorflow::ops::AsNodeOut(scope, stdevs);
  if (!scope.ok()) return;
  auto _minvals = ::tensorflow::ops::AsNodeOut(scope, minvals);
  if (!scope.ok()) return;
  auto _maxvals = ::tensorflow::ops::AsNodeOut(scope, maxvals);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ParameterizedTruncatedNormal");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ParameterizedTruncatedNormal")
                     .Input(_shape)
                     .Input(_means)
                     .Input(_stdevs)
                     .Input(_minvals)
                     .Input(_maxvals)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ParameterizedTruncatedNormal::ParameterizedTruncatedNormal(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           shape,
                                                           ::tensorflow::Input
                                                           means,
                                                           ::tensorflow::Input
                                                           stdevs,
                                                           ::tensorflow::Input
                                                           minvals,
                                                           ::tensorflow::Input
                                                           maxvals)
  : ParameterizedTruncatedNormal(scope, shape, means, stdevs, minvals, maxvals, ParameterizedTruncatedNormal::Attrs()) {}

RandomGamma::RandomGamma(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         shape, ::tensorflow::Input alpha, const
                         RandomGamma::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  auto _alpha = ::tensorflow::ops::AsNodeOut(scope, alpha);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomGamma");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomGamma")
                     .Input(_shape)
                     .Input(_alpha)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RandomGamma::RandomGamma(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         shape, ::tensorflow::Input alpha)
  : RandomGamma(scope, shape, alpha, RandomGamma::Attrs()) {}

RandomPoissonV2::RandomPoissonV2(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input shape, ::tensorflow::Input
                                 rate, const RandomPoissonV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  auto _rate = ::tensorflow::ops::AsNodeOut(scope, rate);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomPoissonV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomPoissonV2")
                     .Input(_shape)
                     .Input(_rate)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
                     .Attr("dtype", attrs.dtype_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RandomPoissonV2::RandomPoissonV2(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input shape, ::tensorflow::Input
                                 rate)
  : RandomPoissonV2(scope, shape, rate, RandomPoissonV2::Attrs()) {}

RandomShuffle::RandomShuffle(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input value, const
                             RandomShuffle::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomShuffle");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomShuffle")
                     .Input(_value)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RandomShuffle::RandomShuffle(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input value)
  : RandomShuffle(scope, value, RandomShuffle::Attrs()) {}

RandomNormal::RandomNormal(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input shape, DataType dtype, const
                           RandomNormal::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomNormal");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomStandardNormal")
                     .Input(_shape)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RandomNormal::RandomNormal(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input shape, DataType dtype)
  : RandomNormal(scope, shape, dtype, RandomNormal::Attrs()) {}

RandomUniform::RandomUniform(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input shape, DataType dtype, const
                             RandomUniform::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomUniform");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomUniform")
                     .Input(_shape)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RandomUniform::RandomUniform(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input shape, DataType dtype)
  : RandomUniform(scope, shape, dtype, RandomUniform::Attrs()) {}

RandomUniformInt::RandomUniformInt(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input shape,
                                   ::tensorflow::Input minval,
                                   ::tensorflow::Input maxval, const
                                   RandomUniformInt::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  auto _minval = ::tensorflow::ops::AsNodeOut(scope, minval);
  if (!scope.ok()) return;
  auto _maxval = ::tensorflow::ops::AsNodeOut(scope, maxval);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RandomUniformInt");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RandomUniformInt")
                     .Input(_shape)
                     .Input(_minval)
                     .Input(_maxval)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

RandomUniformInt::RandomUniformInt(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input shape,
                                   ::tensorflow::Input minval,
                                   ::tensorflow::Input maxval)
  : RandomUniformInt(scope, shape, minval, maxval, RandomUniformInt::Attrs()) {}

TruncatedNormal::TruncatedNormal(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input shape, DataType dtype,
                                 const TruncatedNormal::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TruncatedNormal");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TruncatedNormal")
                     .Input(_shape)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
                     .Attr("dtype", dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

TruncatedNormal::TruncatedNormal(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input shape, DataType dtype)
  : TruncatedNormal(scope, shape, dtype, TruncatedNormal::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
