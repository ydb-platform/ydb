// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/math_ops.h"

namespace tensorflow {
namespace ops {

Abs::Abs(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Abs");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Abs")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

AccumulateNV2::AccumulateNV2(const ::tensorflow::Scope& scope,
                             ::tensorflow::InputList inputs, PartialTensorShape
                             shape) {
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOutList(scope, inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AccumulateNV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AccumulateNV2")
                     .Input(_inputs)
                     .Attr("shape", shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->sum = Output(ret, 0);
}

Acos::Acos(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Acos");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Acos")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Acosh::Acosh(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Acosh");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Acosh")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Add::Add(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Add");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Add")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

AddN::AddN(const ::tensorflow::Scope& scope, ::tensorflow::InputList inputs) {
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOutList(scope, inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AddN");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AddN")
                     .Input(_inputs)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->sum = Output(ret, 0);
}

AddV2::AddV2(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
             ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AddV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AddV2")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

All::All(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis, const All::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("All");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "All")
                     .Input(_input)
                     .Input(_axis)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

All::All(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis)
  : All(scope, input, axis, All::Attrs()) {}

Angle::Angle(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
             Angle::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Angle");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Angle")
                     .Input(_input)
                     .Attr("Tout", attrs.Tout_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Angle::Angle(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Angle(scope, input, Angle::Attrs()) {}

Any::Any(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis, const Any::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Any");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Any")
                     .Input(_input)
                     .Input(_axis)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Any::Any(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis)
  : Any(scope, input, axis, Any::Attrs()) {}

ApproximateEqual::ApproximateEqual(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x, ::tensorflow::Input
                                   y, const ApproximateEqual::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ApproximateEqual");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ApproximateEqual")
                     .Input(_x)
                     .Input(_y)
                     .Attr("tolerance", attrs.tolerance_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

ApproximateEqual::ApproximateEqual(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x, ::tensorflow::Input
                                   y)
  : ApproximateEqual(scope, x, y, ApproximateEqual::Attrs()) {}

ArgMax::ArgMax(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input dimension, const ArgMax::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _dimension = ::tensorflow::ops::AsNodeOut(scope, dimension);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ArgMax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ArgMax")
                     .Input(_input)
                     .Input(_dimension)
                     .Attr("output_type", attrs.output_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ArgMax::ArgMax(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input dimension)
  : ArgMax(scope, input, dimension, ArgMax::Attrs()) {}

ArgMin::ArgMin(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input dimension, const ArgMin::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _dimension = ::tensorflow::ops::AsNodeOut(scope, dimension);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ArgMin");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ArgMin")
                     .Input(_input)
                     .Input(_dimension)
                     .Attr("output_type", attrs.output_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ArgMin::ArgMin(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input dimension)
  : ArgMin(scope, input, dimension, ArgMin::Attrs()) {}

Asin::Asin(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Asin");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Asin")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Asinh::Asinh(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Asinh");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Asinh")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Atan::Atan(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Atan");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Atan")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Atan2::Atan2(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
             ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Atan2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Atan2")
                     .Input(_y)
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Atanh::Atanh(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Atanh");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Atanh")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

BatchMatMul::BatchMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         x, ::tensorflow::Input y, const BatchMatMul::Attrs&
                         attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BatchMatMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BatchMatMul")
                     .Input(_x)
                     .Input(_y)
                     .Attr("adj_x", attrs.adj_x_)
                     .Attr("adj_y", attrs.adj_y_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

BatchMatMul::BatchMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         x, ::tensorflow::Input y)
  : BatchMatMul(scope, x, y, BatchMatMul::Attrs()) {}

BesselI0e::BesselI0e(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BesselI0e");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BesselI0e")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

BesselI1e::BesselI1e(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BesselI1e");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BesselI1e")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Betainc::Betainc(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
                 ::tensorflow::Input b, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _b = ::tensorflow::ops::AsNodeOut(scope, b);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Betainc");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Betainc")
                     .Input(_a)
                     .Input(_b)
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Bincount::Bincount(const ::tensorflow::Scope& scope, ::tensorflow::Input arr,
                   ::tensorflow::Input size, ::tensorflow::Input weights) {
  if (!scope.ok()) return;
  auto _arr = ::tensorflow::ops::AsNodeOut(scope, arr);
  if (!scope.ok()) return;
  auto _size = ::tensorflow::ops::AsNodeOut(scope, size);
  if (!scope.ok()) return;
  auto _weights = ::tensorflow::ops::AsNodeOut(scope, weights);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Bincount");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Bincount")
                     .Input(_arr)
                     .Input(_size)
                     .Input(_weights)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->bins = Output(ret, 0);
}

Bucketize::Bucketize(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, const gtl::ArraySlice<float>& boundaries) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Bucketize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Bucketize")
                     .Input(_input)
                     .Attr("boundaries", boundaries)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Cast::Cast(const ::tensorflow::Scope& scope, ::tensorflow::Input x, DataType
           DstT) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Cast");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Cast")
                     .Input(_x)
                     .Attr("DstT", DstT)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Ceil::Ceil(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Ceil");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Ceil")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

ClipByValue::ClipByValue(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         t, ::tensorflow::Input clip_value_min,
                         ::tensorflow::Input clip_value_max) {
  if (!scope.ok()) return;
  auto _t = ::tensorflow::ops::AsNodeOut(scope, t);
  if (!scope.ok()) return;
  auto _clip_value_min = ::tensorflow::ops::AsNodeOut(scope, clip_value_min);
  if (!scope.ok()) return;
  auto _clip_value_max = ::tensorflow::ops::AsNodeOut(scope, clip_value_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ClipByValue");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ClipByValue")
                     .Input(_t)
                     .Input(_clip_value_min)
                     .Input(_clip_value_max)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

CompareAndBitpack::CompareAndBitpack(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input input,
                                     ::tensorflow::Input threshold) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _threshold = ::tensorflow::ops::AsNodeOut(scope, threshold);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("CompareAndBitpack");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "CompareAndBitpack")
                     .Input(_input)
                     .Input(_threshold)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Complex::Complex(const ::tensorflow::Scope& scope, ::tensorflow::Input real,
                 ::tensorflow::Input imag, const Complex::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _real = ::tensorflow::ops::AsNodeOut(scope, real);
  if (!scope.ok()) return;
  auto _imag = ::tensorflow::ops::AsNodeOut(scope, imag);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Complex");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Complex")
                     .Input(_real)
                     .Input(_imag)
                     .Attr("Tout", attrs.Tout_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

Complex::Complex(const ::tensorflow::Scope& scope, ::tensorflow::Input real,
                 ::tensorflow::Input imag)
  : Complex(scope, real, imag, Complex::Attrs()) {}

ComplexAbs::ComplexAbs(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                       const ComplexAbs::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ComplexAbs");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ComplexAbs")
                     .Input(_x)
                     .Attr("Tout", attrs.Tout_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

ComplexAbs::ComplexAbs(const ::tensorflow::Scope& scope, ::tensorflow::Input x)
  : ComplexAbs(scope, x, ComplexAbs::Attrs()) {}

Conj::Conj(const ::tensorflow::Scope& scope, ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Conj");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Conj")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Cos::Cos(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Cos");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Cos")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Cosh::Cosh(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Cosh");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Cosh")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Cross::Cross(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
             ::tensorflow::Input b) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _b = ::tensorflow::ops::AsNodeOut(scope, b);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Cross");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Cross")
                     .Input(_a)
                     .Input(_b)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->product = Output(ret, 0);
}

Cumprod::Cumprod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input axis, const Cumprod::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Cumprod");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Cumprod")
                     .Input(_x)
                     .Input(_axis)
                     .Attr("exclusive", attrs.exclusive_)
                     .Attr("reverse", attrs.reverse_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

Cumprod::Cumprod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input axis)
  : Cumprod(scope, x, axis, Cumprod::Attrs()) {}

Cumsum::Cumsum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
               ::tensorflow::Input axis, const Cumsum::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Cumsum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Cumsum")
                     .Input(_x)
                     .Input(_axis)
                     .Attr("exclusive", attrs.exclusive_)
                     .Attr("reverse", attrs.reverse_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

Cumsum::Cumsum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
               ::tensorflow::Input axis)
  : Cumsum(scope, x, axis, Cumsum::Attrs()) {}

Digamma::Digamma(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Digamma");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Digamma")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Div::Div(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Div");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Div")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Equal::Equal(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
             ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Equal");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Equal")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Erf::Erf(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Erf");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Erf")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Erfc::Erfc(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Erfc");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Erfc")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Exp::Exp(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Exp");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Exp")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Expm1::Expm1(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Expm1");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Expm1")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Floor::Floor(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Floor");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Floor")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

FloorDiv::FloorDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                   ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FloorDiv");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FloorDiv")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

FloorMod::FloorMod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                   ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FloorMod");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FloorMod")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Greater::Greater(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Greater");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Greater")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

GreaterEqual::GreaterEqual(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input x, ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("GreaterEqual");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "GreaterEqual")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

HistogramFixedWidth::HistogramFixedWidth(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input values,
                                         ::tensorflow::Input value_range,
                                         ::tensorflow::Input nbins, const
                                         HistogramFixedWidth::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  auto _value_range = ::tensorflow::ops::AsNodeOut(scope, value_range);
  if (!scope.ok()) return;
  auto _nbins = ::tensorflow::ops::AsNodeOut(scope, nbins);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("HistogramFixedWidth");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "HistogramFixedWidth")
                     .Input(_values)
                     .Input(_value_range)
                     .Input(_nbins)
                     .Attr("dtype", attrs.dtype_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->out = Output(ret, 0);
}

HistogramFixedWidth::HistogramFixedWidth(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input values,
                                         ::tensorflow::Input value_range,
                                         ::tensorflow::Input nbins)
  : HistogramFixedWidth(scope, values, value_range, nbins, HistogramFixedWidth::Attrs()) {}

Igamma::Igamma(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
               ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Igamma");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Igamma")
                     .Input(_a)
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Igammac::Igammac(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
                 ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Igammac");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Igammac")
                     .Input(_a)
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Imag::Imag(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
           Imag::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Imag");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Imag")
                     .Input(_input)
                     .Attr("Tout", attrs.Tout_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Imag::Imag(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Imag(scope, input, Imag::Attrs()) {}

Inv::Inv(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Inv");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Inv")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

IsFinite::IsFinite(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("IsFinite");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "IsFinite")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

IsInf::IsInf(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("IsInf");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "IsInf")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

IsNan::IsNan(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("IsNan");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "IsNan")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Less::Less(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
           ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Less");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Less")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

LessEqual::LessEqual(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                     ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LessEqual");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LessEqual")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Lgamma::Lgamma(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Lgamma");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Lgamma")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

LinSpace::LinSpace(const ::tensorflow::Scope& scope, ::tensorflow::Input start,
                   ::tensorflow::Input stop, ::tensorflow::Input num) {
  if (!scope.ok()) return;
  auto _start = ::tensorflow::ops::AsNodeOut(scope, start);
  if (!scope.ok()) return;
  auto _stop = ::tensorflow::ops::AsNodeOut(scope, stop);
  if (!scope.ok()) return;
  auto _num = ::tensorflow::ops::AsNodeOut(scope, num);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LinSpace");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LinSpace")
                     .Input(_start)
                     .Input(_stop)
                     .Input(_num)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Log::Log(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Log");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Log")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Log1p::Log1p(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Log1p");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Log1p")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

LogicalAnd::LogicalAnd(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                       ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LogicalAnd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LogicalAnd")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

LogicalNot::LogicalNot(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LogicalNot");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LogicalNot")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

LogicalOr::LogicalOr(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                     ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LogicalOr");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LogicalOr")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

MatMul::MatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
               ::tensorflow::Input b, const MatMul::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _b = ::tensorflow::ops::AsNodeOut(scope, b);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatMul")
                     .Input(_a)
                     .Input(_b)
                     .Attr("transpose_a", attrs.transpose_a_)
                     .Attr("transpose_b", attrs.transpose_b_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->product = Output(ret, 0);
}

MatMul::MatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
               ::tensorflow::Input b)
  : MatMul(scope, a, b, MatMul::Attrs()) {}

Max::Max(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis, const Max::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Max");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Max")
                     .Input(_input)
                     .Input(_axis)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Max::Max(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis)
  : Max(scope, input, axis, Max::Attrs()) {}

Maximum::Maximum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Maximum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Maximum")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Mean::Mean(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input axis, const Mean::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Mean");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Mean")
                     .Input(_input)
                     .Input(_axis)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Mean::Mean(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input axis)
  : Mean(scope, input, axis, Mean::Attrs()) {}

Min::Min(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis, const Min::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Min");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Min")
                     .Input(_input)
                     .Input(_axis)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Min::Min(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis)
  : Min(scope, input, axis, Min::Attrs()) {}

Minimum::Minimum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Minimum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Minimum")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Mod::Mod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Mod");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Mod")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Multiply::Multiply(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                   ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Multiply");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Mul")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Negate::Negate(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Negate");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Neg")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

NotEqual::NotEqual(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                   ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("NotEqual");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "NotEqual")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Polygamma::Polygamma(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
                     ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Polygamma");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Polygamma")
                     .Input(_a)
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Pow::Pow(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Pow");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Pow")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Prod::Prod(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input axis, const Prod::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Prod");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Prod")
                     .Input(_input)
                     .Input(_axis)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Prod::Prod(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input axis)
  : Prod(scope, input, axis, Prod::Attrs()) {}

QuantizeDownAndShrinkRange::QuantizeDownAndShrinkRange(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input
                                                       input,
                                                       ::tensorflow::Input
                                                       input_min,
                                                       ::tensorflow::Input
                                                       input_max, DataType
                                                       out_type) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _input_min = ::tensorflow::ops::AsNodeOut(scope, input_min);
  if (!scope.ok()) return;
  auto _input_max = ::tensorflow::ops::AsNodeOut(scope, input_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizeDownAndShrinkRange");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizeDownAndShrinkRange")
                     .Input(_input)
                     .Input(_input_min)
                     .Input(_input_max)
                     .Attr("out_type", out_type)
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
  this->output_min = Output(ret, _outputs_range["output_min"].first);
  this->output_max = Output(ret, _outputs_range["output_max"].first);
}

QuantizedAdd::QuantizedAdd(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input x, ::tensorflow::Input y,
                           ::tensorflow::Input min_x, ::tensorflow::Input
                           max_x, ::tensorflow::Input min_y,
                           ::tensorflow::Input max_y, const
                           QuantizedAdd::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  auto _min_x = ::tensorflow::ops::AsNodeOut(scope, min_x);
  if (!scope.ok()) return;
  auto _max_x = ::tensorflow::ops::AsNodeOut(scope, max_x);
  if (!scope.ok()) return;
  auto _min_y = ::tensorflow::ops::AsNodeOut(scope, min_y);
  if (!scope.ok()) return;
  auto _max_y = ::tensorflow::ops::AsNodeOut(scope, max_y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedAdd")
                     .Input(_x)
                     .Input(_y)
                     .Input(_min_x)
                     .Input(_max_x)
                     .Input(_min_y)
                     .Input(_max_y)
                     .Attr("Toutput", attrs.Toutput_)
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

  this->z = Output(ret, _outputs_range["z"].first);
  this->min_z = Output(ret, _outputs_range["min_z"].first);
  this->max_z = Output(ret, _outputs_range["max_z"].first);
}

QuantizedAdd::QuantizedAdd(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input x, ::tensorflow::Input y,
                           ::tensorflow::Input min_x, ::tensorflow::Input
                           max_x, ::tensorflow::Input min_y,
                           ::tensorflow::Input max_y)
  : QuantizedAdd(scope, x, y, min_x, max_x, min_y, max_y, QuantizedAdd::Attrs()) {}

QuantizedMatMul::QuantizedMatMul(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input a, ::tensorflow::Input b,
                                 ::tensorflow::Input min_a, ::tensorflow::Input
                                 max_a, ::tensorflow::Input min_b,
                                 ::tensorflow::Input max_b, const
                                 QuantizedMatMul::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _b = ::tensorflow::ops::AsNodeOut(scope, b);
  if (!scope.ok()) return;
  auto _min_a = ::tensorflow::ops::AsNodeOut(scope, min_a);
  if (!scope.ok()) return;
  auto _max_a = ::tensorflow::ops::AsNodeOut(scope, max_a);
  if (!scope.ok()) return;
  auto _min_b = ::tensorflow::ops::AsNodeOut(scope, min_b);
  if (!scope.ok()) return;
  auto _max_b = ::tensorflow::ops::AsNodeOut(scope, max_b);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedMatMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedMatMul")
                     .Input(_a)
                     .Input(_b)
                     .Input(_min_a)
                     .Input(_max_a)
                     .Input(_min_b)
                     .Input(_max_b)
                     .Attr("Toutput", attrs.Toutput_)
                     .Attr("transpose_a", attrs.transpose_a_)
                     .Attr("transpose_b", attrs.transpose_b_)
                     .Attr("Tactivation", attrs.Tactivation_)
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

  this->out = Output(ret, _outputs_range["out"].first);
  this->min_out = Output(ret, _outputs_range["min_out"].first);
  this->max_out = Output(ret, _outputs_range["max_out"].first);
}

QuantizedMatMul::QuantizedMatMul(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input a, ::tensorflow::Input b,
                                 ::tensorflow::Input min_a, ::tensorflow::Input
                                 max_a, ::tensorflow::Input min_b,
                                 ::tensorflow::Input max_b)
  : QuantizedMatMul(scope, a, b, min_a, max_a, min_b, max_b, QuantizedMatMul::Attrs()) {}

QuantizedMul::QuantizedMul(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input x, ::tensorflow::Input y,
                           ::tensorflow::Input min_x, ::tensorflow::Input
                           max_x, ::tensorflow::Input min_y,
                           ::tensorflow::Input max_y, const
                           QuantizedMul::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  auto _min_x = ::tensorflow::ops::AsNodeOut(scope, min_x);
  if (!scope.ok()) return;
  auto _max_x = ::tensorflow::ops::AsNodeOut(scope, max_x);
  if (!scope.ok()) return;
  auto _min_y = ::tensorflow::ops::AsNodeOut(scope, min_y);
  if (!scope.ok()) return;
  auto _max_y = ::tensorflow::ops::AsNodeOut(scope, max_y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedMul")
                     .Input(_x)
                     .Input(_y)
                     .Input(_min_x)
                     .Input(_max_x)
                     .Input(_min_y)
                     .Input(_max_y)
                     .Attr("Toutput", attrs.Toutput_)
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

  this->z = Output(ret, _outputs_range["z"].first);
  this->min_z = Output(ret, _outputs_range["min_z"].first);
  this->max_z = Output(ret, _outputs_range["max_z"].first);
}

QuantizedMul::QuantizedMul(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input x, ::tensorflow::Input y,
                           ::tensorflow::Input min_x, ::tensorflow::Input
                           max_x, ::tensorflow::Input min_y,
                           ::tensorflow::Input max_y)
  : QuantizedMul(scope, x, y, min_x, max_x, min_y, max_y, QuantizedMul::Attrs()) {}

Range::Range(const ::tensorflow::Scope& scope, ::tensorflow::Input start,
             ::tensorflow::Input limit, ::tensorflow::Input delta) {
  if (!scope.ok()) return;
  auto _start = ::tensorflow::ops::AsNodeOut(scope, start);
  if (!scope.ok()) return;
  auto _limit = ::tensorflow::ops::AsNodeOut(scope, limit);
  if (!scope.ok()) return;
  auto _delta = ::tensorflow::ops::AsNodeOut(scope, delta);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Range");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Range")
                     .Input(_start)
                     .Input(_limit)
                     .Input(_delta)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Real::Real(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
           Real::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Real");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Real")
                     .Input(_input)
                     .Attr("Tout", attrs.Tout_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Real::Real(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Real(scope, input, Real::Attrs()) {}

RealDiv::RealDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RealDiv");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RealDiv")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Reciprocal::Reciprocal(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Reciprocal");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Reciprocal")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

RequantizationRange::RequantizationRange(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input input,
                                         ::tensorflow::Input input_min,
                                         ::tensorflow::Input input_max) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _input_min = ::tensorflow::ops::AsNodeOut(scope, input_min);
  if (!scope.ok()) return;
  auto _input_max = ::tensorflow::ops::AsNodeOut(scope, input_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RequantizationRange");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RequantizationRange")
                     .Input(_input)
                     .Input(_input_min)
                     .Input(_input_max)
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

  this->output_min = Output(ret, _outputs_range["output_min"].first);
  this->output_max = Output(ret, _outputs_range["output_max"].first);
}

Requantize::Requantize(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input input_min,
                       ::tensorflow::Input input_max, ::tensorflow::Input
                       requested_output_min, ::tensorflow::Input
                       requested_output_max, DataType out_type) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _input_min = ::tensorflow::ops::AsNodeOut(scope, input_min);
  if (!scope.ok()) return;
  auto _input_max = ::tensorflow::ops::AsNodeOut(scope, input_max);
  if (!scope.ok()) return;
  auto _requested_output_min = ::tensorflow::ops::AsNodeOut(scope, requested_output_min);
  if (!scope.ok()) return;
  auto _requested_output_max = ::tensorflow::ops::AsNodeOut(scope, requested_output_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Requantize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Requantize")
                     .Input(_input)
                     .Input(_input_min)
                     .Input(_input_max)
                     .Input(_requested_output_min)
                     .Input(_requested_output_max)
                     .Attr("out_type", out_type)
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
  this->output_min = Output(ret, _outputs_range["output_min"].first);
  this->output_max = Output(ret, _outputs_range["output_max"].first);
}

Rint::Rint(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Rint");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Rint")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Round::Round(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Round");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Round")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Rsqrt::Rsqrt(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Rsqrt");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Rsqrt")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

SegmentMax::SegmentMax(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       data, ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SegmentMax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SegmentMax")
                     .Input(_data)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SegmentMean::SegmentMean(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         data, ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SegmentMean");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SegmentMean")
                     .Input(_data)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SegmentMin::SegmentMin(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       data, ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SegmentMin");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SegmentMin")
                     .Input(_data)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SegmentProd::SegmentProd(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         data, ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SegmentProd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SegmentProd")
                     .Input(_data)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SegmentSum::SegmentSum(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       data, ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SegmentSum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SegmentSum")
                     .Input(_data)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Where3::Where3(const ::tensorflow::Scope& scope, ::tensorflow::Input condition,
               ::tensorflow::Input x, ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _condition = ::tensorflow::ops::AsNodeOut(scope, condition);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Where3");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Select")
                     .Input(_condition)
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Sigmoid::Sigmoid(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Sigmoid");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Sigmoid")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Sign::Sign(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Sign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Sign")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Sin::Sin(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Sin");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Sin")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Sinh::Sinh(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Sinh");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Sinh")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

SparseMatMul::SparseMatMul(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input a, ::tensorflow::Input b, const
                           SparseMatMul::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _a = ::tensorflow::ops::AsNodeOut(scope, a);
  if (!scope.ok()) return;
  auto _b = ::tensorflow::ops::AsNodeOut(scope, b);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseMatMul");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseMatMul")
                     .Input(_a)
                     .Input(_b)
                     .Attr("transpose_a", attrs.transpose_a_)
                     .Attr("transpose_b", attrs.transpose_b_)
                     .Attr("a_is_sparse", attrs.a_is_sparse_)
                     .Attr("b_is_sparse", attrs.b_is_sparse_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->product = Output(ret, 0);
}

SparseMatMul::SparseMatMul(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input a, ::tensorflow::Input b)
  : SparseMatMul(scope, a, b, SparseMatMul::Attrs()) {}

SparseSegmentMean::SparseSegmentMean(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input data,
                                     ::tensorflow::Input indices,
                                     ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentMean");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentMean")
                     .Input(_data)
                     .Input(_indices)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSegmentMeanGrad::SparseSegmentMeanGrad(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input grad,
                                             ::tensorflow::Input indices,
                                             ::tensorflow::Input segment_ids,
                                             ::tensorflow::Input output_dim0) {
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _output_dim0 = ::tensorflow::ops::AsNodeOut(scope, output_dim0);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentMeanGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentMeanGrad")
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_segment_ids)
                     .Input(_output_dim0)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSegmentMeanWithNumSegments::SparseSegmentMeanWithNumSegments(const
                                                                   ::tensorflow::Scope&
                                                                   scope,
                                                                   ::tensorflow::Input
                                                                   data,
                                                                   ::tensorflow::Input
                                                                   indices,
                                                                   ::tensorflow::Input
                                                                   segment_ids,
                                                                   ::tensorflow::Input
                                                                   num_segments) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _num_segments = ::tensorflow::ops::AsNodeOut(scope, num_segments);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentMeanWithNumSegments");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentMeanWithNumSegments")
                     .Input(_data)
                     .Input(_indices)
                     .Input(_segment_ids)
                     .Input(_num_segments)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSegmentSqrtN::SparseSegmentSqrtN(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input data,
                                       ::tensorflow::Input indices,
                                       ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentSqrtN");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentSqrtN")
                     .Input(_data)
                     .Input(_indices)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSegmentSqrtNGrad::SparseSegmentSqrtNGrad(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input grad,
                                               ::tensorflow::Input indices,
                                               ::tensorflow::Input segment_ids,
                                               ::tensorflow::Input output_dim0) {
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _output_dim0 = ::tensorflow::ops::AsNodeOut(scope, output_dim0);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentSqrtNGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentSqrtNGrad")
                     .Input(_grad)
                     .Input(_indices)
                     .Input(_segment_ids)
                     .Input(_output_dim0)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSegmentSqrtNWithNumSegments::SparseSegmentSqrtNWithNumSegments(const
                                                                     ::tensorflow::Scope&
                                                                     scope,
                                                                     ::tensorflow::Input
                                                                     data,
                                                                     ::tensorflow::Input
                                                                     indices,
                                                                     ::tensorflow::Input
                                                                     segment_ids,
                                                                     ::tensorflow::Input
                                                                     num_segments) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _num_segments = ::tensorflow::ops::AsNodeOut(scope, num_segments);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentSqrtNWithNumSegments");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentSqrtNWithNumSegments")
                     .Input(_data)
                     .Input(_indices)
                     .Input(_segment_ids)
                     .Input(_num_segments)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSegmentSum::SparseSegmentSum(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input data,
                                   ::tensorflow::Input indices,
                                   ::tensorflow::Input segment_ids) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentSum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentSum")
                     .Input(_data)
                     .Input(_indices)
                     .Input(_segment_ids)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SparseSegmentSumWithNumSegments::SparseSegmentSumWithNumSegments(const
                                                                 ::tensorflow::Scope&
                                                                 scope,
                                                                 ::tensorflow::Input
                                                                 data,
                                                                 ::tensorflow::Input
                                                                 indices,
                                                                 ::tensorflow::Input
                                                                 segment_ids,
                                                                 ::tensorflow::Input
                                                                 num_segments) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _num_segments = ::tensorflow::ops::AsNodeOut(scope, num_segments);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSegmentSumWithNumSegments");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSegmentSumWithNumSegments")
                     .Input(_data)
                     .Input(_indices)
                     .Input(_segment_ids)
                     .Input(_num_segments)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Sqrt::Sqrt(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Sqrt");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Sqrt")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Square::Square(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Square");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Square")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

SquaredDifference::SquaredDifference(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input x, ::tensorflow::Input
                                     y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SquaredDifference");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SquaredDifference")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Subtract::Subtract(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                   ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Subtract");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Sub")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

Sum::Sum(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis, const Sum::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Sum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Sum")
                     .Input(_input)
                     .Input(_axis)
                     .Attr("keep_dims", attrs.keep_dims_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Sum::Sum(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input axis)
  : Sum(scope, input, axis, Sum::Attrs()) {}

Tan::Tan(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Tan");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Tan")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Tanh::Tanh(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Tanh");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Tanh")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

TruncateDiv::TruncateDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         x, ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TruncateDiv");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TruncateDiv")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

TruncateMod::TruncateMod(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         x, ::tensorflow::Input y) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TruncateMod");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TruncateMod")
                     .Input(_x)
                     .Input(_y)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

UnsortedSegmentMax::UnsortedSegmentMax(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input data,
                                       ::tensorflow::Input segment_ids,
                                       ::tensorflow::Input num_segments) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _num_segments = ::tensorflow::ops::AsNodeOut(scope, num_segments);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UnsortedSegmentMax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UnsortedSegmentMax")
                     .Input(_data)
                     .Input(_segment_ids)
                     .Input(_num_segments)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

UnsortedSegmentMin::UnsortedSegmentMin(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input data,
                                       ::tensorflow::Input segment_ids,
                                       ::tensorflow::Input num_segments) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _num_segments = ::tensorflow::ops::AsNodeOut(scope, num_segments);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UnsortedSegmentMin");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UnsortedSegmentMin")
                     .Input(_data)
                     .Input(_segment_ids)
                     .Input(_num_segments)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

UnsortedSegmentProd::UnsortedSegmentProd(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input data,
                                         ::tensorflow::Input segment_ids,
                                         ::tensorflow::Input num_segments) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _num_segments = ::tensorflow::ops::AsNodeOut(scope, num_segments);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UnsortedSegmentProd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UnsortedSegmentProd")
                     .Input(_data)
                     .Input(_segment_ids)
                     .Input(_num_segments)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

UnsortedSegmentSum::UnsortedSegmentSum(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input data,
                                       ::tensorflow::Input segment_ids,
                                       ::tensorflow::Input num_segments) {
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOut(scope, data);
  if (!scope.ok()) return;
  auto _segment_ids = ::tensorflow::ops::AsNodeOut(scope, segment_ids);
  if (!scope.ok()) return;
  auto _num_segments = ::tensorflow::ops::AsNodeOut(scope, num_segments);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UnsortedSegmentSum");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UnsortedSegmentSum")
                     .Input(_data)
                     .Input(_segment_ids)
                     .Input(_num_segments)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Zeta::Zeta(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
           ::tensorflow::Input q) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _q = ::tensorflow::ops::AsNodeOut(scope, q);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Zeta");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Zeta")
                     .Input(_x)
                     .Input(_q)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->z = Output(ret, 0);
}

/// @}

}  // namespace ops
}  // namespace tensorflow
