// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/array_ops.h"

namespace tensorflow {
namespace ops {

BatchToSpace::BatchToSpace(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, ::tensorflow::Input
                           crops, int64 block_size) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _crops = ::tensorflow::ops::AsNodeOut(scope, crops);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BatchToSpace");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BatchToSpace")
                     .Input(_input)
                     .Input(_crops)
                     .Attr("block_size", block_size)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

BatchToSpaceND::BatchToSpaceND(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input, ::tensorflow::Input
                               block_shape, ::tensorflow::Input crops) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _block_shape = ::tensorflow::ops::AsNodeOut(scope, block_shape);
  if (!scope.ok()) return;
  auto _crops = ::tensorflow::ops::AsNodeOut(scope, crops);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BatchToSpaceND");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BatchToSpaceND")
                     .Input(_input)
                     .Input(_block_shape)
                     .Input(_crops)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Bitcast::Bitcast(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
                 DataType type) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Bitcast");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Bitcast")
                     .Input(_input)
                     .Attr("type", type)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

BroadcastDynamicShape::BroadcastDynamicShape(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input s0,
                                             ::tensorflow::Input s1) {
  if (!scope.ok()) return;
  auto _s0 = ::tensorflow::ops::AsNodeOut(scope, s0);
  if (!scope.ok()) return;
  auto _s1 = ::tensorflow::ops::AsNodeOut(scope, s1);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BroadcastDynamicShape");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BroadcastArgs")
                     .Input(_s0)
                     .Input(_s1)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->r0 = Output(ret, 0);
}

BroadcastTo::BroadcastTo(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         input, ::tensorflow::Input shape) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BroadcastTo");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BroadcastTo")
                     .Input(_input)
                     .Input(_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

CheckNumerics::CheckNumerics(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input tensor, StringPiece message) {
  if (!scope.ok()) return;
  auto _tensor = ::tensorflow::ops::AsNodeOut(scope, tensor);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("CheckNumerics");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "CheckNumerics")
                     .Input(_tensor)
                     .Attr("message", message)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Concat::Concat(const ::tensorflow::Scope& scope, ::tensorflow::InputList
               values, ::tensorflow::Input axis) {
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Concat");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ConcatV2")
                     .Input(_values)
                     .Input(_axis)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ConjugateTranspose::ConjugateTranspose(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input x,
                                       ::tensorflow::Input perm) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _perm = ::tensorflow::ops::AsNodeOut(scope, perm);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ConjugateTranspose");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ConjugateTranspose")
                     .Input(_x)
                     .Input(_perm)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

DebugGradientIdentity::DebugGradientIdentity(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DebugGradientIdentity");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DebugGradientIdentity")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DebugGradientRefIdentity::DebugGradientRefIdentity(const ::tensorflow::Scope&
                                                   scope, ::tensorflow::Input
                                                   input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DebugGradientRefIdentity");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DebugGradientRefIdentity")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DeepCopy::DeepCopy(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DeepCopy");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DeepCopy")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

DepthToSpace::DepthToSpace(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, int64 block_size, const
                           DepthToSpace::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DepthToSpace");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DepthToSpace")
                     .Input(_input)
                     .Attr("block_size", block_size)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DepthToSpace::DepthToSpace(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, int64 block_size)
  : DepthToSpace(scope, input, block_size, DepthToSpace::Attrs()) {}

Dequantize::Dequantize(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input min_range,
                       ::tensorflow::Input max_range, const Dequantize::Attrs&
                       attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _min_range = ::tensorflow::ops::AsNodeOut(scope, min_range);
  if (!scope.ok()) return;
  auto _max_range = ::tensorflow::ops::AsNodeOut(scope, max_range);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Dequantize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Dequantize")
                     .Input(_input)
                     .Input(_min_range)
                     .Input(_max_range)
                     .Attr("mode", attrs.mode_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Dequantize::Dequantize(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input min_range,
                       ::tensorflow::Input max_range)
  : Dequantize(scope, input, min_range, max_range, Dequantize::Attrs()) {}

Diag::Diag(const ::tensorflow::Scope& scope, ::tensorflow::Input diagonal) {
  if (!scope.ok()) return;
  auto _diagonal = ::tensorflow::ops::AsNodeOut(scope, diagonal);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Diag");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Diag")
                     .Input(_diagonal)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DiagPart::DiagPart(const ::tensorflow::Scope& scope, ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DiagPart");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DiagPart")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->diagonal = Output(ret, 0);
}

EditDistance::EditDistance(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input hypothesis_indices,
                           ::tensorflow::Input hypothesis_values,
                           ::tensorflow::Input hypothesis_shape,
                           ::tensorflow::Input truth_indices,
                           ::tensorflow::Input truth_values,
                           ::tensorflow::Input truth_shape, const
                           EditDistance::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _hypothesis_indices = ::tensorflow::ops::AsNodeOut(scope, hypothesis_indices);
  if (!scope.ok()) return;
  auto _hypothesis_values = ::tensorflow::ops::AsNodeOut(scope, hypothesis_values);
  if (!scope.ok()) return;
  auto _hypothesis_shape = ::tensorflow::ops::AsNodeOut(scope, hypothesis_shape);
  if (!scope.ok()) return;
  auto _truth_indices = ::tensorflow::ops::AsNodeOut(scope, truth_indices);
  if (!scope.ok()) return;
  auto _truth_values = ::tensorflow::ops::AsNodeOut(scope, truth_values);
  if (!scope.ok()) return;
  auto _truth_shape = ::tensorflow::ops::AsNodeOut(scope, truth_shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("EditDistance");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "EditDistance")
                     .Input(_hypothesis_indices)
                     .Input(_hypothesis_values)
                     .Input(_hypothesis_shape)
                     .Input(_truth_indices)
                     .Input(_truth_values)
                     .Input(_truth_shape)
                     .Attr("normalize", attrs.normalize_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

EditDistance::EditDistance(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input hypothesis_indices,
                           ::tensorflow::Input hypothesis_values,
                           ::tensorflow::Input hypothesis_shape,
                           ::tensorflow::Input truth_indices,
                           ::tensorflow::Input truth_values,
                           ::tensorflow::Input truth_shape)
  : EditDistance(scope, hypothesis_indices, hypothesis_values, hypothesis_shape, truth_indices, truth_values, truth_shape, EditDistance::Attrs()) {}

Empty::Empty(const ::tensorflow::Scope& scope, ::tensorflow::Input shape,
             DataType dtype, const Empty::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Empty");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Empty")
                     .Input(_shape)
                     .Attr("dtype", dtype)
                     .Attr("init", attrs.init_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Empty::Empty(const ::tensorflow::Scope& scope, ::tensorflow::Input shape,
             DataType dtype)
  : Empty(scope, shape, dtype, Empty::Attrs()) {}

ExpandDims::ExpandDims(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input axis) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ExpandDims");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ExpandDims")
                     .Input(_input)
                     .Input(_axis)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ExtractImagePatches::ExtractImagePatches(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input images, const
                                         gtl::ArraySlice<int>& ksizes, const
                                         gtl::ArraySlice<int>& strides, const
                                         gtl::ArraySlice<int>& rates,
                                         StringPiece padding) {
  if (!scope.ok()) return;
  auto _images = ::tensorflow::ops::AsNodeOut(scope, images);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ExtractImagePatches");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ExtractImagePatches")
                     .Input(_images)
                     .Attr("ksizes", ksizes)
                     .Attr("strides", strides)
                     .Attr("rates", rates)
                     .Attr("padding", padding)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->patches = Output(ret, 0);
}

FakeQuantWithMinMaxArgs::FakeQuantWithMinMaxArgs(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 inputs, const
                                                 FakeQuantWithMinMaxArgs::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOut(scope, inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FakeQuantWithMinMaxArgs");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FakeQuantWithMinMaxArgs")
                     .Input(_inputs)
                     .Attr("min", attrs.min_)
                     .Attr("max", attrs.max_)
                     .Attr("num_bits", attrs.num_bits_)
                     .Attr("narrow_range", attrs.narrow_range_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->outputs = Output(ret, 0);
}

FakeQuantWithMinMaxArgs::FakeQuantWithMinMaxArgs(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 inputs)
  : FakeQuantWithMinMaxArgs(scope, inputs, FakeQuantWithMinMaxArgs::Attrs()) {}

FakeQuantWithMinMaxArgsGradient::FakeQuantWithMinMaxArgsGradient(const
                                                                 ::tensorflow::Scope&
                                                                 scope,
                                                                 ::tensorflow::Input
                                                                 gradients,
                                                                 ::tensorflow::Input
                                                                 inputs, const
                                                                 FakeQuantWithMinMaxArgsGradient::Attrs&
                                                                 attrs) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOut(scope, inputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FakeQuantWithMinMaxArgsGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FakeQuantWithMinMaxArgsGradient")
                     .Input(_gradients)
                     .Input(_inputs)
                     .Attr("min", attrs.min_)
                     .Attr("max", attrs.max_)
                     .Attr("num_bits", attrs.num_bits_)
                     .Attr("narrow_range", attrs.narrow_range_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->backprops = Output(ret, 0);
}

FakeQuantWithMinMaxArgsGradient::FakeQuantWithMinMaxArgsGradient(const
                                                                 ::tensorflow::Scope&
                                                                 scope,
                                                                 ::tensorflow::Input
                                                                 gradients,
                                                                 ::tensorflow::Input
                                                                 inputs)
  : FakeQuantWithMinMaxArgsGradient(scope, gradients, inputs, FakeQuantWithMinMaxArgsGradient::Attrs()) {}

FakeQuantWithMinMaxVars::FakeQuantWithMinMaxVars(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 inputs, ::tensorflow::Input
                                                 min, ::tensorflow::Input max,
                                                 const
                                                 FakeQuantWithMinMaxVars::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOut(scope, inputs);
  if (!scope.ok()) return;
  auto _min = ::tensorflow::ops::AsNodeOut(scope, min);
  if (!scope.ok()) return;
  auto _max = ::tensorflow::ops::AsNodeOut(scope, max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FakeQuantWithMinMaxVars");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FakeQuantWithMinMaxVars")
                     .Input(_inputs)
                     .Input(_min)
                     .Input(_max)
                     .Attr("num_bits", attrs.num_bits_)
                     .Attr("narrow_range", attrs.narrow_range_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->outputs = Output(ret, 0);
}

FakeQuantWithMinMaxVars::FakeQuantWithMinMaxVars(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 inputs, ::tensorflow::Input
                                                 min, ::tensorflow::Input max)
  : FakeQuantWithMinMaxVars(scope, inputs, min, max, FakeQuantWithMinMaxVars::Attrs()) {}

FakeQuantWithMinMaxVarsGradient::FakeQuantWithMinMaxVarsGradient(const
                                                                 ::tensorflow::Scope&
                                                                 scope,
                                                                 ::tensorflow::Input
                                                                 gradients,
                                                                 ::tensorflow::Input
                                                                 inputs,
                                                                 ::tensorflow::Input
                                                                 min,
                                                                 ::tensorflow::Input
                                                                 max, const
                                                                 FakeQuantWithMinMaxVarsGradient::Attrs&
                                                                 attrs) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOut(scope, inputs);
  if (!scope.ok()) return;
  auto _min = ::tensorflow::ops::AsNodeOut(scope, min);
  if (!scope.ok()) return;
  auto _max = ::tensorflow::ops::AsNodeOut(scope, max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FakeQuantWithMinMaxVarsGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FakeQuantWithMinMaxVarsGradient")
                     .Input(_gradients)
                     .Input(_inputs)
                     .Input(_min)
                     .Input(_max)
                     .Attr("num_bits", attrs.num_bits_)
                     .Attr("narrow_range", attrs.narrow_range_)
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

  this->backprops_wrt_input = Output(ret, _outputs_range["backprops_wrt_input"].first);
  this->backprop_wrt_min = Output(ret, _outputs_range["backprop_wrt_min"].first);
  this->backprop_wrt_max = Output(ret, _outputs_range["backprop_wrt_max"].first);
}

FakeQuantWithMinMaxVarsGradient::FakeQuantWithMinMaxVarsGradient(const
                                                                 ::tensorflow::Scope&
                                                                 scope,
                                                                 ::tensorflow::Input
                                                                 gradients,
                                                                 ::tensorflow::Input
                                                                 inputs,
                                                                 ::tensorflow::Input
                                                                 min,
                                                                 ::tensorflow::Input
                                                                 max)
  : FakeQuantWithMinMaxVarsGradient(scope, gradients, inputs, min, max, FakeQuantWithMinMaxVarsGradient::Attrs()) {}

FakeQuantWithMinMaxVarsPerChannel::FakeQuantWithMinMaxVarsPerChannel(const
                                                                     ::tensorflow::Scope&
                                                                     scope,
                                                                     ::tensorflow::Input
                                                                     inputs,
                                                                     ::tensorflow::Input
                                                                     min,
                                                                     ::tensorflow::Input
                                                                     max, const
                                                                     FakeQuantWithMinMaxVarsPerChannel::Attrs&
                                                                     attrs) {
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOut(scope, inputs);
  if (!scope.ok()) return;
  auto _min = ::tensorflow::ops::AsNodeOut(scope, min);
  if (!scope.ok()) return;
  auto _max = ::tensorflow::ops::AsNodeOut(scope, max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FakeQuantWithMinMaxVarsPerChannel");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FakeQuantWithMinMaxVarsPerChannel")
                     .Input(_inputs)
                     .Input(_min)
                     .Input(_max)
                     .Attr("num_bits", attrs.num_bits_)
                     .Attr("narrow_range", attrs.narrow_range_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->outputs = Output(ret, 0);
}

FakeQuantWithMinMaxVarsPerChannel::FakeQuantWithMinMaxVarsPerChannel(const
                                                                     ::tensorflow::Scope&
                                                                     scope,
                                                                     ::tensorflow::Input
                                                                     inputs,
                                                                     ::tensorflow::Input
                                                                     min,
                                                                     ::tensorflow::Input
                                                                     max)
  : FakeQuantWithMinMaxVarsPerChannel(scope, inputs, min, max, FakeQuantWithMinMaxVarsPerChannel::Attrs()) {}

FakeQuantWithMinMaxVarsPerChannelGradient::FakeQuantWithMinMaxVarsPerChannelGradient(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients, ::tensorflow::Input inputs, ::tensorflow::Input min, ::tensorflow::Input max, const FakeQuantWithMinMaxVarsPerChannelGradient::Attrs&
                                                                                     attrs) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _inputs = ::tensorflow::ops::AsNodeOut(scope, inputs);
  if (!scope.ok()) return;
  auto _min = ::tensorflow::ops::AsNodeOut(scope, min);
  if (!scope.ok()) return;
  auto _max = ::tensorflow::ops::AsNodeOut(scope, max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FakeQuantWithMinMaxVarsPerChannelGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FakeQuantWithMinMaxVarsPerChannelGradient")
                     .Input(_gradients)
                     .Input(_inputs)
                     .Input(_min)
                     .Input(_max)
                     .Attr("num_bits", attrs.num_bits_)
                     .Attr("narrow_range", attrs.narrow_range_)
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

  this->backprops_wrt_input = Output(ret, _outputs_range["backprops_wrt_input"].first);
  this->backprop_wrt_min = Output(ret, _outputs_range["backprop_wrt_min"].first);
  this->backprop_wrt_max = Output(ret, _outputs_range["backprop_wrt_max"].first);
}

FakeQuantWithMinMaxVarsPerChannelGradient::FakeQuantWithMinMaxVarsPerChannelGradient(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients, ::tensorflow::Input inputs, ::tensorflow::Input min, ::tensorflow::Input
                                                                                     max)
  : FakeQuantWithMinMaxVarsPerChannelGradient(scope, gradients, inputs, min, max, FakeQuantWithMinMaxVarsPerChannelGradient::Attrs()) {}

Fill::Fill(const ::tensorflow::Scope& scope, ::tensorflow::Input dims,
           ::tensorflow::Input value) {
  if (!scope.ok()) return;
  auto _dims = ::tensorflow::ops::AsNodeOut(scope, dims);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Fill");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Fill")
                     .Input(_dims)
                     .Input(_value)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Gather::Gather(const ::tensorflow::Scope& scope, ::tensorflow::Input params,
               ::tensorflow::Input indices, const Gather::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _params = ::tensorflow::ops::AsNodeOut(scope, params);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Gather");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Gather")
                     .Input(_params)
                     .Input(_indices)
                     .Attr("validate_indices", attrs.validate_indices_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Gather::Gather(const ::tensorflow::Scope& scope, ::tensorflow::Input params,
               ::tensorflow::Input indices)
  : Gather(scope, params, indices, Gather::Attrs()) {}

GatherNd::GatherNd(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   params, ::tensorflow::Input indices) {
  if (!scope.ok()) return;
  auto _params = ::tensorflow::ops::AsNodeOut(scope, params);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("GatherNd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "GatherNd")
                     .Input(_params)
                     .Input(_indices)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

GatherV2::GatherV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   params, ::tensorflow::Input indices, ::tensorflow::Input
                   axis) {
  if (!scope.ok()) return;
  auto _params = ::tensorflow::ops::AsNodeOut(scope, params);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("GatherV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "GatherV2")
                     .Input(_params)
                     .Input(_indices)
                     .Input(_axis)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

GuaranteeConst::GuaranteeConst(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("GuaranteeConst");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "GuaranteeConst")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Identity::Identity(const ::tensorflow::Scope& scope, ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Identity");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Identity")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

IdentityN::IdentityN(const ::tensorflow::Scope& scope, ::tensorflow::InputList
                     input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOutList(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("IdentityN");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "IdentityN")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->output.push_back(Output(ret, i));
}

ImmutableConst::ImmutableConst(const ::tensorflow::Scope& scope, DataType
                               dtype, PartialTensorShape shape, StringPiece
                               memory_region_name) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ImmutableConst");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ImmutableConst")
                     .Attr("dtype", dtype)
                     .Attr("shape", shape)
                     .Attr("memory_region_name", memory_region_name)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->tensor = Output(ret, 0);
}

InplaceAdd::InplaceAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                       ::tensorflow::Input i, ::tensorflow::Input v) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _i = ::tensorflow::ops::AsNodeOut(scope, i);
  if (!scope.ok()) return;
  auto _v = ::tensorflow::ops::AsNodeOut(scope, v);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InplaceAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InplaceAdd")
                     .Input(_x)
                     .Input(_i)
                     .Input(_v)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

InplaceSub::InplaceSub(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                       ::tensorflow::Input i, ::tensorflow::Input v) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _i = ::tensorflow::ops::AsNodeOut(scope, i);
  if (!scope.ok()) return;
  auto _v = ::tensorflow::ops::AsNodeOut(scope, v);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InplaceSub");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InplaceSub")
                     .Input(_x)
                     .Input(_i)
                     .Input(_v)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

InplaceUpdate::InplaceUpdate(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input x, ::tensorflow::Input i,
                             ::tensorflow::Input v) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _i = ::tensorflow::ops::AsNodeOut(scope, i);
  if (!scope.ok()) return;
  auto _v = ::tensorflow::ops::AsNodeOut(scope, v);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InplaceUpdate");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InplaceUpdate")
                     .Input(_x)
                     .Input(_i)
                     .Input(_v)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

InvertPermutation::InvertPermutation(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InvertPermutation");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InvertPermutation")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

SetDiff1D::SetDiff1D(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                     ::tensorflow::Input y, const SetDiff1D::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _y = ::tensorflow::ops::AsNodeOut(scope, y);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SetDiff1D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ListDiff")
                     .Input(_x)
                     .Input(_y)
                     .Attr("out_idx", attrs.out_idx_)
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
  this->idx = Output(ret, _outputs_range["idx"].first);
}

SetDiff1D::SetDiff1D(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                     ::tensorflow::Input y)
  : SetDiff1D(scope, x, y, SetDiff1D::Attrs()) {}

MatrixBandPart::MatrixBandPart(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input, ::tensorflow::Input
                               num_lower, ::tensorflow::Input num_upper) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _num_lower = ::tensorflow::ops::AsNodeOut(scope, num_lower);
  if (!scope.ok()) return;
  auto _num_upper = ::tensorflow::ops::AsNodeOut(scope, num_upper);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixBandPart");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixBandPart")
                     .Input(_input)
                     .Input(_num_lower)
                     .Input(_num_upper)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->band = Output(ret, 0);
}

MatrixDiag::MatrixDiag(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       diagonal) {
  if (!scope.ok()) return;
  auto _diagonal = ::tensorflow::ops::AsNodeOut(scope, diagonal);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixDiag");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixDiag")
                     .Input(_diagonal)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MatrixDiagPart::MatrixDiagPart(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixDiagPart");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixDiagPart")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->diagonal = Output(ret, 0);
}

MatrixSetDiag::MatrixSetDiag(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input input, ::tensorflow::Input
                             diagonal) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _diagonal = ::tensorflow::ops::AsNodeOut(scope, diagonal);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixSetDiag");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixSetDiag")
                     .Input(_input)
                     .Input(_diagonal)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MirrorPad::MirrorPad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, ::tensorflow::Input paddings, StringPiece mode) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _paddings = ::tensorflow::ops::AsNodeOut(scope, paddings);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MirrorPad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MirrorPad")
                     .Input(_input)
                     .Input(_paddings)
                     .Attr("mode", mode)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

OneHot::OneHot(const ::tensorflow::Scope& scope, ::tensorflow::Input indices,
               ::tensorflow::Input depth, ::tensorflow::Input on_value,
               ::tensorflow::Input off_value, const OneHot::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _depth = ::tensorflow::ops::AsNodeOut(scope, depth);
  if (!scope.ok()) return;
  auto _on_value = ::tensorflow::ops::AsNodeOut(scope, on_value);
  if (!scope.ok()) return;
  auto _off_value = ::tensorflow::ops::AsNodeOut(scope, off_value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OneHot");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OneHot")
                     .Input(_indices)
                     .Input(_depth)
                     .Input(_on_value)
                     .Input(_off_value)
                     .Attr("axis", attrs.axis_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

OneHot::OneHot(const ::tensorflow::Scope& scope, ::tensorflow::Input indices,
               ::tensorflow::Input depth, ::tensorflow::Input on_value,
               ::tensorflow::Input off_value)
  : OneHot(scope, indices, depth, on_value, off_value, OneHot::Attrs()) {}

OnesLike::OnesLike(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("OnesLike");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "OnesLike")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Stack::Stack(const ::tensorflow::Scope& scope, ::tensorflow::InputList values,
             const Stack::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Stack");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Pack")
                     .Input(_values)
                     .Attr("axis", attrs.axis_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Stack::Stack(const ::tensorflow::Scope& scope, ::tensorflow::InputList values)
  : Stack(scope, values, Stack::Attrs()) {}

Pad::Pad(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
         ::tensorflow::Input paddings) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _paddings = ::tensorflow::ops::AsNodeOut(scope, paddings);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Pad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Pad")
                     .Input(_input)
                     .Input(_paddings)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

PadV2::PadV2(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
             ::tensorflow::Input paddings, ::tensorflow::Input constant_values) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _paddings = ::tensorflow::ops::AsNodeOut(scope, paddings);
  if (!scope.ok()) return;
  auto _constant_values = ::tensorflow::ops::AsNodeOut(scope, constant_values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("PadV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "PadV2")
                     .Input(_input)
                     .Input(_paddings)
                     .Input(_constant_values)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ParallelConcat::ParallelConcat(const ::tensorflow::Scope& scope,
                               ::tensorflow::InputList values,
                               PartialTensorShape shape) {
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ParallelConcat");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ParallelConcat")
                     .Input(_values)
                     .Attr("shape", shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Placeholder::Placeholder(const ::tensorflow::Scope& scope, DataType dtype,
                         const Placeholder::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Placeholder");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Placeholder")
                     .Attr("dtype", dtype)
                     .Attr("shape", attrs.shape_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Placeholder::Placeholder(const ::tensorflow::Scope& scope, DataType dtype)
  : Placeholder(scope, dtype, Placeholder::Attrs()) {}

PlaceholderWithDefault::PlaceholderWithDefault(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input
                                               input, PartialTensorShape shape) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("PlaceholderWithDefault");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "PlaceholderWithDefault")
                     .Input(_input)
                     .Attr("shape", shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

PreventGradient::PreventGradient(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input, const
                                 PreventGradient::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("PreventGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "PreventGradient")
                     .Input(_input)
                     .Attr("message", attrs.message_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

PreventGradient::PreventGradient(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input)
  : PreventGradient(scope, input, PreventGradient::Attrs()) {}

QuantizeAndDequantizeV2::QuantizeAndDequantizeV2(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 input_min, ::tensorflow::Input
                                                 input_max, const
                                                 QuantizeAndDequantizeV2::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _input_min = ::tensorflow::ops::AsNodeOut(scope, input_min);
  if (!scope.ok()) return;
  auto _input_max = ::tensorflow::ops::AsNodeOut(scope, input_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizeAndDequantizeV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizeAndDequantizeV2")
                     .Input(_input)
                     .Input(_input_min)
                     .Input(_input_max)
                     .Attr("signed_input", attrs.signed_input_)
                     .Attr("num_bits", attrs.num_bits_)
                     .Attr("range_given", attrs.range_given_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

QuantizeAndDequantizeV2::QuantizeAndDequantizeV2(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 input_min, ::tensorflow::Input
                                                 input_max)
  : QuantizeAndDequantizeV2(scope, input, input_min, input_max, QuantizeAndDequantizeV2::Attrs()) {}

QuantizeAndDequantizeV3::QuantizeAndDequantizeV3(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 input_min, ::tensorflow::Input
                                                 input_max, ::tensorflow::Input
                                                 num_bits, const
                                                 QuantizeAndDequantizeV3::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _input_min = ::tensorflow::ops::AsNodeOut(scope, input_min);
  if (!scope.ok()) return;
  auto _input_max = ::tensorflow::ops::AsNodeOut(scope, input_max);
  if (!scope.ok()) return;
  auto _num_bits = ::tensorflow::ops::AsNodeOut(scope, num_bits);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizeAndDequantizeV3");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizeAndDequantizeV3")
                     .Input(_input)
                     .Input(_input_min)
                     .Input(_input_max)
                     .Input(_num_bits)
                     .Attr("signed_input", attrs.signed_input_)
                     .Attr("range_given", attrs.range_given_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

QuantizeAndDequantizeV3::QuantizeAndDequantizeV3(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 input_min, ::tensorflow::Input
                                                 input_max, ::tensorflow::Input
                                                 num_bits)
  : QuantizeAndDequantizeV3(scope, input, input_min, input_max, num_bits, QuantizeAndDequantizeV3::Attrs()) {}

QuantizeV2::QuantizeV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input min_range,
                       ::tensorflow::Input max_range, DataType T, const
                       QuantizeV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _min_range = ::tensorflow::ops::AsNodeOut(scope, min_range);
  if (!scope.ok()) return;
  auto _max_range = ::tensorflow::ops::AsNodeOut(scope, max_range);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizeV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizeV2")
                     .Input(_input)
                     .Input(_min_range)
                     .Input(_max_range)
                     .Attr("T", T)
                     .Attr("mode", attrs.mode_)
                     .Attr("round_mode", attrs.round_mode_)
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

QuantizeV2::QuantizeV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input min_range,
                       ::tensorflow::Input max_range, DataType T)
  : QuantizeV2(scope, input, min_range, max_range, T, QuantizeV2::Attrs()) {}

QuantizedConcat::QuantizedConcat(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input concat_dim,
                                 ::tensorflow::InputList values,
                                 ::tensorflow::InputList input_mins,
                                 ::tensorflow::InputList input_maxes) {
  if (!scope.ok()) return;
  auto _concat_dim = ::tensorflow::ops::AsNodeOut(scope, concat_dim);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOutList(scope, values);
  if (!scope.ok()) return;
  auto _input_mins = ::tensorflow::ops::AsNodeOutList(scope, input_mins);
  if (!scope.ok()) return;
  auto _input_maxes = ::tensorflow::ops::AsNodeOutList(scope, input_maxes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedConcat");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedConcat")
                     .Input(_concat_dim)
                     .Input(_values)
                     .Input(_input_mins)
                     .Input(_input_maxes)
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

QuantizedInstanceNorm::QuantizedInstanceNorm(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input x,
                                             ::tensorflow::Input x_min,
                                             ::tensorflow::Input x_max, const
                                             QuantizedInstanceNorm::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _x_min = ::tensorflow::ops::AsNodeOut(scope, x_min);
  if (!scope.ok()) return;
  auto _x_max = ::tensorflow::ops::AsNodeOut(scope, x_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedInstanceNorm");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedInstanceNorm")
                     .Input(_x)
                     .Input(_x_min)
                     .Input(_x_max)
                     .Attr("output_range_given", attrs.output_range_given_)
                     .Attr("given_y_min", attrs.given_y_min_)
                     .Attr("given_y_max", attrs.given_y_max_)
                     .Attr("variance_epsilon", attrs.variance_epsilon_)
                     .Attr("min_separation", attrs.min_separation_)
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

  this->y = Output(ret, _outputs_range["y"].first);
  this->y_min = Output(ret, _outputs_range["y_min"].first);
  this->y_max = Output(ret, _outputs_range["y_max"].first);
}

QuantizedInstanceNorm::QuantizedInstanceNorm(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input x,
                                             ::tensorflow::Input x_min,
                                             ::tensorflow::Input x_max)
  : QuantizedInstanceNorm(scope, x, x_min, x_max, QuantizedInstanceNorm::Attrs()) {}

QuantizedReshape::QuantizedReshape(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input tensor,
                                   ::tensorflow::Input shape,
                                   ::tensorflow::Input input_min,
                                   ::tensorflow::Input input_max) {
  if (!scope.ok()) return;
  auto _tensor = ::tensorflow::ops::AsNodeOut(scope, tensor);
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  auto _input_min = ::tensorflow::ops::AsNodeOut(scope, input_min);
  if (!scope.ok()) return;
  auto _input_max = ::tensorflow::ops::AsNodeOut(scope, input_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedReshape");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedReshape")
                     .Input(_tensor)
                     .Input(_shape)
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

  this->output = Output(ret, _outputs_range["output"].first);
  this->output_min = Output(ret, _outputs_range["output_min"].first);
  this->output_max = Output(ret, _outputs_range["output_max"].first);
}

Rank::Rank(const ::tensorflow::Scope& scope, ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Rank");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Rank")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Reshape::Reshape(const ::tensorflow::Scope& scope, ::tensorflow::Input tensor,
                 ::tensorflow::Input shape) {
  if (!scope.ok()) return;
  auto _tensor = ::tensorflow::ops::AsNodeOut(scope, tensor);
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Reshape");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Reshape")
                     .Input(_tensor)
                     .Input(_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ResourceStridedSliceAssign::ResourceStridedSliceAssign(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input ref,
                                                       ::tensorflow::Input
                                                       begin,
                                                       ::tensorflow::Input end,
                                                       ::tensorflow::Input
                                                       strides,
                                                       ::tensorflow::Input
                                                       value, const
                                                       ResourceStridedSliceAssign::Attrs&
                                                       attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _begin = ::tensorflow::ops::AsNodeOut(scope, begin);
  if (!scope.ok()) return;
  auto _end = ::tensorflow::ops::AsNodeOut(scope, end);
  if (!scope.ok()) return;
  auto _strides = ::tensorflow::ops::AsNodeOut(scope, strides);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ResourceStridedSliceAssign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ResourceStridedSliceAssign")
                     .Input(_ref)
                     .Input(_begin)
                     .Input(_end)
                     .Input(_strides)
                     .Input(_value)
                     .Attr("begin_mask", attrs.begin_mask_)
                     .Attr("end_mask", attrs.end_mask_)
                     .Attr("ellipsis_mask", attrs.ellipsis_mask_)
                     .Attr("new_axis_mask", attrs.new_axis_mask_)
                     .Attr("shrink_axis_mask", attrs.shrink_axis_mask_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ResourceStridedSliceAssign::ResourceStridedSliceAssign(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input ref,
                                                       ::tensorflow::Input
                                                       begin,
                                                       ::tensorflow::Input end,
                                                       ::tensorflow::Input
                                                       strides,
                                                       ::tensorflow::Input
                                                       value)
  : ResourceStridedSliceAssign(scope, ref, begin, end, strides, value, ResourceStridedSliceAssign::Attrs()) {}

ReverseSequence::ReverseSequence(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input, ::tensorflow::Input
                                 seq_lengths, int64 seq_dim, const
                                 ReverseSequence::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _seq_lengths = ::tensorflow::ops::AsNodeOut(scope, seq_lengths);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReverseSequence");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReverseSequence")
                     .Input(_input)
                     .Input(_seq_lengths)
                     .Attr("seq_dim", seq_dim)
                     .Attr("batch_dim", attrs.batch_dim_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ReverseSequence::ReverseSequence(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input, ::tensorflow::Input
                                 seq_lengths, int64 seq_dim)
  : ReverseSequence(scope, input, seq_lengths, seq_dim, ReverseSequence::Attrs()) {}

Reverse::Reverse(const ::tensorflow::Scope& scope, ::tensorflow::Input tensor,
                 ::tensorflow::Input axis) {
  if (!scope.ok()) return;
  auto _tensor = ::tensorflow::ops::AsNodeOut(scope, tensor);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Reverse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReverseV2")
                     .Input(_tensor)
                     .Input(_axis)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ScatterNd::ScatterNd(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     indices, ::tensorflow::Input updates, ::tensorflow::Input
                     shape) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterNd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterNd")
                     .Input(_indices)
                     .Input(_updates)
                     .Input(_shape)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

ScatterNdNonAliasingAdd::ScatterNdNonAliasingAdd(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 indices, ::tensorflow::Input
                                                 updates) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _updates = ::tensorflow::ops::AsNodeOut(scope, updates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ScatterNdNonAliasingAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ScatterNdNonAliasingAdd")
                     .Input(_input)
                     .Input(_indices)
                     .Input(_updates)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Shape::Shape(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
             Shape::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Shape");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Shape")
                     .Input(_input)
                     .Attr("out_type", attrs.out_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Shape::Shape(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Shape(scope, input, Shape::Attrs()) {}

ShapeN::ShapeN(const ::tensorflow::Scope& scope, ::tensorflow::InputList input,
               const ShapeN::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOutList(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ShapeN");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ShapeN")
                     .Input(_input)
                     .Attr("out_type", attrs.out_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->output.push_back(Output(ret, i));
}

ShapeN::ShapeN(const ::tensorflow::Scope& scope, ::tensorflow::InputList input)
  : ShapeN(scope, input, ShapeN::Attrs()) {}

Size::Size(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
           Size::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Size");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Size")
                     .Input(_input)
                     .Attr("out_type", attrs.out_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Size::Size(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Size(scope, input, Size::Attrs()) {}

Slice::Slice(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
             ::tensorflow::Input begin, ::tensorflow::Input size) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _begin = ::tensorflow::ops::AsNodeOut(scope, begin);
  if (!scope.ok()) return;
  auto _size = ::tensorflow::ops::AsNodeOut(scope, size);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Slice");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Slice")
                     .Input(_input)
                     .Input(_begin)
                     .Input(_size)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Snapshot::Snapshot(const ::tensorflow::Scope& scope, ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Snapshot");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Snapshot")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SpaceToBatch::SpaceToBatch(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, ::tensorflow::Input
                           paddings, int64 block_size) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _paddings = ::tensorflow::ops::AsNodeOut(scope, paddings);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SpaceToBatch");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SpaceToBatch")
                     .Input(_input)
                     .Input(_paddings)
                     .Attr("block_size", block_size)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SpaceToBatchND::SpaceToBatchND(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input, ::tensorflow::Input
                               block_shape, ::tensorflow::Input paddings) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _block_shape = ::tensorflow::ops::AsNodeOut(scope, block_shape);
  if (!scope.ok()) return;
  auto _paddings = ::tensorflow::ops::AsNodeOut(scope, paddings);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SpaceToBatchND");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SpaceToBatchND")
                     .Input(_input)
                     .Input(_block_shape)
                     .Input(_paddings)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SpaceToDepth::SpaceToDepth(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, int64 block_size, const
                           SpaceToDepth::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SpaceToDepth");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SpaceToDepth")
                     .Input(_input)
                     .Attr("block_size", block_size)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SpaceToDepth::SpaceToDepth(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, int64 block_size)
  : SpaceToDepth(scope, input, block_size, SpaceToDepth::Attrs()) {}

Split::Split(const ::tensorflow::Scope& scope, ::tensorflow::Input axis,
             ::tensorflow::Input value, int64 num_split) {
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Split");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Split")
                     .Input(_axis)
                     .Input(_value)
                     .Attr("num_split", num_split)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->output.push_back(Output(ret, i));
}

SplitV::SplitV(const ::tensorflow::Scope& scope, ::tensorflow::Input value,
               ::tensorflow::Input size_splits, ::tensorflow::Input axis, int64
               num_split) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  auto _size_splits = ::tensorflow::ops::AsNodeOut(scope, size_splits);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SplitV");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SplitV")
                     .Input(_value)
                     .Input(_size_splits)
                     .Input(_axis)
                     .Attr("num_split", num_split)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->output.push_back(Output(ret, i));
}

Squeeze::Squeeze(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
                 const Squeeze::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Squeeze");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Squeeze")
                     .Input(_input)
                     .Attr("squeeze_dims", attrs.axis_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Squeeze::Squeeze(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Squeeze(scope, input, Squeeze::Attrs()) {}

StopGradient::StopGradient(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StopGradient");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StopGradient")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

StridedSlice::StridedSlice(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, ::tensorflow::Input
                           begin, ::tensorflow::Input end, ::tensorflow::Input
                           strides, const StridedSlice::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _begin = ::tensorflow::ops::AsNodeOut(scope, begin);
  if (!scope.ok()) return;
  auto _end = ::tensorflow::ops::AsNodeOut(scope, end);
  if (!scope.ok()) return;
  auto _strides = ::tensorflow::ops::AsNodeOut(scope, strides);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StridedSlice");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StridedSlice")
                     .Input(_input)
                     .Input(_begin)
                     .Input(_end)
                     .Input(_strides)
                     .Attr("begin_mask", attrs.begin_mask_)
                     .Attr("end_mask", attrs.end_mask_)
                     .Attr("ellipsis_mask", attrs.ellipsis_mask_)
                     .Attr("new_axis_mask", attrs.new_axis_mask_)
                     .Attr("shrink_axis_mask", attrs.shrink_axis_mask_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

StridedSlice::StridedSlice(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, ::tensorflow::Input
                           begin, ::tensorflow::Input end, ::tensorflow::Input
                           strides)
  : StridedSlice(scope, input, begin, end, strides, StridedSlice::Attrs()) {}

StridedSliceAssign::StridedSliceAssign(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input ref,
                                       ::tensorflow::Input begin,
                                       ::tensorflow::Input end,
                                       ::tensorflow::Input strides,
                                       ::tensorflow::Input value, const
                                       StridedSliceAssign::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _ref = ::tensorflow::ops::AsNodeOut(scope, ref);
  if (!scope.ok()) return;
  auto _begin = ::tensorflow::ops::AsNodeOut(scope, begin);
  if (!scope.ok()) return;
  auto _end = ::tensorflow::ops::AsNodeOut(scope, end);
  if (!scope.ok()) return;
  auto _strides = ::tensorflow::ops::AsNodeOut(scope, strides);
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StridedSliceAssign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StridedSliceAssign")
                     .Input(_ref)
                     .Input(_begin)
                     .Input(_end)
                     .Input(_strides)
                     .Input(_value)
                     .Attr("begin_mask", attrs.begin_mask_)
                     .Attr("end_mask", attrs.end_mask_)
                     .Attr("ellipsis_mask", attrs.ellipsis_mask_)
                     .Attr("new_axis_mask", attrs.new_axis_mask_)
                     .Attr("shrink_axis_mask", attrs.shrink_axis_mask_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output_ref = Output(ret, 0);
}

StridedSliceAssign::StridedSliceAssign(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input ref,
                                       ::tensorflow::Input begin,
                                       ::tensorflow::Input end,
                                       ::tensorflow::Input strides,
                                       ::tensorflow::Input value)
  : StridedSliceAssign(scope, ref, begin, end, strides, value, StridedSliceAssign::Attrs()) {}

StridedSliceGrad::StridedSliceGrad(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input shape,
                                   ::tensorflow::Input begin,
                                   ::tensorflow::Input end, ::tensorflow::Input
                                   strides, ::tensorflow::Input dy, const
                                   StridedSliceGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _shape = ::tensorflow::ops::AsNodeOut(scope, shape);
  if (!scope.ok()) return;
  auto _begin = ::tensorflow::ops::AsNodeOut(scope, begin);
  if (!scope.ok()) return;
  auto _end = ::tensorflow::ops::AsNodeOut(scope, end);
  if (!scope.ok()) return;
  auto _strides = ::tensorflow::ops::AsNodeOut(scope, strides);
  if (!scope.ok()) return;
  auto _dy = ::tensorflow::ops::AsNodeOut(scope, dy);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StridedSliceGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StridedSliceGrad")
                     .Input(_shape)
                     .Input(_begin)
                     .Input(_end)
                     .Input(_strides)
                     .Input(_dy)
                     .Attr("begin_mask", attrs.begin_mask_)
                     .Attr("end_mask", attrs.end_mask_)
                     .Attr("ellipsis_mask", attrs.ellipsis_mask_)
                     .Attr("new_axis_mask", attrs.new_axis_mask_)
                     .Attr("shrink_axis_mask", attrs.shrink_axis_mask_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

StridedSliceGrad::StridedSliceGrad(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input shape,
                                   ::tensorflow::Input begin,
                                   ::tensorflow::Input end, ::tensorflow::Input
                                   strides, ::tensorflow::Input dy)
  : StridedSliceGrad(scope, shape, begin, end, strides, dy, StridedSliceGrad::Attrs()) {}

Tile::Tile(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input multiples) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _multiples = ::tensorflow::ops::AsNodeOut(scope, multiples);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Tile");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Tile")
                     .Input(_input)
                     .Input(_multiples)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Transpose::Transpose(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                     ::tensorflow::Input perm) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _perm = ::tensorflow::ops::AsNodeOut(scope, perm);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Transpose");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Transpose")
                     .Input(_x)
                     .Input(_perm)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

Unique::Unique(const ::tensorflow::Scope& scope, ::tensorflow::Input x, const
               Unique::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Unique");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Unique")
                     .Input(_x)
                     .Attr("out_idx", attrs.out_idx_)
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

  this->y = Output(ret, _outputs_range["y"].first);
  this->idx = Output(ret, _outputs_range["idx"].first);
}

Unique::Unique(const ::tensorflow::Scope& scope, ::tensorflow::Input x)
  : Unique(scope, x, Unique::Attrs()) {}

UniqueV2::UniqueV2(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                   ::tensorflow::Input axis, const UniqueV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UniqueV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UniqueV2")
                     .Input(_x)
                     .Input(_axis)
                     .Attr("out_idx", attrs.out_idx_)
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

  this->y = Output(ret, _outputs_range["y"].first);
  this->idx = Output(ret, _outputs_range["idx"].first);
}

UniqueV2::UniqueV2(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                   ::tensorflow::Input axis)
  : UniqueV2(scope, x, axis, UniqueV2::Attrs()) {}

UniqueWithCounts::UniqueWithCounts(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x, const
                                   UniqueWithCounts::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UniqueWithCounts");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UniqueWithCounts")
                     .Input(_x)
                     .Attr("out_idx", attrs.out_idx_)
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

  this->y = Output(ret, _outputs_range["y"].first);
  this->idx = Output(ret, _outputs_range["idx"].first);
  this->count = Output(ret, _outputs_range["count"].first);
}

UniqueWithCounts::UniqueWithCounts(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x)
  : UniqueWithCounts(scope, x, UniqueWithCounts::Attrs()) {}

UniqueWithCountsV2::UniqueWithCountsV2(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input x,
                                       ::tensorflow::Input axis, const
                                       UniqueWithCountsV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _axis = ::tensorflow::ops::AsNodeOut(scope, axis);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UniqueWithCountsV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UniqueWithCountsV2")
                     .Input(_x)
                     .Input(_axis)
                     .Attr("out_idx", attrs.out_idx_)
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

  this->y = Output(ret, _outputs_range["y"].first);
  this->idx = Output(ret, _outputs_range["idx"].first);
  this->count = Output(ret, _outputs_range["count"].first);
}

UniqueWithCountsV2::UniqueWithCountsV2(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input x,
                                       ::tensorflow::Input axis)
  : UniqueWithCountsV2(scope, x, axis, UniqueWithCountsV2::Attrs()) {}

Unstack::Unstack(const ::tensorflow::Scope& scope, ::tensorflow::Input value,
                 int64 num, const Unstack::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Unstack");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Unpack")
                     .Input(_value)
                     .Attr("num", num)
                     .Attr("axis", attrs.axis_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->output.push_back(Output(ret, i));
}

Unstack::Unstack(const ::tensorflow::Scope& scope, ::tensorflow::Input value,
                 int64 num)
  : Unstack(scope, value, num, Unstack::Attrs()) {}

UnravelIndex::UnravelIndex(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input indices, ::tensorflow::Input
                           dims) {
  if (!scope.ok()) return;
  auto _indices = ::tensorflow::ops::AsNodeOut(scope, indices);
  if (!scope.ok()) return;
  auto _dims = ::tensorflow::ops::AsNodeOut(scope, dims);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UnravelIndex");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UnravelIndex")
                     .Input(_indices)
                     .Input(_dims)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Where::Where(const ::tensorflow::Scope& scope, ::tensorflow::Input condition) {
  if (!scope.ok()) return;
  auto _condition = ::tensorflow::ops::AsNodeOut(scope, condition);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Where");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Where")
                     .Input(_condition)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->index = Output(ret, 0);
}

ZerosLike::ZerosLike(const ::tensorflow::Scope& scope, ::tensorflow::Input x) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ZerosLike");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ZerosLike")
                     .Input(_x)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

/// @}

}  // namespace ops
}  // namespace tensorflow
