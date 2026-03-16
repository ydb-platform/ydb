// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/nn_ops_internal.h"

namespace tensorflow {
namespace ops {
namespace internal {
// NOTE: This namespace has internal TensorFlow details that
// are not part of TensorFlow's public API.

AvgPoolGrad::AvgPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         orig_input_shape, ::tensorflow::Input grad, const
                         gtl::ArraySlice<int>& ksize, const
                         gtl::ArraySlice<int>& strides, StringPiece padding,
                         const AvgPoolGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _orig_input_shape = ::tensorflow::ops::AsNodeOut(scope, orig_input_shape);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AvgPoolGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AvgPoolGrad")
                     .Input(_orig_input_shape)
                     .Input(_grad)
                     .Attr("ksize", ksize)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

AvgPoolGrad::AvgPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         orig_input_shape, ::tensorflow::Input grad, const
                         gtl::ArraySlice<int>& ksize, const
                         gtl::ArraySlice<int>& strides, StringPiece padding)
  : AvgPoolGrad(scope, orig_input_shape, grad, ksize, strides, padding, AvgPoolGrad::Attrs()) {}

EluGrad::EluGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                 gradients, ::tensorflow::Input outputs) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _outputs = ::tensorflow::ops::AsNodeOut(scope, outputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("EluGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "EluGrad")
                     .Input(_gradients)
                     .Input(_outputs)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->backprops = Output(ret, 0);
}

FractionalAvgPoolGrad::FractionalAvgPoolGrad(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input
                                             orig_input_tensor_shape,
                                             ::tensorflow::Input out_backprop,
                                             ::tensorflow::Input
                                             row_pooling_sequence,
                                             ::tensorflow::Input
                                             col_pooling_sequence, const
                                             FractionalAvgPoolGrad::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _orig_input_tensor_shape = ::tensorflow::ops::AsNodeOut(scope, orig_input_tensor_shape);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  auto _row_pooling_sequence = ::tensorflow::ops::AsNodeOut(scope, row_pooling_sequence);
  if (!scope.ok()) return;
  auto _col_pooling_sequence = ::tensorflow::ops::AsNodeOut(scope, col_pooling_sequence);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FractionalAvgPoolGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FractionalAvgPoolGrad")
                     .Input(_orig_input_tensor_shape)
                     .Input(_out_backprop)
                     .Input(_row_pooling_sequence)
                     .Input(_col_pooling_sequence)
                     .Attr("overlapping", attrs.overlapping_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

FractionalAvgPoolGrad::FractionalAvgPoolGrad(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input
                                             orig_input_tensor_shape,
                                             ::tensorflow::Input out_backprop,
                                             ::tensorflow::Input
                                             row_pooling_sequence,
                                             ::tensorflow::Input
                                             col_pooling_sequence)
  : FractionalAvgPoolGrad(scope, orig_input_tensor_shape, out_backprop, row_pooling_sequence, col_pooling_sequence, FractionalAvgPoolGrad::Attrs()) {}

FractionalMaxPoolGrad::FractionalMaxPoolGrad(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input orig_input,
                                             ::tensorflow::Input orig_output,
                                             ::tensorflow::Input out_backprop,
                                             ::tensorflow::Input
                                             row_pooling_sequence,
                                             ::tensorflow::Input
                                             col_pooling_sequence, const
                                             FractionalMaxPoolGrad::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _orig_input = ::tensorflow::ops::AsNodeOut(scope, orig_input);
  if (!scope.ok()) return;
  auto _orig_output = ::tensorflow::ops::AsNodeOut(scope, orig_output);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  auto _row_pooling_sequence = ::tensorflow::ops::AsNodeOut(scope, row_pooling_sequence);
  if (!scope.ok()) return;
  auto _col_pooling_sequence = ::tensorflow::ops::AsNodeOut(scope, col_pooling_sequence);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FractionalMaxPoolGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FractionalMaxPoolGrad")
                     .Input(_orig_input)
                     .Input(_orig_output)
                     .Input(_out_backprop)
                     .Input(_row_pooling_sequence)
                     .Input(_col_pooling_sequence)
                     .Attr("overlapping", attrs.overlapping_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

FractionalMaxPoolGrad::FractionalMaxPoolGrad(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input orig_input,
                                             ::tensorflow::Input orig_output,
                                             ::tensorflow::Input out_backprop,
                                             ::tensorflow::Input
                                             row_pooling_sequence,
                                             ::tensorflow::Input
                                             col_pooling_sequence)
  : FractionalMaxPoolGrad(scope, orig_input, orig_output, out_backprop, row_pooling_sequence, col_pooling_sequence, FractionalMaxPoolGrad::Attrs()) {}

LRNGrad::LRNGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                 input_grads, ::tensorflow::Input input_image,
                 ::tensorflow::Input output_image, const LRNGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input_grads = ::tensorflow::ops::AsNodeOut(scope, input_grads);
  if (!scope.ok()) return;
  auto _input_image = ::tensorflow::ops::AsNodeOut(scope, input_image);
  if (!scope.ok()) return;
  auto _output_image = ::tensorflow::ops::AsNodeOut(scope, output_image);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LRNGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LRNGrad")
                     .Input(_input_grads)
                     .Input(_input_image)
                     .Input(_output_image)
                     .Attr("depth_radius", attrs.depth_radius_)
                     .Attr("bias", attrs.bias_)
                     .Attr("alpha", attrs.alpha_)
                     .Attr("beta", attrs.beta_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

LRNGrad::LRNGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                 input_grads, ::tensorflow::Input input_image,
                 ::tensorflow::Input output_image)
  : LRNGrad(scope, input_grads, input_image, output_image, LRNGrad::Attrs()) {}

MaxPoolGrad::MaxPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         orig_input, ::tensorflow::Input orig_output,
                         ::tensorflow::Input grad, const gtl::ArraySlice<int>&
                         ksize, const gtl::ArraySlice<int>& strides,
                         StringPiece padding, const MaxPoolGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _orig_input = ::tensorflow::ops::AsNodeOut(scope, orig_input);
  if (!scope.ok()) return;
  auto _orig_output = ::tensorflow::ops::AsNodeOut(scope, orig_output);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolGrad")
                     .Input(_orig_input)
                     .Input(_orig_output)
                     .Input(_grad)
                     .Attr("ksize", ksize)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MaxPoolGrad::MaxPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         orig_input, ::tensorflow::Input orig_output,
                         ::tensorflow::Input grad, const gtl::ArraySlice<int>&
                         ksize, const gtl::ArraySlice<int>& strides,
                         StringPiece padding)
  : MaxPoolGrad(scope, orig_input, orig_output, grad, ksize, strides, padding, MaxPoolGrad::Attrs()) {}

MaxPoolGradWithArgmax::MaxPoolGradWithArgmax(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input,
                                             ::tensorflow::Input grad,
                                             ::tensorflow::Input argmax, const
                                             gtl::ArraySlice<int>& ksize, const
                                             gtl::ArraySlice<int>& strides,
                                             StringPiece padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _argmax = ::tensorflow::ops::AsNodeOut(scope, argmax);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolGradWithArgmax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolGradWithArgmax")
                     .Input(_input)
                     .Input(_grad)
                     .Input(_argmax)
                     .Attr("ksize", ksize)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Relu6Grad::Relu6Grad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     gradients, ::tensorflow::Input features) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Relu6Grad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Relu6Grad")
                     .Input(_gradients)
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->backprops = Output(ret, 0);
}

ReluGrad::ReluGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   gradients, ::tensorflow::Input features) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReluGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReluGrad")
                     .Input(_gradients)
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->backprops = Output(ret, 0);
}

SeluGrad::SeluGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   gradients, ::tensorflow::Input outputs) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _outputs = ::tensorflow::ops::AsNodeOut(scope, outputs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SeluGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SeluGrad")
                     .Input(_gradients)
                     .Input(_outputs)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->backprops = Output(ret, 0);
}

SoftplusGrad::SoftplusGrad(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input gradients, ::tensorflow::Input
                           features) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SoftplusGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SoftplusGrad")
                     .Input(_gradients)
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->backprops = Output(ret, 0);
}

SoftsignGrad::SoftsignGrad(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input gradients, ::tensorflow::Input
                           features) {
  if (!scope.ok()) return;
  auto _gradients = ::tensorflow::ops::AsNodeOut(scope, gradients);
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SoftsignGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SoftsignGrad")
                     .Input(_gradients)
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->backprops = Output(ret, 0);
}

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow
