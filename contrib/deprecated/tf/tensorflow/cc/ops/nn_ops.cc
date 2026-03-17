// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/nn_ops.h"

namespace tensorflow {
namespace ops {

AvgPool::AvgPool(const ::tensorflow::Scope& scope, ::tensorflow::Input value,
                 const gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>&
                 strides, StringPiece padding, const AvgPool::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AvgPool");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AvgPool")
                     .Input(_value)
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

AvgPool::AvgPool(const ::tensorflow::Scope& scope, ::tensorflow::Input value,
                 const gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>&
                 strides, StringPiece padding)
  : AvgPool(scope, value, ksize, strides, padding, AvgPool::Attrs()) {}

AvgPool3D::AvgPool3D(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, const gtl::ArraySlice<int>& ksize, const
                     gtl::ArraySlice<int>& strides, StringPiece padding, const
                     AvgPool3D::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AvgPool3D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AvgPool3D")
                     .Input(_input)
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

AvgPool3D::AvgPool3D(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, const gtl::ArraySlice<int>& ksize, const
                     gtl::ArraySlice<int>& strides, StringPiece padding)
  : AvgPool3D(scope, input, ksize, strides, padding, AvgPool3D::Attrs()) {}

AvgPool3DGrad::AvgPool3DGrad(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input orig_input_shape,
                             ::tensorflow::Input grad, const
                             gtl::ArraySlice<int>& ksize, const
                             gtl::ArraySlice<int>& strides, StringPiece
                             padding, const AvgPool3DGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _orig_input_shape = ::tensorflow::ops::AsNodeOut(scope, orig_input_shape);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AvgPool3DGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AvgPool3DGrad")
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

AvgPool3DGrad::AvgPool3DGrad(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input orig_input_shape,
                             ::tensorflow::Input grad, const
                             gtl::ArraySlice<int>& ksize, const
                             gtl::ArraySlice<int>& strides, StringPiece
                             padding)
  : AvgPool3DGrad(scope, orig_input_shape, grad, ksize, strides, padding, AvgPool3DGrad::Attrs()) {}

BiasAdd::BiasAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input value,
                 ::tensorflow::Input bias, const BiasAdd::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  auto _bias = ::tensorflow::ops::AsNodeOut(scope, bias);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BiasAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BiasAdd")
                     .Input(_value)
                     .Input(_bias)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

BiasAdd::BiasAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input value,
                 ::tensorflow::Input bias)
  : BiasAdd(scope, value, bias, BiasAdd::Attrs()) {}

BiasAddGrad::BiasAddGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         out_backprop, const BiasAddGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("BiasAddGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "BiasAddGrad")
                     .Input(_out_backprop)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

BiasAddGrad::BiasAddGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         out_backprop)
  : BiasAddGrad(scope, out_backprop, BiasAddGrad::Attrs()) {}

Conv2D::Conv2D(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input filter, const gtl::ArraySlice<int>& strides,
               StringPiece padding, const Conv2D::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Conv2D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Conv2D")
                     .Input(_input)
                     .Input(_filter)
                     .Attr("strides", strides)
                     .Attr("use_cudnn_on_gpu", attrs.use_cudnn_on_gpu_)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Conv2D::Conv2D(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input filter, const gtl::ArraySlice<int>& strides,
               StringPiece padding)
  : Conv2D(scope, input, filter, strides, padding, Conv2D::Attrs()) {}

Conv2DBackpropFilter::Conv2DBackpropFilter(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input input,
                                           ::tensorflow::Input filter_sizes,
                                           ::tensorflow::Input out_backprop,
                                           const gtl::ArraySlice<int>& strides,
                                           StringPiece padding, const
                                           Conv2DBackpropFilter::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter_sizes = ::tensorflow::ops::AsNodeOut(scope, filter_sizes);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Conv2DBackpropFilter");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Conv2DBackpropFilter")
                     .Input(_input)
                     .Input(_filter_sizes)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("use_cudnn_on_gpu", attrs.use_cudnn_on_gpu_)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Conv2DBackpropFilter::Conv2DBackpropFilter(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input input,
                                           ::tensorflow::Input filter_sizes,
                                           ::tensorflow::Input out_backprop,
                                           const gtl::ArraySlice<int>& strides,
                                           StringPiece padding)
  : Conv2DBackpropFilter(scope, input, filter_sizes, out_backprop, strides, padding, Conv2DBackpropFilter::Attrs()) {}

Conv2DBackpropInput::Conv2DBackpropInput(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input input_sizes,
                                         ::tensorflow::Input filter,
                                         ::tensorflow::Input out_backprop,
                                         const gtl::ArraySlice<int>& strides,
                                         StringPiece padding, const
                                         Conv2DBackpropInput::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input_sizes = ::tensorflow::ops::AsNodeOut(scope, input_sizes);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Conv2DBackpropInput");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Conv2DBackpropInput")
                     .Input(_input_sizes)
                     .Input(_filter)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("use_cudnn_on_gpu", attrs.use_cudnn_on_gpu_)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Conv2DBackpropInput::Conv2DBackpropInput(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input input_sizes,
                                         ::tensorflow::Input filter,
                                         ::tensorflow::Input out_backprop,
                                         const gtl::ArraySlice<int>& strides,
                                         StringPiece padding)
  : Conv2DBackpropInput(scope, input_sizes, filter, out_backprop, strides, padding, Conv2DBackpropInput::Attrs()) {}

Conv3D::Conv3D(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input filter, const gtl::ArraySlice<int>& strides,
               StringPiece padding, const Conv3D::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Conv3D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Conv3D")
                     .Input(_input)
                     .Input(_filter)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Conv3D::Conv3D(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input filter, const gtl::ArraySlice<int>& strides,
               StringPiece padding)
  : Conv3D(scope, input, filter, strides, padding, Conv3D::Attrs()) {}

Conv3DBackpropFilterV2::Conv3DBackpropFilterV2(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input
                                               input, ::tensorflow::Input
                                               filter_sizes,
                                               ::tensorflow::Input
                                               out_backprop, const
                                               gtl::ArraySlice<int>& strides,
                                               StringPiece padding, const
                                               Conv3DBackpropFilterV2::Attrs&
                                               attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter_sizes = ::tensorflow::ops::AsNodeOut(scope, filter_sizes);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Conv3DBackpropFilterV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Conv3DBackpropFilterV2")
                     .Input(_input)
                     .Input(_filter_sizes)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Conv3DBackpropFilterV2::Conv3DBackpropFilterV2(const ::tensorflow::Scope&
                                               scope, ::tensorflow::Input
                                               input, ::tensorflow::Input
                                               filter_sizes,
                                               ::tensorflow::Input
                                               out_backprop, const
                                               gtl::ArraySlice<int>& strides,
                                               StringPiece padding)
  : Conv3DBackpropFilterV2(scope, input, filter_sizes, out_backprop, strides, padding, Conv3DBackpropFilterV2::Attrs()) {}

Conv3DBackpropInputV2::Conv3DBackpropInputV2(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input_sizes,
                                             ::tensorflow::Input filter,
                                             ::tensorflow::Input out_backprop,
                                             const gtl::ArraySlice<int>&
                                             strides, StringPiece padding,
                                             const
                                             Conv3DBackpropInputV2::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _input_sizes = ::tensorflow::ops::AsNodeOut(scope, input_sizes);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Conv3DBackpropInputV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Conv3DBackpropInputV2")
                     .Input(_input_sizes)
                     .Input(_filter)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Conv3DBackpropInputV2::Conv3DBackpropInputV2(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input_sizes,
                                             ::tensorflow::Input filter,
                                             ::tensorflow::Input out_backprop,
                                             const gtl::ArraySlice<int>&
                                             strides, StringPiece padding)
  : Conv3DBackpropInputV2(scope, input_sizes, filter, out_backprop, strides, padding, Conv3DBackpropInputV2::Attrs()) {}

DataFormatDimMap::DataFormatDimMap(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x, const
                                   DataFormatDimMap::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DataFormatDimMap");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DataFormatDimMap")
                     .Input(_x)
                     .Attr("src_format", attrs.src_format_)
                     .Attr("dst_format", attrs.dst_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

DataFormatDimMap::DataFormatDimMap(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x)
  : DataFormatDimMap(scope, x, DataFormatDimMap::Attrs()) {}

DataFormatVecPermute::DataFormatVecPermute(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input x, const
                                           DataFormatVecPermute::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DataFormatVecPermute");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DataFormatVecPermute")
                     .Input(_x)
                     .Attr("src_format", attrs.src_format_)
                     .Attr("dst_format", attrs.dst_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->y = Output(ret, 0);
}

DataFormatVecPermute::DataFormatVecPermute(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input x)
  : DataFormatVecPermute(scope, x, DataFormatVecPermute::Attrs()) {}

DepthwiseConv2dNative::DepthwiseConv2dNative(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input,
                                             ::tensorflow::Input filter, const
                                             gtl::ArraySlice<int>& strides,
                                             StringPiece padding, const
                                             DepthwiseConv2dNative::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DepthwiseConv2dNative");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DepthwiseConv2dNative")
                     .Input(_input)
                     .Input(_filter)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DepthwiseConv2dNative::DepthwiseConv2dNative(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input input,
                                             ::tensorflow::Input filter, const
                                             gtl::ArraySlice<int>& strides,
                                             StringPiece padding)
  : DepthwiseConv2dNative(scope, input, filter, strides, padding, DepthwiseConv2dNative::Attrs()) {}

DepthwiseConv2dNativeBackpropFilter::DepthwiseConv2dNativeBackpropFilter(const
                                                                         ::tensorflow::Scope&
                                                                         scope,
                                                                         ::tensorflow::Input
                                                                         input,
                                                                         ::tensorflow::Input
                                                                         filter_sizes,
                                                                         ::tensorflow::Input
                                                                         out_backprop,
                                                                         const
                                                                         gtl::ArraySlice<int>&
                                                                         strides,
                                                                         StringPiece
                                                                         padding,
                                                                         const
                                                                         DepthwiseConv2dNativeBackpropFilter::Attrs&
                                                                         attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter_sizes = ::tensorflow::ops::AsNodeOut(scope, filter_sizes);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DepthwiseConv2dNativeBackpropFilter");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DepthwiseConv2dNativeBackpropFilter")
                     .Input(_input)
                     .Input(_filter_sizes)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DepthwiseConv2dNativeBackpropFilter::DepthwiseConv2dNativeBackpropFilter(const
                                                                         ::tensorflow::Scope&
                                                                         scope,
                                                                         ::tensorflow::Input
                                                                         input,
                                                                         ::tensorflow::Input
                                                                         filter_sizes,
                                                                         ::tensorflow::Input
                                                                         out_backprop,
                                                                         const
                                                                         gtl::ArraySlice<int>&
                                                                         strides,
                                                                         StringPiece
                                                                         padding)
  : DepthwiseConv2dNativeBackpropFilter(scope, input, filter_sizes, out_backprop, strides, padding, DepthwiseConv2dNativeBackpropFilter::Attrs()) {}

DepthwiseConv2dNativeBackpropInput::DepthwiseConv2dNativeBackpropInput(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       input_sizes,
                                                                       ::tensorflow::Input
                                                                       filter,
                                                                       ::tensorflow::Input
                                                                       out_backprop,
                                                                       const
                                                                       gtl::ArraySlice<int>&
                                                                       strides,
                                                                       StringPiece
                                                                       padding,
                                                                       const
                                                                       DepthwiseConv2dNativeBackpropInput::Attrs&
                                                                       attrs) {
  if (!scope.ok()) return;
  auto _input_sizes = ::tensorflow::ops::AsNodeOut(scope, input_sizes);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DepthwiseConv2dNativeBackpropInput");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DepthwiseConv2dNativeBackpropInput")
                     .Input(_input_sizes)
                     .Input(_filter)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("dilations", attrs.dilations_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DepthwiseConv2dNativeBackpropInput::DepthwiseConv2dNativeBackpropInput(const
                                                                       ::tensorflow::Scope&
                                                                       scope,
                                                                       ::tensorflow::Input
                                                                       input_sizes,
                                                                       ::tensorflow::Input
                                                                       filter,
                                                                       ::tensorflow::Input
                                                                       out_backprop,
                                                                       const
                                                                       gtl::ArraySlice<int>&
                                                                       strides,
                                                                       StringPiece
                                                                       padding)
  : DepthwiseConv2dNativeBackpropInput(scope, input_sizes, filter, out_backprop, strides, padding, DepthwiseConv2dNativeBackpropInput::Attrs()) {}

Dilation2D::Dilation2D(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input filter, const
                       gtl::ArraySlice<int>& strides, const
                       gtl::ArraySlice<int>& rates, StringPiece padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Dilation2D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Dilation2D")
                     .Input(_input)
                     .Input(_filter)
                     .Attr("strides", strides)
                     .Attr("rates", rates)
                     .Attr("padding", padding)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Dilation2DBackpropFilter::Dilation2DBackpropFilter(const ::tensorflow::Scope&
                                                   scope, ::tensorflow::Input
                                                   input, ::tensorflow::Input
                                                   filter, ::tensorflow::Input
                                                   out_backprop, const
                                                   gtl::ArraySlice<int>&
                                                   strides, const
                                                   gtl::ArraySlice<int>& rates,
                                                   StringPiece padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Dilation2DBackpropFilter");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Dilation2DBackpropFilter")
                     .Input(_input)
                     .Input(_filter)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("rates", rates)
                     .Attr("padding", padding)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->filter_backprop = Output(ret, 0);
}

Dilation2DBackpropInput::Dilation2DBackpropInput(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 filter, ::tensorflow::Input
                                                 out_backprop, const
                                                 gtl::ArraySlice<int>& strides,
                                                 const gtl::ArraySlice<int>&
                                                 rates, StringPiece padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  auto _out_backprop = ::tensorflow::ops::AsNodeOut(scope, out_backprop);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Dilation2DBackpropInput");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Dilation2DBackpropInput")
                     .Input(_input)
                     .Input(_filter)
                     .Input(_out_backprop)
                     .Attr("strides", strides)
                     .Attr("rates", rates)
                     .Attr("padding", padding)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->in_backprop = Output(ret, 0);
}

Elu::Elu(const ::tensorflow::Scope& scope, ::tensorflow::Input features) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Elu");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Elu")
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->activations = Output(ret, 0);
}

FractionalAvgPool::FractionalAvgPool(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input value, const
                                     gtl::ArraySlice<float>& pooling_ratio,
                                     const FractionalAvgPool::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FractionalAvgPool");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FractionalAvgPool")
                     .Input(_value)
                     .Attr("pooling_ratio", pooling_ratio)
                     .Attr("pseudo_random", attrs.pseudo_random_)
                     .Attr("overlapping", attrs.overlapping_)
                     .Attr("deterministic", attrs.deterministic_)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
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
  this->row_pooling_sequence = Output(ret, _outputs_range["row_pooling_sequence"].first);
  this->col_pooling_sequence = Output(ret, _outputs_range["col_pooling_sequence"].first);
}

FractionalAvgPool::FractionalAvgPool(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input value, const
                                     gtl::ArraySlice<float>& pooling_ratio)
  : FractionalAvgPool(scope, value, pooling_ratio, FractionalAvgPool::Attrs()) {}

FractionalMaxPool::FractionalMaxPool(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input value, const
                                     gtl::ArraySlice<float>& pooling_ratio,
                                     const FractionalMaxPool::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _value = ::tensorflow::ops::AsNodeOut(scope, value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FractionalMaxPool");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FractionalMaxPool")
                     .Input(_value)
                     .Attr("pooling_ratio", pooling_ratio)
                     .Attr("pseudo_random", attrs.pseudo_random_)
                     .Attr("overlapping", attrs.overlapping_)
                     .Attr("deterministic", attrs.deterministic_)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
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
  this->row_pooling_sequence = Output(ret, _outputs_range["row_pooling_sequence"].first);
  this->col_pooling_sequence = Output(ret, _outputs_range["col_pooling_sequence"].first);
}

FractionalMaxPool::FractionalMaxPool(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input value, const
                                     gtl::ArraySlice<float>& pooling_ratio)
  : FractionalMaxPool(scope, value, pooling_ratio, FractionalMaxPool::Attrs()) {}

FusedBatchNorm::FusedBatchNorm(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input x, ::tensorflow::Input
                               scale, ::tensorflow::Input offset,
                               ::tensorflow::Input mean, ::tensorflow::Input
                               variance, const FusedBatchNorm::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _scale = ::tensorflow::ops::AsNodeOut(scope, scale);
  if (!scope.ok()) return;
  auto _offset = ::tensorflow::ops::AsNodeOut(scope, offset);
  if (!scope.ok()) return;
  auto _mean = ::tensorflow::ops::AsNodeOut(scope, mean);
  if (!scope.ok()) return;
  auto _variance = ::tensorflow::ops::AsNodeOut(scope, variance);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FusedBatchNorm");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FusedBatchNorm")
                     .Input(_x)
                     .Input(_scale)
                     .Input(_offset)
                     .Input(_mean)
                     .Input(_variance)
                     .Attr("epsilon", attrs.epsilon_)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("is_training", attrs.is_training_)
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
  this->batch_mean = Output(ret, _outputs_range["batch_mean"].first);
  this->batch_variance = Output(ret, _outputs_range["batch_variance"].first);
  this->reserve_space_1 = Output(ret, _outputs_range["reserve_space_1"].first);
  this->reserve_space_2 = Output(ret, _outputs_range["reserve_space_2"].first);
}

FusedBatchNorm::FusedBatchNorm(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input x, ::tensorflow::Input
                               scale, ::tensorflow::Input offset,
                               ::tensorflow::Input mean, ::tensorflow::Input
                               variance)
  : FusedBatchNorm(scope, x, scale, offset, mean, variance, FusedBatchNorm::Attrs()) {}

FusedBatchNormGrad::FusedBatchNormGrad(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input y_backprop,
                                       ::tensorflow::Input x,
                                       ::tensorflow::Input scale,
                                       ::tensorflow::Input reserve_space_1,
                                       ::tensorflow::Input reserve_space_2,
                                       const FusedBatchNormGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _y_backprop = ::tensorflow::ops::AsNodeOut(scope, y_backprop);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _scale = ::tensorflow::ops::AsNodeOut(scope, scale);
  if (!scope.ok()) return;
  auto _reserve_space_1 = ::tensorflow::ops::AsNodeOut(scope, reserve_space_1);
  if (!scope.ok()) return;
  auto _reserve_space_2 = ::tensorflow::ops::AsNodeOut(scope, reserve_space_2);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FusedBatchNormGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FusedBatchNormGrad")
                     .Input(_y_backprop)
                     .Input(_x)
                     .Input(_scale)
                     .Input(_reserve_space_1)
                     .Input(_reserve_space_2)
                     .Attr("epsilon", attrs.epsilon_)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("is_training", attrs.is_training_)
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

  this->x_backprop = Output(ret, _outputs_range["x_backprop"].first);
  this->scale_backprop = Output(ret, _outputs_range["scale_backprop"].first);
  this->offset_backprop = Output(ret, _outputs_range["offset_backprop"].first);
  this->reserve_space_3 = Output(ret, _outputs_range["reserve_space_3"].first);
  this->reserve_space_4 = Output(ret, _outputs_range["reserve_space_4"].first);
}

FusedBatchNormGrad::FusedBatchNormGrad(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input y_backprop,
                                       ::tensorflow::Input x,
                                       ::tensorflow::Input scale,
                                       ::tensorflow::Input reserve_space_1,
                                       ::tensorflow::Input reserve_space_2)
  : FusedBatchNormGrad(scope, y_backprop, x, scale, reserve_space_1, reserve_space_2, FusedBatchNormGrad::Attrs()) {}

FusedBatchNormGradV2::FusedBatchNormGradV2(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input y_backprop,
                                           ::tensorflow::Input x,
                                           ::tensorflow::Input scale,
                                           ::tensorflow::Input reserve_space_1,
                                           ::tensorflow::Input reserve_space_2,
                                           const FusedBatchNormGradV2::Attrs&
                                           attrs) {
  if (!scope.ok()) return;
  auto _y_backprop = ::tensorflow::ops::AsNodeOut(scope, y_backprop);
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _scale = ::tensorflow::ops::AsNodeOut(scope, scale);
  if (!scope.ok()) return;
  auto _reserve_space_1 = ::tensorflow::ops::AsNodeOut(scope, reserve_space_1);
  if (!scope.ok()) return;
  auto _reserve_space_2 = ::tensorflow::ops::AsNodeOut(scope, reserve_space_2);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FusedBatchNormGradV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FusedBatchNormGradV2")
                     .Input(_y_backprop)
                     .Input(_x)
                     .Input(_scale)
                     .Input(_reserve_space_1)
                     .Input(_reserve_space_2)
                     .Attr("epsilon", attrs.epsilon_)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("is_training", attrs.is_training_)
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

  this->x_backprop = Output(ret, _outputs_range["x_backprop"].first);
  this->scale_backprop = Output(ret, _outputs_range["scale_backprop"].first);
  this->offset_backprop = Output(ret, _outputs_range["offset_backprop"].first);
  this->reserve_space_3 = Output(ret, _outputs_range["reserve_space_3"].first);
  this->reserve_space_4 = Output(ret, _outputs_range["reserve_space_4"].first);
}

FusedBatchNormGradV2::FusedBatchNormGradV2(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input y_backprop,
                                           ::tensorflow::Input x,
                                           ::tensorflow::Input scale,
                                           ::tensorflow::Input reserve_space_1,
                                           ::tensorflow::Input reserve_space_2)
  : FusedBatchNormGradV2(scope, y_backprop, x, scale, reserve_space_1, reserve_space_2, FusedBatchNormGradV2::Attrs()) {}

FusedBatchNormV2::FusedBatchNormV2(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x, ::tensorflow::Input
                                   scale, ::tensorflow::Input offset,
                                   ::tensorflow::Input mean,
                                   ::tensorflow::Input variance, const
                                   FusedBatchNormV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _x = ::tensorflow::ops::AsNodeOut(scope, x);
  if (!scope.ok()) return;
  auto _scale = ::tensorflow::ops::AsNodeOut(scope, scale);
  if (!scope.ok()) return;
  auto _offset = ::tensorflow::ops::AsNodeOut(scope, offset);
  if (!scope.ok()) return;
  auto _mean = ::tensorflow::ops::AsNodeOut(scope, mean);
  if (!scope.ok()) return;
  auto _variance = ::tensorflow::ops::AsNodeOut(scope, variance);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FusedBatchNormV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FusedBatchNormV2")
                     .Input(_x)
                     .Input(_scale)
                     .Input(_offset)
                     .Input(_mean)
                     .Input(_variance)
                     .Attr("epsilon", attrs.epsilon_)
                     .Attr("data_format", attrs.data_format_)
                     .Attr("is_training", attrs.is_training_)
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
  this->batch_mean = Output(ret, _outputs_range["batch_mean"].first);
  this->batch_variance = Output(ret, _outputs_range["batch_variance"].first);
  this->reserve_space_1 = Output(ret, _outputs_range["reserve_space_1"].first);
  this->reserve_space_2 = Output(ret, _outputs_range["reserve_space_2"].first);
}

FusedBatchNormV2::FusedBatchNormV2(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input x, ::tensorflow::Input
                                   scale, ::tensorflow::Input offset,
                                   ::tensorflow::Input mean,
                                   ::tensorflow::Input variance)
  : FusedBatchNormV2(scope, x, scale, offset, mean, variance, FusedBatchNormV2::Attrs()) {}

FusedPadConv2D::FusedPadConv2D(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input, ::tensorflow::Input
                               paddings, ::tensorflow::Input filter,
                               StringPiece mode, const gtl::ArraySlice<int>&
                               strides, StringPiece padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _paddings = ::tensorflow::ops::AsNodeOut(scope, paddings);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FusedPadConv2D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FusedPadConv2D")
                     .Input(_input)
                     .Input(_paddings)
                     .Input(_filter)
                     .Attr("mode", mode)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

FusedResizeAndPadConv2D::FusedResizeAndPadConv2D(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 size, ::tensorflow::Input
                                                 paddings, ::tensorflow::Input
                                                 filter, StringPiece mode,
                                                 const gtl::ArraySlice<int>&
                                                 strides, StringPiece padding,
                                                 const
                                                 FusedResizeAndPadConv2D::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _size = ::tensorflow::ops::AsNodeOut(scope, size);
  if (!scope.ok()) return;
  auto _paddings = ::tensorflow::ops::AsNodeOut(scope, paddings);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FusedResizeAndPadConv2D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FusedResizeAndPadConv2D")
                     .Input(_input)
                     .Input(_size)
                     .Input(_paddings)
                     .Input(_filter)
                     .Attr("resize_align_corners", attrs.resize_align_corners_)
                     .Attr("mode", mode)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

FusedResizeAndPadConv2D::FusedResizeAndPadConv2D(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 input, ::tensorflow::Input
                                                 size, ::tensorflow::Input
                                                 paddings, ::tensorflow::Input
                                                 filter, StringPiece mode,
                                                 const gtl::ArraySlice<int>&
                                                 strides, StringPiece padding)
  : FusedResizeAndPadConv2D(scope, input, size, paddings, filter, mode, strides, padding, FusedResizeAndPadConv2D::Attrs()) {}

InTopK::InTopK(const ::tensorflow::Scope& scope, ::tensorflow::Input
               predictions, ::tensorflow::Input targets, int64 k) {
  if (!scope.ok()) return;
  auto _predictions = ::tensorflow::ops::AsNodeOut(scope, predictions);
  if (!scope.ok()) return;
  auto _targets = ::tensorflow::ops::AsNodeOut(scope, targets);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InTopK");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InTopK")
                     .Input(_predictions)
                     .Input(_targets)
                     .Attr("k", k)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->precision = Output(ret, 0);
}

InTopKV2::InTopKV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   predictions, ::tensorflow::Input targets,
                   ::tensorflow::Input k) {
  if (!scope.ok()) return;
  auto _predictions = ::tensorflow::ops::AsNodeOut(scope, predictions);
  if (!scope.ok()) return;
  auto _targets = ::tensorflow::ops::AsNodeOut(scope, targets);
  if (!scope.ok()) return;
  auto _k = ::tensorflow::ops::AsNodeOut(scope, k);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InTopKV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InTopKV2")
                     .Input(_predictions)
                     .Input(_targets)
                     .Input(_k)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->precision = Output(ret, 0);
}

L2Loss::L2Loss(const ::tensorflow::Scope& scope, ::tensorflow::Input t) {
  if (!scope.ok()) return;
  auto _t = ::tensorflow::ops::AsNodeOut(scope, t);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("L2Loss");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "L2Loss")
                     .Input(_t)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

LRN::LRN(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
         LRN::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LRN");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LRN")
                     .Input(_input)
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

LRN::LRN(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : LRN(scope, input, LRN::Attrs()) {}

LogSoftmax::LogSoftmax(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       logits) {
  if (!scope.ok()) return;
  auto _logits = ::tensorflow::ops::AsNodeOut(scope, logits);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LogSoftmax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LogSoftmax")
                     .Input(_logits)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->logsoftmax = Output(ret, 0);
}

MaxPool::MaxPool(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
                 const gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>&
                 strides, StringPiece padding, const MaxPool::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPool");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPool")
                     .Input(_input)
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

MaxPool::MaxPool(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
                 const gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>&
                 strides, StringPiece padding)
  : MaxPool(scope, input, ksize, strides, padding, MaxPool::Attrs()) {}

MaxPool3D::MaxPool3D(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, const gtl::ArraySlice<int>& ksize, const
                     gtl::ArraySlice<int>& strides, StringPiece padding, const
                     MaxPool3D::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPool3D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPool3D")
                     .Input(_input)
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

MaxPool3D::MaxPool3D(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, const gtl::ArraySlice<int>& ksize, const
                     gtl::ArraySlice<int>& strides, StringPiece padding)
  : MaxPool3D(scope, input, ksize, strides, padding, MaxPool3D::Attrs()) {}

MaxPool3DGrad::MaxPool3DGrad(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input orig_input,
                             ::tensorflow::Input orig_output,
                             ::tensorflow::Input grad, const
                             gtl::ArraySlice<int>& ksize, const
                             gtl::ArraySlice<int>& strides, StringPiece
                             padding, const MaxPool3DGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _orig_input = ::tensorflow::ops::AsNodeOut(scope, orig_input);
  if (!scope.ok()) return;
  auto _orig_output = ::tensorflow::ops::AsNodeOut(scope, orig_output);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPool3DGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPool3DGrad")
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

MaxPool3DGrad::MaxPool3DGrad(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input orig_input,
                             ::tensorflow::Input orig_output,
                             ::tensorflow::Input grad, const
                             gtl::ArraySlice<int>& ksize, const
                             gtl::ArraySlice<int>& strides, StringPiece
                             padding)
  : MaxPool3DGrad(scope, orig_input, orig_output, grad, ksize, strides, padding, MaxPool3DGrad::Attrs()) {}

MaxPool3DGradGrad::MaxPool3DGradGrad(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input orig_input,
                                     ::tensorflow::Input orig_output,
                                     ::tensorflow::Input grad, const
                                     gtl::ArraySlice<int>& ksize, const
                                     gtl::ArraySlice<int>& strides, StringPiece
                                     padding, const MaxPool3DGradGrad::Attrs&
                                     attrs) {
  if (!scope.ok()) return;
  auto _orig_input = ::tensorflow::ops::AsNodeOut(scope, orig_input);
  if (!scope.ok()) return;
  auto _orig_output = ::tensorflow::ops::AsNodeOut(scope, orig_output);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPool3DGradGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPool3DGradGrad")
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

MaxPool3DGradGrad::MaxPool3DGradGrad(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input orig_input,
                                     ::tensorflow::Input orig_output,
                                     ::tensorflow::Input grad, const
                                     gtl::ArraySlice<int>& ksize, const
                                     gtl::ArraySlice<int>& strides, StringPiece
                                     padding)
  : MaxPool3DGradGrad(scope, orig_input, orig_output, grad, ksize, strides, padding, MaxPool3DGradGrad::Attrs()) {}

MaxPoolGradGrad::MaxPoolGradGrad(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input orig_input,
                                 ::tensorflow::Input orig_output,
                                 ::tensorflow::Input grad, const
                                 gtl::ArraySlice<int>& ksize, const
                                 gtl::ArraySlice<int>& strides, StringPiece
                                 padding, const MaxPoolGradGrad::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _orig_input = ::tensorflow::ops::AsNodeOut(scope, orig_input);
  if (!scope.ok()) return;
  auto _orig_output = ::tensorflow::ops::AsNodeOut(scope, orig_output);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolGradGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolGradGrad")
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

MaxPoolGradGrad::MaxPoolGradGrad(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input orig_input,
                                 ::tensorflow::Input orig_output,
                                 ::tensorflow::Input grad, const
                                 gtl::ArraySlice<int>& ksize, const
                                 gtl::ArraySlice<int>& strides, StringPiece
                                 padding)
  : MaxPoolGradGrad(scope, orig_input, orig_output, grad, ksize, strides, padding, MaxPoolGradGrad::Attrs()) {}

MaxPoolGradGradV2::MaxPoolGradGradV2(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input orig_input,
                                     ::tensorflow::Input orig_output,
                                     ::tensorflow::Input grad,
                                     ::tensorflow::Input ksize,
                                     ::tensorflow::Input strides, StringPiece
                                     padding, const MaxPoolGradGradV2::Attrs&
                                     attrs) {
  if (!scope.ok()) return;
  auto _orig_input = ::tensorflow::ops::AsNodeOut(scope, orig_input);
  if (!scope.ok()) return;
  auto _orig_output = ::tensorflow::ops::AsNodeOut(scope, orig_output);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _ksize = ::tensorflow::ops::AsNodeOut(scope, ksize);
  if (!scope.ok()) return;
  auto _strides = ::tensorflow::ops::AsNodeOut(scope, strides);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolGradGradV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolGradGradV2")
                     .Input(_orig_input)
                     .Input(_orig_output)
                     .Input(_grad)
                     .Input(_ksize)
                     .Input(_strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MaxPoolGradGradV2::MaxPoolGradGradV2(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input orig_input,
                                     ::tensorflow::Input orig_output,
                                     ::tensorflow::Input grad,
                                     ::tensorflow::Input ksize,
                                     ::tensorflow::Input strides, StringPiece
                                     padding)
  : MaxPoolGradGradV2(scope, orig_input, orig_output, grad, ksize, strides, padding, MaxPoolGradGradV2::Attrs()) {}

MaxPoolGradGradWithArgmax::MaxPoolGradGradWithArgmax(const ::tensorflow::Scope&
                                                     scope, ::tensorflow::Input
                                                     input, ::tensorflow::Input
                                                     grad, ::tensorflow::Input
                                                     argmax, const
                                                     gtl::ArraySlice<int>&
                                                     ksize, const
                                                     gtl::ArraySlice<int>&
                                                     strides, StringPiece
                                                     padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _argmax = ::tensorflow::ops::AsNodeOut(scope, argmax);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolGradGradWithArgmax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolGradGradWithArgmax")
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

MaxPoolGradV2::MaxPoolGradV2(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input orig_input,
                             ::tensorflow::Input orig_output,
                             ::tensorflow::Input grad, ::tensorflow::Input
                             ksize, ::tensorflow::Input strides, StringPiece
                             padding, const MaxPoolGradV2::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _orig_input = ::tensorflow::ops::AsNodeOut(scope, orig_input);
  if (!scope.ok()) return;
  auto _orig_output = ::tensorflow::ops::AsNodeOut(scope, orig_output);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  auto _ksize = ::tensorflow::ops::AsNodeOut(scope, ksize);
  if (!scope.ok()) return;
  auto _strides = ::tensorflow::ops::AsNodeOut(scope, strides);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolGradV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolGradV2")
                     .Input(_orig_input)
                     .Input(_orig_output)
                     .Input(_grad)
                     .Input(_ksize)
                     .Input(_strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MaxPoolGradV2::MaxPoolGradV2(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input orig_input,
                             ::tensorflow::Input orig_output,
                             ::tensorflow::Input grad, ::tensorflow::Input
                             ksize, ::tensorflow::Input strides, StringPiece
                             padding)
  : MaxPoolGradV2(scope, orig_input, orig_output, grad, ksize, strides, padding, MaxPoolGradV2::Attrs()) {}

MaxPoolV2::MaxPoolV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, ::tensorflow::Input ksize, ::tensorflow::Input
                     strides, StringPiece padding, const MaxPoolV2::Attrs&
                     attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _ksize = ::tensorflow::ops::AsNodeOut(scope, ksize);
  if (!scope.ok()) return;
  auto _strides = ::tensorflow::ops::AsNodeOut(scope, strides);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolV2")
                     .Input(_input)
                     .Input(_ksize)
                     .Input(_strides)
                     .Attr("padding", padding)
                     .Attr("data_format", attrs.data_format_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MaxPoolV2::MaxPoolV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input, ::tensorflow::Input ksize, ::tensorflow::Input
                     strides, StringPiece padding)
  : MaxPoolV2(scope, input, ksize, strides, padding, MaxPoolV2::Attrs()) {}

MaxPoolWithArgmax::MaxPoolWithArgmax(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input input, const
                                     gtl::ArraySlice<int>& ksize, const
                                     gtl::ArraySlice<int>& strides, StringPiece
                                     padding, const MaxPoolWithArgmax::Attrs&
                                     attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MaxPoolWithArgmax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MaxPoolWithArgmax")
                     .Input(_input)
                     .Attr("ksize", ksize)
                     .Attr("strides", strides)
                     .Attr("Targmax", attrs.Targmax_)
                     .Attr("padding", padding)
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
  this->argmax = Output(ret, _outputs_range["argmax"].first);
}

MaxPoolWithArgmax::MaxPoolWithArgmax(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input input, const
                                     gtl::ArraySlice<int>& ksize, const
                                     gtl::ArraySlice<int>& strides, StringPiece
                                     padding)
  : MaxPoolWithArgmax(scope, input, ksize, strides, padding, MaxPoolWithArgmax::Attrs()) {}

NthElement::NthElement(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input n, const NthElement::Attrs&
                       attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _n = ::tensorflow::ops::AsNodeOut(scope, n);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("NthElement");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "NthElement")
                     .Input(_input)
                     .Input(_n)
                     .Attr("reverse", attrs.reverse_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->values = Output(ret, 0);
}

NthElement::NthElement(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, ::tensorflow::Input n)
  : NthElement(scope, input, n, NthElement::Attrs()) {}

QuantizedAvgPool::QuantizedAvgPool(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input input,
                                   ::tensorflow::Input min_input,
                                   ::tensorflow::Input max_input, const
                                   gtl::ArraySlice<int>& ksize, const
                                   gtl::ArraySlice<int>& strides, StringPiece
                                   padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _min_input = ::tensorflow::ops::AsNodeOut(scope, min_input);
  if (!scope.ok()) return;
  auto _max_input = ::tensorflow::ops::AsNodeOut(scope, max_input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedAvgPool");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedAvgPool")
                     .Input(_input)
                     .Input(_min_input)
                     .Input(_max_input)
                     .Attr("ksize", ksize)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
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
  this->min_output = Output(ret, _outputs_range["min_output"].first);
  this->max_output = Output(ret, _outputs_range["max_output"].first);
}

QuantizedBatchNormWithGlobalNormalization::QuantizedBatchNormWithGlobalNormalization(const ::tensorflow::Scope& scope, ::tensorflow::Input t, ::tensorflow::Input t_min, ::tensorflow::Input t_max, ::tensorflow::Input m, ::tensorflow::Input m_min, ::tensorflow::Input m_max, ::tensorflow::Input v, ::tensorflow::Input v_min, ::tensorflow::Input v_max, ::tensorflow::Input beta, ::tensorflow::Input beta_min, ::tensorflow::Input beta_max, ::tensorflow::Input gamma, ::tensorflow::Input gamma_min, ::tensorflow::Input gamma_max, DataType out_type, float variance_epsilon, bool
                                                                                     scale_after_normalization) {
  if (!scope.ok()) return;
  auto _t = ::tensorflow::ops::AsNodeOut(scope, t);
  if (!scope.ok()) return;
  auto _t_min = ::tensorflow::ops::AsNodeOut(scope, t_min);
  if (!scope.ok()) return;
  auto _t_max = ::tensorflow::ops::AsNodeOut(scope, t_max);
  if (!scope.ok()) return;
  auto _m = ::tensorflow::ops::AsNodeOut(scope, m);
  if (!scope.ok()) return;
  auto _m_min = ::tensorflow::ops::AsNodeOut(scope, m_min);
  if (!scope.ok()) return;
  auto _m_max = ::tensorflow::ops::AsNodeOut(scope, m_max);
  if (!scope.ok()) return;
  auto _v = ::tensorflow::ops::AsNodeOut(scope, v);
  if (!scope.ok()) return;
  auto _v_min = ::tensorflow::ops::AsNodeOut(scope, v_min);
  if (!scope.ok()) return;
  auto _v_max = ::tensorflow::ops::AsNodeOut(scope, v_max);
  if (!scope.ok()) return;
  auto _beta = ::tensorflow::ops::AsNodeOut(scope, beta);
  if (!scope.ok()) return;
  auto _beta_min = ::tensorflow::ops::AsNodeOut(scope, beta_min);
  if (!scope.ok()) return;
  auto _beta_max = ::tensorflow::ops::AsNodeOut(scope, beta_max);
  if (!scope.ok()) return;
  auto _gamma = ::tensorflow::ops::AsNodeOut(scope, gamma);
  if (!scope.ok()) return;
  auto _gamma_min = ::tensorflow::ops::AsNodeOut(scope, gamma_min);
  if (!scope.ok()) return;
  auto _gamma_max = ::tensorflow::ops::AsNodeOut(scope, gamma_max);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedBatchNormWithGlobalNormalization");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedBatchNormWithGlobalNormalization")
                     .Input(_t)
                     .Input(_t_min)
                     .Input(_t_max)
                     .Input(_m)
                     .Input(_m_min)
                     .Input(_m_max)
                     .Input(_v)
                     .Input(_v_min)
                     .Input(_v_max)
                     .Input(_beta)
                     .Input(_beta_min)
                     .Input(_beta_max)
                     .Input(_gamma)
                     .Input(_gamma_min)
                     .Input(_gamma_max)
                     .Attr("out_type", out_type)
                     .Attr("variance_epsilon", variance_epsilon)
                     .Attr("scale_after_normalization", scale_after_normalization)
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

  this->result = Output(ret, _outputs_range["result"].first);
  this->result_min = Output(ret, _outputs_range["result_min"].first);
  this->result_max = Output(ret, _outputs_range["result_max"].first);
}

QuantizedBiasAdd::QuantizedBiasAdd(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input input,
                                   ::tensorflow::Input bias,
                                   ::tensorflow::Input min_input,
                                   ::tensorflow::Input max_input,
                                   ::tensorflow::Input min_bias,
                                   ::tensorflow::Input max_bias, DataType
                                   out_type) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _bias = ::tensorflow::ops::AsNodeOut(scope, bias);
  if (!scope.ok()) return;
  auto _min_input = ::tensorflow::ops::AsNodeOut(scope, min_input);
  if (!scope.ok()) return;
  auto _max_input = ::tensorflow::ops::AsNodeOut(scope, max_input);
  if (!scope.ok()) return;
  auto _min_bias = ::tensorflow::ops::AsNodeOut(scope, min_bias);
  if (!scope.ok()) return;
  auto _max_bias = ::tensorflow::ops::AsNodeOut(scope, max_bias);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedBiasAdd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedBiasAdd")
                     .Input(_input)
                     .Input(_bias)
                     .Input(_min_input)
                     .Input(_max_input)
                     .Input(_min_bias)
                     .Input(_max_bias)
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
  this->min_out = Output(ret, _outputs_range["min_out"].first);
  this->max_out = Output(ret, _outputs_range["max_out"].first);
}

QuantizedConv2D::QuantizedConv2D(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input, ::tensorflow::Input
                                 filter, ::tensorflow::Input min_input,
                                 ::tensorflow::Input max_input,
                                 ::tensorflow::Input min_filter,
                                 ::tensorflow::Input max_filter, const
                                 gtl::ArraySlice<int>& strides, StringPiece
                                 padding, const QuantizedConv2D::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _filter = ::tensorflow::ops::AsNodeOut(scope, filter);
  if (!scope.ok()) return;
  auto _min_input = ::tensorflow::ops::AsNodeOut(scope, min_input);
  if (!scope.ok()) return;
  auto _max_input = ::tensorflow::ops::AsNodeOut(scope, max_input);
  if (!scope.ok()) return;
  auto _min_filter = ::tensorflow::ops::AsNodeOut(scope, min_filter);
  if (!scope.ok()) return;
  auto _max_filter = ::tensorflow::ops::AsNodeOut(scope, max_filter);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedConv2D");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedConv2D")
                     .Input(_input)
                     .Input(_filter)
                     .Input(_min_input)
                     .Input(_max_input)
                     .Input(_min_filter)
                     .Input(_max_filter)
                     .Attr("out_type", attrs.out_type_)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
                     .Attr("dilations", attrs.dilations_)
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
  this->min_output = Output(ret, _outputs_range["min_output"].first);
  this->max_output = Output(ret, _outputs_range["max_output"].first);
}

QuantizedConv2D::QuantizedConv2D(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input input, ::tensorflow::Input
                                 filter, ::tensorflow::Input min_input,
                                 ::tensorflow::Input max_input,
                                 ::tensorflow::Input min_filter,
                                 ::tensorflow::Input max_filter, const
                                 gtl::ArraySlice<int>& strides, StringPiece
                                 padding)
  : QuantizedConv2D(scope, input, filter, min_input, max_input, min_filter, max_filter, strides, padding, QuantizedConv2D::Attrs()) {}

QuantizedMaxPool::QuantizedMaxPool(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input input,
                                   ::tensorflow::Input min_input,
                                   ::tensorflow::Input max_input, const
                                   gtl::ArraySlice<int>& ksize, const
                                   gtl::ArraySlice<int>& strides, StringPiece
                                   padding) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _min_input = ::tensorflow::ops::AsNodeOut(scope, min_input);
  if (!scope.ok()) return;
  auto _max_input = ::tensorflow::ops::AsNodeOut(scope, max_input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedMaxPool");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedMaxPool")
                     .Input(_input)
                     .Input(_min_input)
                     .Input(_max_input)
                     .Attr("ksize", ksize)
                     .Attr("strides", strides)
                     .Attr("padding", padding)
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
  this->min_output = Output(ret, _outputs_range["min_output"].first);
  this->max_output = Output(ret, _outputs_range["max_output"].first);
}

QuantizedRelu::QuantizedRelu(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input features, ::tensorflow::Input
                             min_features, ::tensorflow::Input max_features,
                             const QuantizedRelu::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  auto _min_features = ::tensorflow::ops::AsNodeOut(scope, min_features);
  if (!scope.ok()) return;
  auto _max_features = ::tensorflow::ops::AsNodeOut(scope, max_features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedRelu");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedRelu")
                     .Input(_features)
                     .Input(_min_features)
                     .Input(_max_features)
                     .Attr("out_type", attrs.out_type_)
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

  this->activations = Output(ret, _outputs_range["activations"].first);
  this->min_activations = Output(ret, _outputs_range["min_activations"].first);
  this->max_activations = Output(ret, _outputs_range["max_activations"].first);
}

QuantizedRelu::QuantizedRelu(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input features, ::tensorflow::Input
                             min_features, ::tensorflow::Input max_features)
  : QuantizedRelu(scope, features, min_features, max_features, QuantizedRelu::Attrs()) {}

QuantizedRelu6::QuantizedRelu6(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input features,
                               ::tensorflow::Input min_features,
                               ::tensorflow::Input max_features, const
                               QuantizedRelu6::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  auto _min_features = ::tensorflow::ops::AsNodeOut(scope, min_features);
  if (!scope.ok()) return;
  auto _max_features = ::tensorflow::ops::AsNodeOut(scope, max_features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedRelu6");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedRelu6")
                     .Input(_features)
                     .Input(_min_features)
                     .Input(_max_features)
                     .Attr("out_type", attrs.out_type_)
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

  this->activations = Output(ret, _outputs_range["activations"].first);
  this->min_activations = Output(ret, _outputs_range["min_activations"].first);
  this->max_activations = Output(ret, _outputs_range["max_activations"].first);
}

QuantizedRelu6::QuantizedRelu6(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input features,
                               ::tensorflow::Input min_features,
                               ::tensorflow::Input max_features)
  : QuantizedRelu6(scope, features, min_features, max_features, QuantizedRelu6::Attrs()) {}

QuantizedReluX::QuantizedReluX(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input features,
                               ::tensorflow::Input max_value,
                               ::tensorflow::Input min_features,
                               ::tensorflow::Input max_features, const
                               QuantizedReluX::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  auto _max_value = ::tensorflow::ops::AsNodeOut(scope, max_value);
  if (!scope.ok()) return;
  auto _min_features = ::tensorflow::ops::AsNodeOut(scope, min_features);
  if (!scope.ok()) return;
  auto _max_features = ::tensorflow::ops::AsNodeOut(scope, max_features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("QuantizedReluX");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "QuantizedReluX")
                     .Input(_features)
                     .Input(_max_value)
                     .Input(_min_features)
                     .Input(_max_features)
                     .Attr("out_type", attrs.out_type_)
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

  this->activations = Output(ret, _outputs_range["activations"].first);
  this->min_activations = Output(ret, _outputs_range["min_activations"].first);
  this->max_activations = Output(ret, _outputs_range["max_activations"].first);
}

QuantizedReluX::QuantizedReluX(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input features,
                               ::tensorflow::Input max_value,
                               ::tensorflow::Input min_features,
                               ::tensorflow::Input max_features)
  : QuantizedReluX(scope, features, max_value, min_features, max_features, QuantizedReluX::Attrs()) {}

Relu::Relu(const ::tensorflow::Scope& scope, ::tensorflow::Input features) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Relu");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Relu")
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->activations = Output(ret, 0);
}

Relu6::Relu6(const ::tensorflow::Scope& scope, ::tensorflow::Input features) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Relu6");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Relu6")
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->activations = Output(ret, 0);
}

Selu::Selu(const ::tensorflow::Scope& scope, ::tensorflow::Input features) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Selu");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Selu")
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->activations = Output(ret, 0);
}

Softmax::Softmax(const ::tensorflow::Scope& scope, ::tensorflow::Input logits) {
  if (!scope.ok()) return;
  auto _logits = ::tensorflow::ops::AsNodeOut(scope, logits);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Softmax");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Softmax")
                     .Input(_logits)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->softmax = Output(ret, 0);
}

SoftmaxCrossEntropyWithLogits::SoftmaxCrossEntropyWithLogits(const
                                                             ::tensorflow::Scope&
                                                             scope,
                                                             ::tensorflow::Input
                                                             features,
                                                             ::tensorflow::Input
                                                             labels) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  auto _labels = ::tensorflow::ops::AsNodeOut(scope, labels);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SoftmaxCrossEntropyWithLogits");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SoftmaxCrossEntropyWithLogits")
                     .Input(_features)
                     .Input(_labels)
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

  this->loss = Output(ret, _outputs_range["loss"].first);
  this->backprop = Output(ret, _outputs_range["backprop"].first);
}

Softplus::Softplus(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   features) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Softplus");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Softplus")
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->activations = Output(ret, 0);
}

Softsign::Softsign(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   features) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Softsign");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Softsign")
                     .Input(_features)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->activations = Output(ret, 0);
}

SparseSoftmaxCrossEntropyWithLogits::SparseSoftmaxCrossEntropyWithLogits(const
                                                                         ::tensorflow::Scope&
                                                                         scope,
                                                                         ::tensorflow::Input
                                                                         features,
                                                                         ::tensorflow::Input
                                                                         labels) {
  if (!scope.ok()) return;
  auto _features = ::tensorflow::ops::AsNodeOut(scope, features);
  if (!scope.ok()) return;
  auto _labels = ::tensorflow::ops::AsNodeOut(scope, labels);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SparseSoftmaxCrossEntropyWithLogits");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SparseSoftmaxCrossEntropyWithLogits")
                     .Input(_features)
                     .Input(_labels)
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

  this->loss = Output(ret, _outputs_range["loss"].first);
  this->backprop = Output(ret, _outputs_range["backprop"].first);
}

TopK::TopK(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input k, const TopK::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  auto _k = ::tensorflow::ops::AsNodeOut(scope, k);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TopK");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TopKV2")
                     .Input(_input)
                     .Input(_k)
                     .Attr("sorted", attrs.sorted_)
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

  this->values = Output(ret, _outputs_range["values"].first);
  this->indices = Output(ret, _outputs_range["indices"].first);
}

TopK::TopK(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input k)
  : TopK(scope, input, k, TopK::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
