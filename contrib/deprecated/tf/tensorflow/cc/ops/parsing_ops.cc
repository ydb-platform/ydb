// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/parsing_ops.h"

namespace tensorflow {
namespace ops {

DecodeCSV::DecodeCSV(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     records, ::tensorflow::InputList record_defaults, const
                     DecodeCSV::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _records = ::tensorflow::ops::AsNodeOut(scope, records);
  if (!scope.ok()) return;
  auto _record_defaults = ::tensorflow::ops::AsNodeOutList(scope, record_defaults);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DecodeCSV");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DecodeCSV")
                     .Input(_records)
                     .Input(_record_defaults)
                     .Attr("field_delim", attrs.field_delim_)
                     .Attr("use_quote_delim", attrs.use_quote_delim_)
                     .Attr("na_value", attrs.na_value_)
                     .Attr("select_cols", attrs.select_cols_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->output.push_back(Output(ret, i));
}

DecodeCSV::DecodeCSV(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     records, ::tensorflow::InputList record_defaults)
  : DecodeCSV(scope, records, record_defaults, DecodeCSV::Attrs()) {}

DecodeCompressed::DecodeCompressed(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input bytes, const
                                   DecodeCompressed::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _bytes = ::tensorflow::ops::AsNodeOut(scope, bytes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DecodeCompressed");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DecodeCompressed")
                     .Input(_bytes)
                     .Attr("compression_type", attrs.compression_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DecodeCompressed::DecodeCompressed(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input bytes)
  : DecodeCompressed(scope, bytes, DecodeCompressed::Attrs()) {}

DecodeJSONExample::DecodeJSONExample(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input json_examples) {
  if (!scope.ok()) return;
  auto _json_examples = ::tensorflow::ops::AsNodeOut(scope, json_examples);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DecodeJSONExample");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DecodeJSONExample")
                     .Input(_json_examples)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->binary_examples = Output(ret, 0);
}

DecodeRaw::DecodeRaw(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     bytes, DataType out_type, const DecodeRaw::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _bytes = ::tensorflow::ops::AsNodeOut(scope, bytes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DecodeRaw");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DecodeRaw")
                     .Input(_bytes)
                     .Attr("out_type", out_type)
                     .Attr("little_endian", attrs.little_endian_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

DecodeRaw::DecodeRaw(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     bytes, DataType out_type)
  : DecodeRaw(scope, bytes, out_type, DecodeRaw::Attrs()) {}

ParseExample::ParseExample(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input serialized, ::tensorflow::Input
                           names, ::tensorflow::InputList sparse_keys,
                           ::tensorflow::InputList dense_keys,
                           ::tensorflow::InputList dense_defaults, const
                           DataTypeSlice& sparse_types, const
                           gtl::ArraySlice<PartialTensorShape>& dense_shapes) {
  if (!scope.ok()) return;
  auto _serialized = ::tensorflow::ops::AsNodeOut(scope, serialized);
  if (!scope.ok()) return;
  auto _names = ::tensorflow::ops::AsNodeOut(scope, names);
  if (!scope.ok()) return;
  auto _sparse_keys = ::tensorflow::ops::AsNodeOutList(scope, sparse_keys);
  if (!scope.ok()) return;
  auto _dense_keys = ::tensorflow::ops::AsNodeOutList(scope, dense_keys);
  if (!scope.ok()) return;
  auto _dense_defaults = ::tensorflow::ops::AsNodeOutList(scope, dense_defaults);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ParseExample");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ParseExample")
                     .Input(_serialized)
                     .Input(_names)
                     .Input(_sparse_keys)
                     .Input(_dense_keys)
                     .Input(_dense_defaults)
                     .Attr("sparse_types", sparse_types)
                     .Attr("dense_shapes", dense_shapes)
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

  for (int32 i = _outputs_range["sparse_indices"].first; i < _outputs_range["sparse_indices"].second; ++i)
    this->sparse_indices.push_back(Output(ret, i));
  for (int32 i = _outputs_range["sparse_values"].first; i < _outputs_range["sparse_values"].second; ++i)
    this->sparse_values.push_back(Output(ret, i));
  for (int32 i = _outputs_range["sparse_shapes"].first; i < _outputs_range["sparse_shapes"].second; ++i)
    this->sparse_shapes.push_back(Output(ret, i));
  for (int32 i = _outputs_range["dense_values"].first; i < _outputs_range["dense_values"].second; ++i)
    this->dense_values.push_back(Output(ret, i));
}

ParseSingleExample::ParseSingleExample(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input serialized,
                                       ::tensorflow::InputList dense_defaults,
                                       int64 num_sparse, const
                                       gtl::ArraySlice<string>& sparse_keys,
                                       const gtl::ArraySlice<string>&
                                       dense_keys, const DataTypeSlice&
                                       sparse_types, const
                                       gtl::ArraySlice<PartialTensorShape>&
                                       dense_shapes) {
  if (!scope.ok()) return;
  auto _serialized = ::tensorflow::ops::AsNodeOut(scope, serialized);
  if (!scope.ok()) return;
  auto _dense_defaults = ::tensorflow::ops::AsNodeOutList(scope, dense_defaults);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ParseSingleExample");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ParseSingleExample")
                     .Input(_serialized)
                     .Input(_dense_defaults)
                     .Attr("num_sparse", num_sparse)
                     .Attr("sparse_keys", sparse_keys)
                     .Attr("dense_keys", dense_keys)
                     .Attr("sparse_types", sparse_types)
                     .Attr("dense_shapes", dense_shapes)
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

  for (int32 i = _outputs_range["sparse_indices"].first; i < _outputs_range["sparse_indices"].second; ++i)
    this->sparse_indices.push_back(Output(ret, i));
  for (int32 i = _outputs_range["sparse_values"].first; i < _outputs_range["sparse_values"].second; ++i)
    this->sparse_values.push_back(Output(ret, i));
  for (int32 i = _outputs_range["sparse_shapes"].first; i < _outputs_range["sparse_shapes"].second; ++i)
    this->sparse_shapes.push_back(Output(ret, i));
  for (int32 i = _outputs_range["dense_values"].first; i < _outputs_range["dense_values"].second; ++i)
    this->dense_values.push_back(Output(ret, i));
}

ParseSingleSequenceExample::ParseSingleSequenceExample(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input
                                                       serialized,
                                                       ::tensorflow::Input
                                                       feature_list_dense_missing_assumed_empty,
                                                       ::tensorflow::InputList
                                                       context_sparse_keys,
                                                       ::tensorflow::InputList
                                                       context_dense_keys,
                                                       ::tensorflow::InputList
                                                       feature_list_sparse_keys,
                                                       ::tensorflow::InputList
                                                       feature_list_dense_keys,
                                                       ::tensorflow::InputList
                                                       context_dense_defaults,
                                                       ::tensorflow::Input
                                                       debug_name, const
                                                       ParseSingleSequenceExample::Attrs&
                                                       attrs) {
  if (!scope.ok()) return;
  auto _serialized = ::tensorflow::ops::AsNodeOut(scope, serialized);
  if (!scope.ok()) return;
  auto _feature_list_dense_missing_assumed_empty = ::tensorflow::ops::AsNodeOut(scope, feature_list_dense_missing_assumed_empty);
  if (!scope.ok()) return;
  auto _context_sparse_keys = ::tensorflow::ops::AsNodeOutList(scope, context_sparse_keys);
  if (!scope.ok()) return;
  auto _context_dense_keys = ::tensorflow::ops::AsNodeOutList(scope, context_dense_keys);
  if (!scope.ok()) return;
  auto _feature_list_sparse_keys = ::tensorflow::ops::AsNodeOutList(scope, feature_list_sparse_keys);
  if (!scope.ok()) return;
  auto _feature_list_dense_keys = ::tensorflow::ops::AsNodeOutList(scope, feature_list_dense_keys);
  if (!scope.ok()) return;
  auto _context_dense_defaults = ::tensorflow::ops::AsNodeOutList(scope, context_dense_defaults);
  if (!scope.ok()) return;
  auto _debug_name = ::tensorflow::ops::AsNodeOut(scope, debug_name);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ParseSingleSequenceExample");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ParseSingleSequenceExample")
                     .Input(_serialized)
                     .Input(_feature_list_dense_missing_assumed_empty)
                     .Input(_context_sparse_keys)
                     .Input(_context_dense_keys)
                     .Input(_feature_list_sparse_keys)
                     .Input(_feature_list_dense_keys)
                     .Input(_context_dense_defaults)
                     .Input(_debug_name)
                     .Attr("context_sparse_types", attrs.context_sparse_types_)
                     .Attr("feature_list_dense_types", attrs.feature_list_dense_types_)
                     .Attr("context_dense_shapes", attrs.context_dense_shapes_)
                     .Attr("feature_list_sparse_types", attrs.feature_list_sparse_types_)
                     .Attr("feature_list_dense_shapes", attrs.feature_list_dense_shapes_)
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

  for (int32 i = _outputs_range["context_sparse_indices"].first; i < _outputs_range["context_sparse_indices"].second; ++i)
    this->context_sparse_indices.push_back(Output(ret, i));
  for (int32 i = _outputs_range["context_sparse_values"].first; i < _outputs_range["context_sparse_values"].second; ++i)
    this->context_sparse_values.push_back(Output(ret, i));
  for (int32 i = _outputs_range["context_sparse_shapes"].first; i < _outputs_range["context_sparse_shapes"].second; ++i)
    this->context_sparse_shapes.push_back(Output(ret, i));
  for (int32 i = _outputs_range["context_dense_values"].first; i < _outputs_range["context_dense_values"].second; ++i)
    this->context_dense_values.push_back(Output(ret, i));
  for (int32 i = _outputs_range["feature_list_sparse_indices"].first; i < _outputs_range["feature_list_sparse_indices"].second; ++i)
    this->feature_list_sparse_indices.push_back(Output(ret, i));
  for (int32 i = _outputs_range["feature_list_sparse_values"].first; i < _outputs_range["feature_list_sparse_values"].second; ++i)
    this->feature_list_sparse_values.push_back(Output(ret, i));
  for (int32 i = _outputs_range["feature_list_sparse_shapes"].first; i < _outputs_range["feature_list_sparse_shapes"].second; ++i)
    this->feature_list_sparse_shapes.push_back(Output(ret, i));
  for (int32 i = _outputs_range["feature_list_dense_values"].first; i < _outputs_range["feature_list_dense_values"].second; ++i)
    this->feature_list_dense_values.push_back(Output(ret, i));
}

ParseSingleSequenceExample::ParseSingleSequenceExample(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input
                                                       serialized,
                                                       ::tensorflow::Input
                                                       feature_list_dense_missing_assumed_empty,
                                                       ::tensorflow::InputList
                                                       context_sparse_keys,
                                                       ::tensorflow::InputList
                                                       context_dense_keys,
                                                       ::tensorflow::InputList
                                                       feature_list_sparse_keys,
                                                       ::tensorflow::InputList
                                                       feature_list_dense_keys,
                                                       ::tensorflow::InputList
                                                       context_dense_defaults,
                                                       ::tensorflow::Input
                                                       debug_name)
  : ParseSingleSequenceExample(scope, serialized, feature_list_dense_missing_assumed_empty, context_sparse_keys, context_dense_keys, feature_list_sparse_keys, feature_list_dense_keys, context_dense_defaults, debug_name, ParseSingleSequenceExample::Attrs()) {}

ParseTensor::ParseTensor(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         serialized, DataType out_type) {
  if (!scope.ok()) return;
  auto _serialized = ::tensorflow::ops::AsNodeOut(scope, serialized);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ParseTensor");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ParseTensor")
                     .Input(_serialized)
                     .Attr("out_type", out_type)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

SerializeTensor::SerializeTensor(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input tensor) {
  if (!scope.ok()) return;
  auto _tensor = ::tensorflow::ops::AsNodeOut(scope, tensor);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SerializeTensor");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SerializeTensor")
                     .Input(_tensor)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->serialized = Output(ret, 0);
}

StringToNumber::StringToNumber(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input string_tensor, const
                               StringToNumber::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _string_tensor = ::tensorflow::ops::AsNodeOut(scope, string_tensor);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("StringToNumber");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "StringToNumber")
                     .Input(_string_tensor)
                     .Attr("out_type", attrs.out_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

StringToNumber::StringToNumber(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input string_tensor)
  : StringToNumber(scope, string_tensor, StringToNumber::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
