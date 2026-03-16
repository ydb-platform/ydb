// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/io_ops.h"

namespace tensorflow {
namespace ops {

FixedLengthRecordReader::FixedLengthRecordReader(const ::tensorflow::Scope&
                                                 scope, int64 record_bytes,
                                                 const
                                                 FixedLengthRecordReader::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FixedLengthRecordReader");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FixedLengthRecordReaderV2")
                     .Attr("header_bytes", attrs.header_bytes_)
                     .Attr("record_bytes", record_bytes)
                     .Attr("footer_bytes", attrs.footer_bytes_)
                     .Attr("hop_bytes", attrs.hop_bytes_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
                     .Attr("encoding", attrs.encoding_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->reader_handle = Output(ret, 0);
}

FixedLengthRecordReader::FixedLengthRecordReader(const ::tensorflow::Scope&
                                                 scope, int64 record_bytes)
  : FixedLengthRecordReader(scope, record_bytes, FixedLengthRecordReader::Attrs()) {}

IdentityReader::IdentityReader(const ::tensorflow::Scope& scope, const
                               IdentityReader::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("IdentityReader");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "IdentityReaderV2")
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->reader_handle = Output(ret, 0);
}

IdentityReader::IdentityReader(const ::tensorflow::Scope& scope)
  : IdentityReader(scope, IdentityReader::Attrs()) {}

LMDBReader::LMDBReader(const ::tensorflow::Scope& scope, const
                       LMDBReader::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LMDBReader");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LMDBReader")
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->reader_handle = Output(ret, 0);
}

LMDBReader::LMDBReader(const ::tensorflow::Scope& scope)
  : LMDBReader(scope, LMDBReader::Attrs()) {}

MatchingFiles::MatchingFiles(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input pattern) {
  if (!scope.ok()) return;
  auto _pattern = ::tensorflow::ops::AsNodeOut(scope, pattern);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatchingFiles");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatchingFiles")
                     .Input(_pattern)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->filenames = Output(ret, 0);
}

MergeV2Checkpoints::MergeV2Checkpoints(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input checkpoint_prefixes,
                                       ::tensorflow::Input destination_prefix,
                                       const MergeV2Checkpoints::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _checkpoint_prefixes = ::tensorflow::ops::AsNodeOut(scope, checkpoint_prefixes);
  if (!scope.ok()) return;
  auto _destination_prefix = ::tensorflow::ops::AsNodeOut(scope, destination_prefix);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MergeV2Checkpoints");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MergeV2Checkpoints")
                     .Input(_checkpoint_prefixes)
                     .Input(_destination_prefix)
                     .Attr("delete_old_dirs", attrs.delete_old_dirs_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

MergeV2Checkpoints::MergeV2Checkpoints(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input checkpoint_prefixes,
                                       ::tensorflow::Input destination_prefix)
  : MergeV2Checkpoints(scope, checkpoint_prefixes, destination_prefix, MergeV2Checkpoints::Attrs()) {}

ReadFile::ReadFile(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   filename) {
  if (!scope.ok()) return;
  auto _filename = ::tensorflow::ops::AsNodeOut(scope, filename);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReadFile");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReadFile")
                     .Input(_filename)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->contents = Output(ret, 0);
}

ReaderNumRecordsProduced::ReaderNumRecordsProduced(const ::tensorflow::Scope&
                                                   scope, ::tensorflow::Input
                                                   reader_handle) {
  if (!scope.ok()) return;
  auto _reader_handle = ::tensorflow::ops::AsNodeOut(scope, reader_handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReaderNumRecordsProduced");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReaderNumRecordsProducedV2")
                     .Input(_reader_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->records_produced = Output(ret, 0);
}

ReaderNumWorkUnitsCompleted::ReaderNumWorkUnitsCompleted(const
                                                         ::tensorflow::Scope&
                                                         scope,
                                                         ::tensorflow::Input
                                                         reader_handle) {
  if (!scope.ok()) return;
  auto _reader_handle = ::tensorflow::ops::AsNodeOut(scope, reader_handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReaderNumWorkUnitsCompleted");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReaderNumWorkUnitsCompletedV2")
                     .Input(_reader_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->units_completed = Output(ret, 0);
}

ReaderReadUpTo::ReaderReadUpTo(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input reader_handle,
                               ::tensorflow::Input queue_handle,
                               ::tensorflow::Input num_records) {
  if (!scope.ok()) return;
  auto _reader_handle = ::tensorflow::ops::AsNodeOut(scope, reader_handle);
  if (!scope.ok()) return;
  auto _queue_handle = ::tensorflow::ops::AsNodeOut(scope, queue_handle);
  if (!scope.ok()) return;
  auto _num_records = ::tensorflow::ops::AsNodeOut(scope, num_records);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReaderReadUpTo");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReaderReadUpToV2")
                     .Input(_reader_handle)
                     .Input(_queue_handle)
                     .Input(_num_records)
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

  this->keys = Output(ret, _outputs_range["keys"].first);
  this->values = Output(ret, _outputs_range["values"].first);
}

ReaderRead::ReaderRead(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       reader_handle, ::tensorflow::Input queue_handle) {
  if (!scope.ok()) return;
  auto _reader_handle = ::tensorflow::ops::AsNodeOut(scope, reader_handle);
  if (!scope.ok()) return;
  auto _queue_handle = ::tensorflow::ops::AsNodeOut(scope, queue_handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReaderRead");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReaderReadV2")
                     .Input(_reader_handle)
                     .Input(_queue_handle)
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

  this->key = Output(ret, _outputs_range["key"].first);
  this->value = Output(ret, _outputs_range["value"].first);
}

ReaderReset::ReaderReset(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         reader_handle) {
  if (!scope.ok()) return;
  auto _reader_handle = ::tensorflow::ops::AsNodeOut(scope, reader_handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReaderReset");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReaderResetV2")
                     .Input(_reader_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ReaderRestoreState::ReaderRestoreState(const ::tensorflow::Scope& scope,
                                       ::tensorflow::Input reader_handle,
                                       ::tensorflow::Input state) {
  if (!scope.ok()) return;
  auto _reader_handle = ::tensorflow::ops::AsNodeOut(scope, reader_handle);
  if (!scope.ok()) return;
  auto _state = ::tensorflow::ops::AsNodeOut(scope, state);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReaderRestoreState");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReaderRestoreStateV2")
                     .Input(_reader_handle)
                     .Input(_state)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ReaderSerializeState::ReaderSerializeState(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input reader_handle) {
  if (!scope.ok()) return;
  auto _reader_handle = ::tensorflow::ops::AsNodeOut(scope, reader_handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ReaderSerializeState");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ReaderSerializeStateV2")
                     .Input(_reader_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->state = Output(ret, 0);
}

Restore::Restore(const ::tensorflow::Scope& scope, ::tensorflow::Input
                 file_pattern, ::tensorflow::Input tensor_name, DataType dt,
                 const Restore::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _file_pattern = ::tensorflow::ops::AsNodeOut(scope, file_pattern);
  if (!scope.ok()) return;
  auto _tensor_name = ::tensorflow::ops::AsNodeOut(scope, tensor_name);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Restore");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Restore")
                     .Input(_file_pattern)
                     .Input(_tensor_name)
                     .Attr("dt", dt)
                     .Attr("preferred_shard", attrs.preferred_shard_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->tensor = Output(ret, 0);
}

Restore::Restore(const ::tensorflow::Scope& scope, ::tensorflow::Input
                 file_pattern, ::tensorflow::Input tensor_name, DataType dt)
  : Restore(scope, file_pattern, tensor_name, dt, Restore::Attrs()) {}

RestoreSlice::RestoreSlice(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input file_pattern,
                           ::tensorflow::Input tensor_name, ::tensorflow::Input
                           shape_and_slice, DataType dt, const
                           RestoreSlice::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _file_pattern = ::tensorflow::ops::AsNodeOut(scope, file_pattern);
  if (!scope.ok()) return;
  auto _tensor_name = ::tensorflow::ops::AsNodeOut(scope, tensor_name);
  if (!scope.ok()) return;
  auto _shape_and_slice = ::tensorflow::ops::AsNodeOut(scope, shape_and_slice);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RestoreSlice");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RestoreSlice")
                     .Input(_file_pattern)
                     .Input(_tensor_name)
                     .Input(_shape_and_slice)
                     .Attr("dt", dt)
                     .Attr("preferred_shard", attrs.preferred_shard_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->tensor = Output(ret, 0);
}

RestoreSlice::RestoreSlice(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input file_pattern,
                           ::tensorflow::Input tensor_name, ::tensorflow::Input
                           shape_and_slice, DataType dt)
  : RestoreSlice(scope, file_pattern, tensor_name, shape_and_slice, dt, RestoreSlice::Attrs()) {}

RestoreV2::RestoreV2(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     prefix, ::tensorflow::Input tensor_names,
                     ::tensorflow::Input shape_and_slices, const DataTypeSlice&
                     dtypes) {
  if (!scope.ok()) return;
  auto _prefix = ::tensorflow::ops::AsNodeOut(scope, prefix);
  if (!scope.ok()) return;
  auto _tensor_names = ::tensorflow::ops::AsNodeOut(scope, tensor_names);
  if (!scope.ok()) return;
  auto _shape_and_slices = ::tensorflow::ops::AsNodeOut(scope, shape_and_slices);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("RestoreV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "RestoreV2")
                     .Input(_prefix)
                     .Input(_tensor_names)
                     .Input(_shape_and_slices)
                     .Attr("dtypes", dtypes)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  for (int32 i = 0; i < ret->num_outputs(); ++i)
    this->tensors.push_back(Output(ret, i));
}

Save::Save(const ::tensorflow::Scope& scope, ::tensorflow::Input filename,
           ::tensorflow::Input tensor_names, ::tensorflow::InputList data) {
  if (!scope.ok()) return;
  auto _filename = ::tensorflow::ops::AsNodeOut(scope, filename);
  if (!scope.ok()) return;
  auto _tensor_names = ::tensorflow::ops::AsNodeOut(scope, tensor_names);
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOutList(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Save");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Save")
                     .Input(_filename)
                     .Input(_tensor_names)
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

SaveSlices::SaveSlices(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       filename, ::tensorflow::Input tensor_names,
                       ::tensorflow::Input shapes_and_slices,
                       ::tensorflow::InputList data) {
  if (!scope.ok()) return;
  auto _filename = ::tensorflow::ops::AsNodeOut(scope, filename);
  if (!scope.ok()) return;
  auto _tensor_names = ::tensorflow::ops::AsNodeOut(scope, tensor_names);
  if (!scope.ok()) return;
  auto _shapes_and_slices = ::tensorflow::ops::AsNodeOut(scope, shapes_and_slices);
  if (!scope.ok()) return;
  auto _data = ::tensorflow::ops::AsNodeOutList(scope, data);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SaveSlices");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SaveSlices")
                     .Input(_filename)
                     .Input(_tensor_names)
                     .Input(_shapes_and_slices)
                     .Input(_data)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

SaveV2::SaveV2(const ::tensorflow::Scope& scope, ::tensorflow::Input prefix,
               ::tensorflow::Input tensor_names, ::tensorflow::Input
               shape_and_slices, ::tensorflow::InputList tensors) {
  if (!scope.ok()) return;
  auto _prefix = ::tensorflow::ops::AsNodeOut(scope, prefix);
  if (!scope.ok()) return;
  auto _tensor_names = ::tensorflow::ops::AsNodeOut(scope, tensor_names);
  if (!scope.ok()) return;
  auto _shape_and_slices = ::tensorflow::ops::AsNodeOut(scope, shape_and_slices);
  if (!scope.ok()) return;
  auto _tensors = ::tensorflow::ops::AsNodeOutList(scope, tensors);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SaveV2");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SaveV2")
                     .Input(_prefix)
                     .Input(_tensor_names)
                     .Input(_shape_and_slices)
                     .Input(_tensors)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

ShardedFilename::ShardedFilename(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input basename,
                                 ::tensorflow::Input shard, ::tensorflow::Input
                                 num_shards) {
  if (!scope.ok()) return;
  auto _basename = ::tensorflow::ops::AsNodeOut(scope, basename);
  if (!scope.ok()) return;
  auto _shard = ::tensorflow::ops::AsNodeOut(scope, shard);
  if (!scope.ok()) return;
  auto _num_shards = ::tensorflow::ops::AsNodeOut(scope, num_shards);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ShardedFilename");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ShardedFilename")
                     .Input(_basename)
                     .Input(_shard)
                     .Input(_num_shards)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->filename = Output(ret, 0);
}

ShardedFilespec::ShardedFilespec(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input basename,
                                 ::tensorflow::Input num_shards) {
  if (!scope.ok()) return;
  auto _basename = ::tensorflow::ops::AsNodeOut(scope, basename);
  if (!scope.ok()) return;
  auto _num_shards = ::tensorflow::ops::AsNodeOut(scope, num_shards);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ShardedFilespec");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ShardedFilespec")
                     .Input(_basename)
                     .Input(_num_shards)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->filename = Output(ret, 0);
}

TFRecordReader::TFRecordReader(const ::tensorflow::Scope& scope, const
                               TFRecordReader::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TFRecordReader");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TFRecordReaderV2")
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
                     .Attr("compression_type", attrs.compression_type_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->reader_handle = Output(ret, 0);
}

TFRecordReader::TFRecordReader(const ::tensorflow::Scope& scope)
  : TFRecordReader(scope, TFRecordReader::Attrs()) {}

TextLineReader::TextLineReader(const ::tensorflow::Scope& scope, const
                               TextLineReader::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("TextLineReader");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "TextLineReaderV2")
                     .Attr("skip_header_lines", attrs.skip_header_lines_)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->reader_handle = Output(ret, 0);
}

TextLineReader::TextLineReader(const ::tensorflow::Scope& scope)
  : TextLineReader(scope, TextLineReader::Attrs()) {}

WholeFileReader::WholeFileReader(const ::tensorflow::Scope& scope, const
                                 WholeFileReader::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("WholeFileReader");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "WholeFileReaderV2")
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->reader_handle = Output(ret, 0);
}

WholeFileReader::WholeFileReader(const ::tensorflow::Scope& scope)
  : WholeFileReader(scope, WholeFileReader::Attrs()) {}

WriteFile::WriteFile(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     filename, ::tensorflow::Input contents) {
  if (!scope.ok()) return;
  auto _filename = ::tensorflow::ops::AsNodeOut(scope, filename);
  if (!scope.ok()) return;
  auto _contents = ::tensorflow::ops::AsNodeOut(scope, contents);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("WriteFile");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "WriteFile")
                     .Input(_filename)
                     .Input(_contents)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

/// @}

}  // namespace ops
}  // namespace tensorflow
