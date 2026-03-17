// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/lookup_ops.h"

namespace tensorflow {
namespace ops {

HashTable::HashTable(const ::tensorflow::Scope& scope, DataType key_dtype,
                     DataType value_dtype, const HashTable::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("HashTable");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "HashTableV2")
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
                     .Attr("use_node_name_sharing", attrs.use_node_name_sharing_)
                     .Attr("key_dtype", key_dtype)
                     .Attr("value_dtype", value_dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->table_handle = Output(ret, 0);
}

HashTable::HashTable(const ::tensorflow::Scope& scope, DataType key_dtype,
                     DataType value_dtype)
  : HashTable(scope, key_dtype, value_dtype, HashTable::Attrs()) {}

InitializeTableFromTextFile::InitializeTableFromTextFile(const
                                                         ::tensorflow::Scope&
                                                         scope,
                                                         ::tensorflow::Input
                                                         table_handle,
                                                         ::tensorflow::Input
                                                         filename, int64
                                                         key_index, int64
                                                         value_index, const
                                                         InitializeTableFromTextFile::Attrs&
                                                         attrs) {
  if (!scope.ok()) return;
  auto _table_handle = ::tensorflow::ops::AsNodeOut(scope, table_handle);
  if (!scope.ok()) return;
  auto _filename = ::tensorflow::ops::AsNodeOut(scope, filename);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InitializeTableFromTextFile");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InitializeTableFromTextFileV2")
                     .Input(_table_handle)
                     .Input(_filename)
                     .Attr("key_index", key_index)
                     .Attr("value_index", value_index)
                     .Attr("vocab_size", attrs.vocab_size_)
                     .Attr("delimiter", attrs.delimiter_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

InitializeTableFromTextFile::InitializeTableFromTextFile(const
                                                         ::tensorflow::Scope&
                                                         scope,
                                                         ::tensorflow::Input
                                                         table_handle,
                                                         ::tensorflow::Input
                                                         filename, int64
                                                         key_index, int64
                                                         value_index)
  : InitializeTableFromTextFile(scope, table_handle, filename, key_index, value_index, InitializeTableFromTextFile::Attrs()) {}

InitializeTable::InitializeTable(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input table_handle,
                                 ::tensorflow::Input keys, ::tensorflow::Input
                                 values) {
  if (!scope.ok()) return;
  auto _table_handle = ::tensorflow::ops::AsNodeOut(scope, table_handle);
  if (!scope.ok()) return;
  auto _keys = ::tensorflow::ops::AsNodeOut(scope, keys);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("InitializeTable");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "InitializeTableV2")
                     .Input(_table_handle)
                     .Input(_keys)
                     .Input(_values)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

LookupTableExport::LookupTableExport(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input table_handle, DataType
                                     Tkeys, DataType Tvalues) {
  if (!scope.ok()) return;
  auto _table_handle = ::tensorflow::ops::AsNodeOut(scope, table_handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LookupTableExport");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LookupTableExportV2")
                     .Input(_table_handle)
                     .Attr("Tkeys", Tkeys)
                     .Attr("Tvalues", Tvalues)
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

LookupTableFind::LookupTableFind(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input table_handle,
                                 ::tensorflow::Input keys, ::tensorflow::Input
                                 default_value) {
  if (!scope.ok()) return;
  auto _table_handle = ::tensorflow::ops::AsNodeOut(scope, table_handle);
  if (!scope.ok()) return;
  auto _keys = ::tensorflow::ops::AsNodeOut(scope, keys);
  if (!scope.ok()) return;
  auto _default_value = ::tensorflow::ops::AsNodeOut(scope, default_value);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LookupTableFind");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LookupTableFindV2")
                     .Input(_table_handle)
                     .Input(_keys)
                     .Input(_default_value)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->values = Output(ret, 0);
}

LookupTableImport::LookupTableImport(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input table_handle,
                                     ::tensorflow::Input keys,
                                     ::tensorflow::Input values) {
  if (!scope.ok()) return;
  auto _table_handle = ::tensorflow::ops::AsNodeOut(scope, table_handle);
  if (!scope.ok()) return;
  auto _keys = ::tensorflow::ops::AsNodeOut(scope, keys);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LookupTableImport");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LookupTableImportV2")
                     .Input(_table_handle)
                     .Input(_keys)
                     .Input(_values)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

LookupTableInsert::LookupTableInsert(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input table_handle,
                                     ::tensorflow::Input keys,
                                     ::tensorflow::Input values) {
  if (!scope.ok()) return;
  auto _table_handle = ::tensorflow::ops::AsNodeOut(scope, table_handle);
  if (!scope.ok()) return;
  auto _keys = ::tensorflow::ops::AsNodeOut(scope, keys);
  if (!scope.ok()) return;
  auto _values = ::tensorflow::ops::AsNodeOut(scope, values);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LookupTableInsert");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LookupTableInsertV2")
                     .Input(_table_handle)
                     .Input(_keys)
                     .Input(_values)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->operation = Operation(ret);
  return;
}

LookupTableSize::LookupTableSize(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input table_handle) {
  if (!scope.ok()) return;
  auto _table_handle = ::tensorflow::ops::AsNodeOut(scope, table_handle);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LookupTableSize");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LookupTableSizeV2")
                     .Input(_table_handle)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->size = Output(ret, 0);
}

MutableDenseHashTable::MutableDenseHashTable(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input empty_key,
                                             DataType value_dtype, const
                                             MutableDenseHashTable::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _empty_key = ::tensorflow::ops::AsNodeOut(scope, empty_key);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MutableDenseHashTable");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MutableDenseHashTableV2")
                     .Input(_empty_key)
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
                     .Attr("use_node_name_sharing", attrs.use_node_name_sharing_)
                     .Attr("value_dtype", value_dtype)
                     .Attr("value_shape", attrs.value_shape_)
                     .Attr("initial_num_buckets", attrs.initial_num_buckets_)
                     .Attr("max_load_factor", attrs.max_load_factor_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->table_handle = Output(ret, 0);
}

MutableDenseHashTable::MutableDenseHashTable(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input empty_key,
                                             DataType value_dtype)
  : MutableDenseHashTable(scope, empty_key, value_dtype, MutableDenseHashTable::Attrs()) {}

MutableHashTableOfTensors::MutableHashTableOfTensors(const ::tensorflow::Scope&
                                                     scope, DataType key_dtype,
                                                     DataType value_dtype,
                                                     const
                                                     MutableHashTableOfTensors::Attrs&
                                                     attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MutableHashTableOfTensors");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MutableHashTableOfTensorsV2")
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
                     .Attr("use_node_name_sharing", attrs.use_node_name_sharing_)
                     .Attr("key_dtype", key_dtype)
                     .Attr("value_dtype", value_dtype)
                     .Attr("value_shape", attrs.value_shape_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->table_handle = Output(ret, 0);
}

MutableHashTableOfTensors::MutableHashTableOfTensors(const ::tensorflow::Scope&
                                                     scope, DataType key_dtype,
                                                     DataType value_dtype)
  : MutableHashTableOfTensors(scope, key_dtype, value_dtype, MutableHashTableOfTensors::Attrs()) {}

MutableHashTable::MutableHashTable(const ::tensorflow::Scope& scope, DataType
                                   key_dtype, DataType value_dtype, const
                                   MutableHashTable::Attrs& attrs) {
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MutableHashTable");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MutableHashTableV2")
                     .Attr("container", attrs.container_)
                     .Attr("shared_name", attrs.shared_name_)
                     .Attr("use_node_name_sharing", attrs.use_node_name_sharing_)
                     .Attr("key_dtype", key_dtype)
                     .Attr("value_dtype", value_dtype)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->table_handle = Output(ret, 0);
}

MutableHashTable::MutableHashTable(const ::tensorflow::Scope& scope, DataType
                                   key_dtype, DataType value_dtype)
  : MutableHashTable(scope, key_dtype, value_dtype, MutableHashTable::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
