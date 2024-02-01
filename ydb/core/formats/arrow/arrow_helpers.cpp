#include "arrow_helpers.h"
#include "switch_type.h"
#include "one_batch_input_stream.h"
#include "common/validation.h"
#include "merging_sorted_input_stream.h"
#include "permutations.h"
#include "serializer/native.h"
#include "serializer/abstract.h"
#include "serializer/stream.h"
#include "simple_arrays_cache.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/services/services.pb.h>

#include <util/system/yassert.h>
#include <util/string/join.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/library/actors/core/log.h>
#include <memory>

#define Y_VERIFY_OK(status) Y_ABORT_UNLESS(status.ok(), "%s", status.ToString().c_str())

namespace NKikimr::NArrow {

template <typename TType>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl() {
    return std::make_shared<TType>();
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::Decimal128Type>() {
    return arrow::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::TimestampType>() {
    return arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO);
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::DurationType>() {
    return arrow::duration(arrow::TimeUnit::TimeUnit::MICRO);
}

std::shared_ptr<arrow::DataType> GetArrowType(NScheme::TTypeInfo typeId) {
    std::shared_ptr<arrow::DataType> result;
    bool success = SwitchYqlTypeToArrowType(typeId, [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
        Y_UNUSED(typeHolder);
        result = CreateEmptyArrowImpl<TType>();
        return true;
    });
    if (success) {
        return result;
    }
    return std::make_shared<arrow::NullType>();
}

std::shared_ptr<arrow::DataType> GetCSVArrowType(NScheme::TTypeInfo typeId) {
    std::shared_ptr<arrow::DataType> result;
    switch (typeId.GetTypeId()) {
        case NScheme::NTypeIds::Datetime:
            return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::SECOND);
        case NScheme::NTypeIds::Timestamp:
            return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO);
        case NScheme::NTypeIds::Date:
            return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::SECOND);
        default:
            return GetArrowType(typeId);
    }
}

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, const std::set<std::string>& notNullColumns) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columns.size());
    for (auto& [name, ydbType] : columns) {
        std::string colName(name.data(), name.size());
        fields.emplace_back(std::make_shared<arrow::Field>(colName, GetArrowType(ydbType), !notNullColumns.contains(colName)));
    }
    return fields;
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbColumns, const std::set<std::string>& notNullColumns) {
    return std::make_shared<arrow::Schema>(MakeArrowFields(ydbColumns, notNullColumns));
}

TString SerializeSchema(const arrow::Schema& schema) {
    auto buffer = TStatusValidator::GetValid(arrow::ipc::SerializeSchema(schema));
    return buffer->ToString();
}

std::shared_ptr<arrow::Schema> DeserializeSchema(const TString& str) {
    std::shared_ptr<arrow::Buffer> buffer(std::make_shared<NSerialization::TBufferOverString>(str));
    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo dictMemo;
    auto schema = ReadSchema(&reader, &dictMemo);
    if (!schema.ok()) {
        return {};
    }
    return *schema;
}

TString SerializeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const arrow::ipc::IpcWriteOptions& options) {
    return NSerialization::TNativeSerializer(options).SerializePayload(batch);
}

TString SerializeBatchNoCompression(const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto writeOptions = arrow::ipc::IpcWriteOptions::Defaults();
    writeOptions.use_threads = false;
    return SerializeBatch(batch, writeOptions);
}

std::shared_ptr<arrow::RecordBatch> DeserializeBatch(const TString& blob, const std::shared_ptr<arrow::Schema>& schema)
{
    auto result = NSerialization::TNativeSerializer().Deserialize(blob, schema);
    if (result.ok()) {
        return *result;
    } else {
        AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_parse")("message", result.status().ToString())
            ("schema_columns_count", schema->num_fields())("schema_columns", JoinSeq(",", schema->field_names()));
        return nullptr;
    }
}

std::shared_ptr<arrow::RecordBatch> MakeEmptyBatch(const std::shared_ptr<arrow::Schema>& schema, const ui32 rowsCount) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema->num_fields());

    for (auto& field : schema->fields()) {
        auto result = NArrow::TThreadSimpleArraysCache::GetNull(field->type(), rowsCount);
        columns.emplace_back(result);
        Y_ABORT_UNLESS(result);
    }
    return arrow::RecordBatch::Make(schema, 0, columns);
}

namespace {
    template <class TStringType>
    std::shared_ptr<arrow::RecordBatch> ExtractColumnsImpl(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                    const std::vector<TStringType>& columnNames) {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(columnNames.size());
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(columnNames.size());

        auto srcSchema = srcBatch->schema();
        for (auto& name : columnNames) {
            int pos = srcSchema->GetFieldIndex(name);
            if (pos < 0) {
                return {};
            }
            fields.push_back(srcSchema->field(pos));
            columns.push_back(srcBatch->column(pos));
        }

        return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), srcBatch->num_rows(), std::move(columns));
    }
}

std::shared_ptr<arrow::RecordBatch> ExtractColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                   const std::vector<TString>& columnNames) {
    return ExtractColumnsImpl(srcBatch, columnNames);
}

std::shared_ptr<arrow::RecordBatch> ExtractColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                   const std::vector<std::string>& columnNames) {
    return ExtractColumnsImpl(srcBatch, columnNames);
}

std::shared_ptr<arrow::RecordBatch> ExtractColumnsValidate(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
    const std::vector<TString>& columnNames) {
    if (!srcBatch) {
        return srcBatch;
    }
    if (columnNames.empty()) {
        return nullptr;
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columnNames.size());
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(columnNames.size());

    auto srcSchema = srcBatch->schema();
    for (auto& name : columnNames) {
        const int pos = srcSchema->GetFieldIndex(name);
        AFL_VERIFY(pos >= 0)("field_name", name)("names", JoinSeq(",", columnNames))("fields", JoinSeq(",", srcBatch->schema()->field_names()));
        fields.push_back(srcSchema->field(pos));
        columns.push_back(srcBatch->column(pos));
    }

    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), srcBatch->num_rows(), std::move(columns));
}

std::shared_ptr<arrow::RecordBatch> ExtractColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                   const std::shared_ptr<arrow::Schema>& dstSchema,
                                                   bool addNotExisted) {
    Y_ABORT_UNLESS(srcBatch);
    Y_ABORT_UNLESS(dstSchema);
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(dstSchema->num_fields());

    for (auto& field : dstSchema->fields()) {
        columns.push_back(srcBatch->GetColumnByName(field->name()));
        if (!columns.back()) {
            if (addNotExisted) {
                auto result = arrow::MakeArrayOfNull(field->type(), srcBatch->num_rows());
                if (!result.ok()) {
                    return nullptr;
                }
                columns.back() = *result;
            } else {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "not_found_column")("column", field->name())
                    ("column_type", field->type()->ToString())("columns", JoinSeq(",", srcBatch->schema()->field_names()));
                return nullptr;
            }
        } else {
            auto srcField = srcBatch->schema()->GetFieldByName(field->name());
            Y_ABORT_UNLESS(srcField);
            if (!field->Equals(srcField)) {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")("column", field->name())
                                ("column_type", field->ToString(true))("incoming_type", srcField->ToString(true));
                return nullptr;
            }
        }

        Y_ABORT_UNLESS(columns.back());
        if (!columns.back()->type()->Equals(field->type())) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")("column", field->name())
                                ("column_type", field->type()->ToString())("incoming_type", columns.back()->type()->ToString());
            return nullptr;
        }
    }

    return arrow::RecordBatch::Make(dstSchema, srcBatch->num_rows(), columns);
}

std::shared_ptr<arrow::RecordBatch> ExtractExistedColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                          const arrow::FieldVector& fieldsToExtract) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(fieldsToExtract.size());
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(fieldsToExtract.size());

    auto srcSchema = srcBatch->schema();
    for (auto& fldToExtract : fieldsToExtract) {
        auto& name = fldToExtract->name();
        auto field = srcSchema->GetFieldByName(name);
        if (field && field->type()->Equals(fldToExtract->type())) {
            fields.push_back(field);
            columns.push_back(srcBatch->GetColumnByName(name));
        }
    }

    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), srcBatch->num_rows(), std::move(columns));
}

std::shared_ptr<arrow::Table> CombineInTable(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    auto res = arrow::Table::FromRecordBatches(batches);
    if (!res.ok()) {
        return nullptr;
    }

    res = (*res)->CombineChunks();
    if (!res.ok()) {
        return nullptr;
    }

    return res.ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> CombineBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    if (batches.empty()) {
        return nullptr;
    }
    auto table = CombineInTable(batches);
    return table ? ToBatch(table) : nullptr;
}

std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Table>& tableExt, const bool combine) {
    Y_ABORT_UNLESS(tableExt);
    std::shared_ptr<arrow::Table> table;
    if (combine) {
        auto res = tableExt->CombineChunks();
        Y_ABORT_UNLESS(res.ok());
        table = *res;
    } else {
        table = tableExt;
    }
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(table->num_columns());
    for (auto& col : table->columns()) {
        Y_ABORT_UNLESS(col->num_chunks() == 1);
        columns.push_back(col->chunk(0));
    }
    return arrow::RecordBatch::Make(table->schema(), table->num_rows(), columns);
}

std::shared_ptr<arrow::RecordBatch> CombineSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                         const std::shared_ptr<TSortDescription>& description) {
    std::vector<NArrow::IInputStream::TPtr> streams;
    for (auto& batch : batches) {
        streams.push_back(std::make_shared<NArrow::TOneBatchInputStream>(batch));
    }

    auto mergeStream = std::make_shared<NArrow::TMergingSortedInputStream>(streams, description, Max<ui64>());
    std::shared_ptr<arrow::RecordBatch> batch = mergeStream->Read();
    Y_ABORT_UNLESS(!mergeStream->Read());
    return batch;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> MergeSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                                    const std::shared_ptr<TSortDescription>& description,
                                                                    size_t maxBatchRows) {
    Y_ABORT_UNLESS(maxBatchRows);
    ui64 numRows = 0;
    std::vector<NArrow::IInputStream::TPtr> streams;
    streams.reserve(batches.size());
    for (auto& batch : batches) {
        if (batch->num_rows()) {
            numRows += batch->num_rows();
            streams.push_back(std::make_shared<NArrow::TOneBatchInputStream>(batch));
        }
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> out;
    out.reserve(numRows / maxBatchRows + 1);

    auto mergeStream = std::make_shared<NArrow::TMergingSortedInputStream>(streams, description, maxBatchRows);
    while (std::shared_ptr<arrow::RecordBatch> batch = mergeStream->Read()) {
        Y_ABORT_UNLESS(batch->num_rows());
        out.push_back(batch);
    }
    return out;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> SliceSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                                    const std::shared_ptr<TSortDescription>& description,
                                                                    size_t maxBatchRows) {
    Y_ABORT_UNLESS(!description->Reverse);

    std::vector<NArrow::IInputStream::TPtr> streams;
    streams.reserve(batches.size());
    for (auto& batch : batches) {
        if (batch->num_rows()) {
            streams.push_back(std::make_shared<NArrow::TOneBatchInputStream>(batch));
        }
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> out;
    out.reserve(streams.size());

    auto dedupStream = std::make_shared<NArrow::TMergingSortedInputStream>(streams, description, maxBatchRows, true);
    while (std::shared_ptr<arrow::RecordBatch> batch = dedupStream->Read()) {
        Y_ABORT_UNLESS(batch->num_rows());
        out.push_back(batch);
    }
    return out;
}

// Check if the permutation doesn't reorder anything
bool IsTrivial(const arrow::UInt64Array& permutation, const ui64 originalLength) {
    if ((ui64)permutation.length() != originalLength) {
        return false;
    }
    for (i64 i = 0; i < permutation.length(); ++i) {
        if (permutation.Value(i) != (ui64)i) {
            return false;
        }
    }
    return true;
}

std::shared_ptr<arrow::RecordBatch> Reorder(const std::shared_ptr<arrow::RecordBatch>& batch,
                                            const std::shared_ptr<arrow::UInt64Array>& permutation, const bool canRemove) {
    Y_ABORT_UNLESS(permutation->length() == batch->num_rows() || canRemove);

    auto res = IsTrivial(*permutation, batch->num_rows()) ? batch : arrow::compute::Take(batch, permutation);
    Y_ABORT_UNLESS(res.ok());
    return (*res).record_batch();
}

std::vector<std::shared_ptr<arrow::RecordBatch>> ShardingSplit(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                               const std::vector<ui32>& sharding, ui32 numShards) {
    Y_ABORT_UNLESS((size_t)batch->num_rows() == sharding.size());

    std::vector<std::vector<ui32>> shardRows(numShards);
    for (size_t row = 0; row < sharding.size(); ++row) {
        ui32 shardNo = sharding[row];
        Y_ABORT_UNLESS(shardNo < numShards);
        shardRows[shardNo].push_back(row);
    }

    std::shared_ptr<arrow::UInt64Array> permutation;
    {
        arrow::UInt64Builder builder;
        Y_VERIFY_OK(builder.Reserve(sharding.size()));

        for (ui32 shardNo = 0; shardNo < numShards; ++shardNo) {
            for (auto& row : shardRows[shardNo]) {
                Y_VERIFY_OK(builder.Append(row));
            }
        }
        Y_VERIFY_OK(builder.Finish(&permutation));
    }

    auto reorderedBatch = Reorder(batch, permutation, false);

    std::vector<std::shared_ptr<arrow::RecordBatch>> out(numShards);

    int offset = 0;
    for (ui32 shardNo = 0; shardNo < numShards; ++shardNo) {
        int length = shardRows[shardNo].size();
        if (length) {
            out[shardNo] = reorderedBatch->Slice(offset, length);
            offset += length;
        }
    }

    Y_ABORT_UNLESS(offset == batch->num_rows());
    return out;
}

void DedupSortedBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                      const std::shared_ptr<arrow::Schema>& sortingKey,
                      std::vector<std::shared_ptr<arrow::RecordBatch>>& out) {
    if (batch->num_rows() < 2) {
        out.push_back(batch);
        return;
    }

    Y_DEBUG_ABORT_UNLESS(NArrow::IsSorted(batch, sortingKey));

    auto keyBatch = ExtractColumns(batch, sortingKey);
    auto& keyColumns = keyBatch->columns();

    bool same = false;
    int start = 0;
    for (int i = 1; i < batch->num_rows(); ++i) {
        TRawReplaceKey prev(&keyColumns, i - 1);
        TRawReplaceKey current(&keyColumns, i);
        if (prev == current) {
            if (!same) {
                out.push_back(batch->Slice(start, i - start));
                Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(out.back(), sortingKey));
                same = true;
            }
        } else if (same) {
            same = false;
            start = i;
        }
    }
    if (!start) {
        out.push_back(batch);
    } else if (!same) {
        out.push_back(batch->Slice(start, batch->num_rows() - start));
    }
    Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(out.back(), sortingKey));
}

template <bool desc, bool uniq>
static bool IsSelfSorted(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch->num_rows() < 2) {
        return true;
    }
    auto& columns = batch->columns();

    for (int i = 1; i < batch->num_rows(); ++i) {
        TRawReplaceKey prev(&columns, i - 1);
        TRawReplaceKey current(&columns, i);
        if constexpr (desc) {
            if (prev < current) {
                AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "prev < current")("current", current.DebugString())("prev", prev.DebugString());
                return false;
            }
        } else {
            if (current < prev) {
                AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "current < prev")("current", current.DebugString())("prev", prev.DebugString());
                return false;
            }
        }
        if constexpr (uniq) {
            if (prev == current) {
                AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "equal")("current", current.DebugString())("prev", prev.DebugString());
                return false;
            }
        }
    }
    return true;
}

bool IsSorted(const std::shared_ptr<arrow::RecordBatch>& batch,
              const std::shared_ptr<arrow::Schema>& sortingKey, bool desc) {
    auto keyBatch = ExtractColumns(batch, sortingKey);
    if (desc) {
        return IsSelfSorted<true, false>(keyBatch);
    } else {
        return IsSelfSorted<false, false>(keyBatch);
    }
}

bool IsSortedAndUnique(const std::shared_ptr<arrow::RecordBatch>& batch,
                       const std::shared_ptr<arrow::Schema>& sortingKey, bool desc) {
    auto keyBatch = ExtractColumns(batch, sortingKey);
    if (desc) {
        return IsSelfSorted<true, true>(keyBatch);
    } else {
        return IsSelfSorted<false, true>(keyBatch);
    }
}

bool HasAllColumns(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& schema) {
    for (auto& field : schema->fields()) {
        if (batch->schema()->GetFieldIndex(field->name()) < 0) {
            return false;
        }
    }
    return true;
}

std::vector<std::unique_ptr<arrow::ArrayBuilder>> MakeBuilders(const std::shared_ptr<arrow::Schema>& schema,
                                                               size_t reserve, const std::map<std::string, ui64>& sizeByColumn) {
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.reserve(schema->num_fields());

    for (auto& field : schema->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        TStatusValidator::Validate(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &builder));
        if (sizeByColumn.size()) {
            auto it = sizeByColumn.find(field->name());
            if (it != sizeByColumn.end()) {
                AFL_VERIFY(NArrow::ReserveData(*builder, it->second))("size", it->second)("field", field->name());
            }
        }

        if (reserve) {
            TStatusValidator::Validate(builder->Reserve(reserve));
        }

        builders.emplace_back(std::move(builder));

    }
    return builders;
}

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::Field>& field) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    TStatusValidator::Validate(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &builder));
    return std::move(builder);
}

std::vector<std::shared_ptr<arrow::Array>> Finish(std::vector<std::unique_ptr<arrow::ArrayBuilder>>&& builders) {
    std::vector<std::shared_ptr<arrow::Array>> out;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        TStatusValidator::Validate(builder->Finish(&array));
        out.emplace_back(array);
    }
    return out;
}

std::vector<TString> ColumnNames(const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<TString> out;
    out.reserve(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto& name = schema->field(i)->name();
        out.emplace_back(TString(name.data(), name.size()));
    }
    return out;
}

std::shared_ptr<arrow::UInt64Array> MakeUI64Array(ui64 value, i64 size) {
    auto res = arrow::MakeArrayFromScalar(arrow::UInt64Scalar(value), size);
    Y_ABORT_UNLESS(res.ok());
    return std::static_pointer_cast<arrow::UInt64Array>(*res);
}

std::pair<int, int> FindMinMaxPosition(const std::shared_ptr<arrow::Array>& array) {
    if (array->length() == 0) {
        return {-1, -1};
    }

    int minPos = 0;
    int maxPos = 0;
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);

        for (int i = 1; i < column.length(); ++i) {
            const auto& value = column.GetView(i);
            if (value < column.GetView(minPos)) {
                minPos = i;
            }
            if (value > column.GetView(maxPos)) {
                maxPos = i;
            }
        }
        return true;
    });
    return {minPos, maxPos};
}

std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type) {
    std::shared_ptr<arrow::Scalar> out;
    SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow::TypeTraits<T>::ScalarType;

        if constexpr (std::is_same_v<T, arrow::StringType> ||
                      std::is_same_v<T, arrow::BinaryType> ||
                      std::is_same_v<T, arrow::LargeStringType> ||
                      std::is_same_v<T, arrow::LargeBinaryType>) {
            out = std::make_shared<TScalar>(arrow::Buffer::FromString(""), type);
        } else if constexpr (std::is_same_v<T, arrow::FixedSizeBinaryType>) {
            std::string s(static_cast<arrow::FixedSizeBinaryType&>(*type).byte_width(), '\0');
            out = std::make_shared<TScalar>(arrow::Buffer::FromString(s), type);
        } else if constexpr (std::is_same_v<T, arrow::HalfFloatType>) {
            return false;
        } else if constexpr (arrow::is_temporal_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
            out = std::make_shared<TScalar>(Min<TCType>(), type);
        } else if constexpr (arrow::has_c_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
            out = std::make_shared<TScalar>(Min<TCType>());
        } else {
            return false;
        }
        return true;
    });
    Y_ABORT_UNLESS(out);
    return out;
}

std::shared_ptr<arrow::Scalar> GetScalar(const std::shared_ptr<arrow::Array>& array, int position) {
    auto res = array->GetScalar(position);
    Y_ABORT_UNLESS(res.ok());
    return *res;
}

bool IsGoodScalar(const std::shared_ptr<arrow::Scalar>& x) {
    if (!x) {
        return false;
    }

    return SwitchType(x->type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        using TValue = std::decay_t<decltype(static_cast<const TScalar&>(*x).value)>;

        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            const auto& xval = static_cast<const TScalar&>(*x).value;
            return xval && xval->data();
        }
        if constexpr (std::is_arithmetic_v<TValue>) {
            return true;
        }
        return false;
    });
}

bool ScalarLess(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y) {
    Y_ABORT_UNLESS(x);
    Y_ABORT_UNLESS(y);
    return ScalarLess(*x, *y);
}

bool ScalarLess(const arrow::Scalar& x, const arrow::Scalar& y) {
    return ScalarCompare(x, y) < 0;
}

int ScalarCompare(const arrow::Scalar& x, const arrow::Scalar& y) {
    Y_VERIFY_S(x.type->Equals(y.type), x.type->ToString() + " vs " + y.type->ToString());

    return SwitchTypeImpl<int, 0>(x.type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        using TValue = std::decay_t<decltype(static_cast<const TScalar&>(x).value)>;

        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            const auto& xval = static_cast<const TScalar&>(x).value;
            const auto& yval = static_cast<const TScalar&>(y).value;
            Y_ABORT_UNLESS(xval);
            Y_ABORT_UNLESS(yval);
            TStringBuf xBuf(reinterpret_cast<const char*>(xval->data()), xval->size());
            TStringBuf yBuf(reinterpret_cast<const char*>(yval->data()), yval->size());
            if (xBuf < yBuf) {
                return -1;
            } else if (yBuf < xBuf) {
                return 1;
            } else {
                return 0;
            }
        }
        if constexpr (std::is_arithmetic_v<TValue>) {
            const auto& xval = static_cast<const TScalar&>(x).value;
            const auto& yval = static_cast<const TScalar&>(y).value;
            if (xval < yval) {
                return -1;
            } else if (yval < xval) {
                return 1;
            } else {
                return 0;
            }
        }
        Y_ABORT_UNLESS(false); // TODO: non primitive types
        return 0;
    });
}

int ScalarCompare(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y) {
    Y_ABORT_UNLESS(x);
    Y_ABORT_UNLESS(y);
    return ScalarCompare(*x, *y);
}

std::shared_ptr<arrow::RecordBatch> SortBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                              const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique) {
    auto sortPermutation = MakeSortPermutation(batch, sortingKey, andUnique);
    if (sortPermutation) {
        return Reorder(batch, sortPermutation, andUnique);
    } else {
        return batch;
    }
}

std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<bool>& vec) {
    std::shared_ptr<arrow::Array> out;
    arrow::BooleanBuilder builder;
    for (const auto val : vec) {
        Y_ABORT_UNLESS(builder.Append(val).ok());
    }
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}


bool ArrayScalarsEqual(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs) {
    bool res = lhs->length() == rhs->length();
    for (int64_t i = 0; i < lhs->length() && res; ++i) {
        res &= arrow::ScalarEquals(*lhs->GetScalar(i).ValueOrDie(), *rhs->GetScalar(i).ValueOrDie());
    }
    return res;
}

bool ReserveData(arrow::ArrayBuilder& builder, const size_t size) {
    arrow::Status result = arrow::Status::OK();
    if (builder.type()->id() == arrow::Type::BINARY ||
        builder.type()->id() == arrow::Type::STRING)
    {
        static_assert(std::is_convertible_v<arrow::StringBuilder&, arrow::BaseBinaryBuilder<arrow::BinaryType>&>,
            "Expected StringBuilder to be BaseBinaryBuilder<BinaryType>");
        auto& bBuilder = static_cast<arrow::BaseBinaryBuilder<arrow::BinaryType>&>(builder);
        result = bBuilder.ReserveData(size);
    }

    if (!result.ok()) {
        AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "ReserveData")("error", result.ToString());
    }
    return result.ok();
}

template <class TData, class TColumn, class TBuilder>
bool MergeBatchColumnsImpl(const std::vector<std::shared_ptr<TData>>& batches, std::shared_ptr<TData>& result,
    const std::vector<std::string>& columnsOrder, const bool orderFieldsAreNecessary, const TBuilder& builder) {
    if (batches.empty()) {
        result = nullptr;
        return true;
    }
    if (batches.size() == 1) {
        result = batches.front();
        return true;
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<TColumn>> columns;
    std::map<std::string, ui32> fieldNames;
    for (auto&& i : batches) {
        Y_ABORT_UNLESS(i);
        for (auto&& f : i->schema()->fields()) {
            if (!fieldNames.emplace(f->name(), fields.size()).second) {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "duplicated column")("name", f->name());
                return false;
            }
            fields.emplace_back(f);
        }
        if (i->num_rows() != batches.front()->num_rows()) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "inconsistency record sizes")("i", i->num_rows())("front", batches.front()->num_rows());
            return false;
        }
        for (auto&& c : i->columns()) {
            columns.emplace_back(c);
        }
    }

    Y_ABORT_UNLESS(fields.size() == columns.size());
    if (columnsOrder.size()) {
        std::vector<std::shared_ptr<arrow::Field>> fieldsOrdered;
        std::vector<std::shared_ptr<TColumn>> columnsOrdered;
        for (auto&& i : columnsOrder) {
            auto it = fieldNames.find(i);
            if (orderFieldsAreNecessary) {
                Y_ABORT_UNLESS(it != fieldNames.end());
            } else if (it == fieldNames.end()) {
                continue;
            }
            fieldsOrdered.emplace_back(fields[it->second]);
            columnsOrdered.emplace_back(columns[it->second]);
        }
        std::swap(fieldsOrdered, fields);
        std::swap(columnsOrdered, columns);
    }
    result = builder(std::make_shared<arrow::Schema>(fields), batches.front()->num_rows(), std::move(columns));
    return true;
}

bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow::Table>>& batches, std::shared_ptr<arrow::Table>& result, const std::vector<std::string>& columnsOrder, const bool orderFieldsAreNecessary) {
    const auto builder = [](const std::shared_ptr<arrow::Schema>& schema, const ui32 recordsCount, std::vector<std::shared_ptr<arrow::ChunkedArray>>&& columns) {
        return arrow::Table::Make(schema, columns, recordsCount);
    };

    return MergeBatchColumnsImpl<arrow::Table, arrow::ChunkedArray>(batches, result, columnsOrder, orderFieldsAreNecessary, builder);
}

bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, std::shared_ptr<arrow::RecordBatch>& result, const std::vector<std::string>& columnsOrder, const bool orderFieldsAreNecessary) {
    const auto builder = [](const std::shared_ptr<arrow::Schema>& schema, const ui32 recordsCount, std::vector<std::shared_ptr<arrow::Array>>&& columns) {
        return arrow::RecordBatch::Make(schema, recordsCount, columns);
    };

    return MergeBatchColumnsImpl<arrow::RecordBatch, arrow::Array>(batches, result, columnsOrder, orderFieldsAreNecessary, builder);
}

std::partial_ordering ColumnsCompare(const std::vector<std::shared_ptr<arrow::Array>>& x, const ui32 xRow, const std::vector<std::shared_ptr<arrow::Array>>& y, const ui32 yRow) {
    return TRawReplaceKey(&x, xRow).CompareNotNull(TRawReplaceKey(&y, yRow));
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> array, const ui32 position) {
    NJson::TJsonValue result = NJson::JSON_ARRAY;
    for (auto&& i : array->columns()) {
        result.AppendValue(DebugJson(i, position));
    }
    return result;
}

TString DebugString(std::shared_ptr<arrow::Array> array, const ui32 position) {
    if (!array) {
        return "_NO_DATA";
    }
    Y_ABORT_UNLESS(position < array->length());
    TStringBuilder result;
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            auto value = column.GetString(position);
            result << TString(value.data(), value.size());
        }
        if constexpr (arrow::has_c_type<typename TWrap::T>()) {
            result << column.Value(position);
        }
        return true;
    });
    return result;
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 position) {
    if (!array) {
        return NJson::JSON_NULL;
    }
    Y_ABORT_UNLESS(position < array->length());
    NJson::TJsonValue result = NJson::JSON_MAP;
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);
        result.InsertValue("type", typeid(TArray).name());
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            auto value = column.GetString(position);
            result.InsertValue("value", TString(value.data(), value.size()));
        }
        if constexpr (arrow::has_c_type<typename TWrap::T>()) {
            result.InsertValue("value", column.Value(position));
        }
        return true;
    });
    return result;
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 head, const ui32 tail) {
    if (!array) {
        return NJson::JSON_NULL;
    }
    NJson::TJsonValue resultFull = NJson::JSON_MAP;
    resultFull.InsertValue("length", array->length());
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);
        resultFull.InsertValue("type", typeid(TArray).name());
        resultFull.InsertValue("head", head);
        resultFull.InsertValue("tail", tail);
        auto& result = resultFull.InsertValue("data", NJson::JSON_ARRAY);
        for (int i = 0; i < column.length(); ++i) {
            if (i >= (int)head && i + (int)tail < column.length()) {
                continue;
            }
            if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                auto value = column.GetString(i);
                result.AppendValue(TString(value.data(), value.size()));
            }
            if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                result.AppendValue(column.Value(i));
            }
        }
        return true;
        });
    return resultFull;
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> batch, const ui32 head, const ui32 tail) {
    if (!batch) {
        return NJson::JSON_NULL;
    }
    NJson::TJsonValue result = NJson::JSON_ARRAY;
    ui32 idx = 0;
    for (auto&& i : batch->columns()) {
        auto& jsonColumn = result.AppendValue(NJson::JSON_MAP);
        jsonColumn.InsertValue("name", batch->column_name(idx));
        jsonColumn.InsertValue("data", DebugJson(i, head, tail));
        ++idx;
    }
    return result;
}

std::shared_ptr<arrow::RecordBatch> ReallocateBatch(std::shared_ptr<arrow::RecordBatch> original) {
    if (!original) {
        return nullptr;
    }
    return DeserializeBatch(SerializeBatch(original, arrow::ipc::IpcWriteOptions::Defaults()), original->schema());
}

std::shared_ptr<arrow::RecordBatch> MergeColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::optional<ui32> recordsCount;
    std::set<std::string> columnNames;
    for (auto&& batch : batches) {
        if (!batch) {
            continue;
        }
        for (auto&& column : batch->columns()) {
            columns.emplace_back(column);
            if (!recordsCount) {
                recordsCount = column->length();
            } else {
                Y_ABORT_UNLESS(*recordsCount == column->length());
            }
        }
        for (auto&& field : batch->schema()->fields()) {
            AFL_VERIFY(columnNames.emplace(field->name()).second)("field_name", field->name());
            fields.emplace_back(field);
        }
    }
    if (columns.empty()) {
        return nullptr;
    }
    auto schema = std::make_shared<arrow::Schema>(fields);
    return arrow::RecordBatch::Make(schema, *recordsCount, columns);
}

}
