#include "arrow_helpers.h"
#include "switch/switch_type.h"
#include "permutations.h"
#include "common/adapter.h"
#include "serializer/native.h"
#include "serializer/abstract.h"
#include "serializer/stream.h"

#include <ydb/library/formats/arrow/common/validation.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>
#include <ydb/library/formats/arrow/replace_key.h>
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
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl(const NScheme::TTypeInfo& typeInfo) {
    Y_UNUSED(typeInfo);
    return std::make_shared<TType>();
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::Decimal128Type>(const NScheme::TTypeInfo& typeInfo) {
    return arrow::decimal(typeInfo.GetDecimalType().GetPrecision(), typeInfo.GetDecimalType().GetScale());
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::TimestampType>(const NScheme::TTypeInfo& typeInfo) {
    Y_UNUSED(typeInfo);
    return arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO);
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::DurationType>(const NScheme::TTypeInfo& typeInfo) {
    Y_UNUSED(typeInfo);
    return arrow::duration(arrow::TimeUnit::TimeUnit::MICRO);
}

arrow::Result<std::shared_ptr<arrow::DataType>> GetArrowType(NScheme::TTypeInfo typeInfo) {
    std::shared_ptr<arrow::DataType> result;
    bool success = SwitchYqlTypeToArrowType(typeInfo, [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
        Y_UNUSED(typeHolder);
        result = CreateEmptyArrowImpl<TType>(typeInfo);
        return true;
    });
    if (success) {
        return result;
    }
    
    return arrow::Status::TypeError("unsupported type ", NKikimr::NScheme::TypeName(typeInfo));
}

arrow::Result<std::shared_ptr<arrow::DataType>> GetCSVArrowType(NScheme::TTypeInfo typeId) {
    std::shared_ptr<arrow::DataType> result;
    switch (typeId.GetTypeId()) {
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Datetime64:
            return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::SECOND);
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Timestamp64:
            return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO);
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Date32:
            return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::SECOND);
        default:
            return GetArrowType(typeId);
    }
}

arrow::Result<arrow::FieldVector> MakeArrowFields(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, const std::set<std::string>& notNullColumns) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columns.size());
    TVector<TString> errors;
    for (auto& [name, ydbType] : columns) {
        std::string colName(name.data(), name.size());
        auto arrowType = GetArrowType(ydbType);
        if (arrowType.ok()) {
            fields.emplace_back(std::make_shared<arrow::Field>(colName, arrowType.ValueUnsafe(), !notNullColumns.contains(colName)));
        } else {
            errors.emplace_back(colName + " error: " + arrowType.status().ToString());
        }
    }
    if (errors.empty()) {
        return fields;
    }
    return arrow::Status::TypeError(JoinSeq(", ", errors));
}

arrow::Result<std::shared_ptr<arrow::Schema>> MakeArrowSchema(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbColumns, const std::set<std::string>& notNullColumns) {
    const auto fields = MakeArrowFields(ydbColumns, notNullColumns);
    if (fields.ok()) {
        return std::make_shared<arrow::Schema>(fields.ValueUnsafe());
    }
    return fields.status();
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

void DedupSortedBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                      const std::shared_ptr<arrow::Schema>& sortingKey,
                      std::vector<std::shared_ptr<arrow::RecordBatch>>& out) {
    if (batch->num_rows() < 2) {
        out.push_back(batch);
        return;
    }

    Y_DEBUG_ABORT_UNLESS(NArrow::IsSorted(batch, sortingKey));

    auto keyBatch = TColumnOperator().Adapt(batch, sortingKey).DetachResult();
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

bool IsSorted(const std::shared_ptr<arrow::RecordBatch>& batch,
              const std::shared_ptr<arrow::Schema>& sortingKey, bool desc) {
    auto keyBatch = TColumnOperator().Adapt(batch, sortingKey).DetachResult();
    if (desc) {
        return IsSelfSorted<true, false>(keyBatch);
    } else {
        return IsSelfSorted<false, false>(keyBatch);
    }
}

bool IsSortedAndUnique(const std::shared_ptr<arrow::RecordBatch>& batch,
                       const std::shared_ptr<arrow::Schema>& sortingKey, bool desc) {
    auto keyBatch = TColumnOperator().Adapt(batch, sortingKey).DetachResult();
    if (desc) {
        return IsSelfSorted<true, true>(keyBatch);
    } else {
        return IsSelfSorted<false, true>(keyBatch);
    }
}

std::shared_ptr<arrow::RecordBatch> SortBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::shared_ptr<arrow::Array>>& sortingKey, const bool andUnique) {
    auto sortPermutation = MakeSortPermutation(sortingKey, andUnique);
    if (sortPermutation) {
        return Reorder(batch, sortPermutation, andUnique);
    } else {
        return batch;
    }
}

std::shared_ptr<arrow::RecordBatch> SortBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& sortingKey,
    const bool andUnique) {
    auto sortPermutation = MakeSortPermutation(batch, sortingKey, andUnique);
    if (sortPermutation) {
        return Reorder(batch, sortPermutation, andUnique);
    } else {
        return batch;
    }
}

std::shared_ptr<arrow::RecordBatch> ReallocateBatch(std::shared_ptr<arrow::RecordBatch> original) {
    if (!original) {
        return nullptr;
    }
    return DeserializeBatch(SerializeBatch(original, arrow::ipc::IpcWriteOptions::Defaults()), original->schema());
}

std::shared_ptr<arrow::Table> ReallocateBatch(const std::shared_ptr<arrow::Table>& original) {
    if (!original) {
        return original;
    }
    auto batches = NArrow::SliceToRecordBatches(original);
    for (auto&& i : batches) {
        i = NArrow::TStatusValidator::GetValid(
            NArrow::NSerialization::TNativeSerializer().Deserialize(NArrow::NSerialization::TNativeSerializer().SerializeFull(i)));
    }
    return NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches(batches));
}

}
