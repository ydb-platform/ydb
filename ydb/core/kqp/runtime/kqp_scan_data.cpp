#include "kqp_scan_data.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/size_calcer.h>

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/pack.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_scalar.h>

namespace NKikimr {
namespace NMiniKQL {

TBytesStatistics GetUnboxedValueSize(const NUdf::TUnboxedValue& value, const NScheme::TTypeInfo& type) {
    namespace NTypeIds = NScheme::NTypeIds;
    if (!value) {
        return { sizeof(NUdf::TUnboxedValue), 8 }; // Special value for NULL elements
    }
    switch (type.GetTypeId()) {
        case NTypeIds::Pg:
        {
            return {
                sizeof(NUdf::TUnboxedValue),
                PgValueSize(value, NPg::TypeDescGetTypeLen(type.GetTypeDesc()))
            };
        }

        case NTypeIds::Bool:
        case NTypeIds::Int8:
        case NTypeIds::Uint8:

        case NTypeIds::Int16:
        case NTypeIds::Uint16:

        case NTypeIds::Int32:
        case NTypeIds::Uint32:
        case NTypeIds::Float:
        case NTypeIds::Date:

        case NTypeIds::Int64:
        case NTypeIds::Uint64:
        case NTypeIds::Double:
        case NTypeIds::Datetime:
        case NTypeIds::Timestamp:
        case NTypeIds::Interval:
        case NTypeIds::Date32:
        case NTypeIds::Datetime64:
        case NTypeIds::Timestamp64:
        case NTypeIds::Interval64:
        case NTypeIds::ActorId:
        case NTypeIds::StepOrderId:
        {
            YQL_ENSURE(value.IsEmbedded(), "Passed wrong type: " << NScheme::TypeName(type.GetTypeId()));
            return { sizeof(NUdf::TUnboxedValue), sizeof(i64) };
        }
        case NTypeIds::Decimal:
        {
            YQL_ENSURE(value.IsEmbedded(), "Passed wrong type: " << NScheme::TypeName(type.GetTypeId()));
            return { sizeof(NUdf::TUnboxedValue), sizeof(NYql::NDecimal::TInt128) };
        }
        case NTypeIds::String:
        case NTypeIds::Utf8:
        case NTypeIds::Json:
        case NTypeIds::Yson:
        case NTypeIds::JsonDocument:
        case NTypeIds::DyNumber:
        case NTypeIds::Uuid:
        case NTypeIds::PairUi64Ui64:
        {
            if (value.IsEmbedded()) {
                return { sizeof(NUdf::TUnboxedValue), std::max((ui32)8, value.AsStringRef().Size()) };
            } else {
                Y_VERIFY_DEBUG_S(8 < value.AsStringRef().Size(), "Small string of size " << value.AsStringRef().Size() << " is not embedded.");
                return { sizeof(NUdf::TUnboxedValue) + value.AsStringRef().Size(), value.AsStringRef().Size() };
            }
        }


        default:
            Y_VERIFY_DEBUG_S(false, "Unsupported type " << NScheme::TypeName(type.GetTypeId()));
            if (value.IsEmbedded()) {
                return { sizeof(NUdf::TUnboxedValue), sizeof(NUdf::TUnboxedValue) };
            } else {
                return { sizeof(NUdf::TUnboxedValue) + value.AsStringRef().Size(), value.AsStringRef().Size() };
            }
    }
}

void FillSystemColumn(NUdf::TUnboxedValue& rowItem, TMaybe<ui64> shardId, NTable::TTag tag, NScheme::TTypeInfo) {
    YQL_ENSURE(tag == TKeyDesc::EColumnIdDataShard, "Unknown system column tag: " << tag);

    if (shardId) {
        rowItem = NUdf::TUnboxedValuePod(*shardId);
    } else {
        rowItem = NUdf::TUnboxedValue();
    }
}

namespace {

TBytesStatistics GetRowSize(const NUdf::TUnboxedValue* row, const TSmallVec<TKqpComputeContextBase::TColumn>& columns,
    const TSmallVec<TKqpComputeContextBase::TColumn>& systemColumns) {
    TBytesStatistics rowStats{ systemColumns.size() * sizeof(NUdf::TUnboxedValue), 0 };
    for (size_t columnIndex = 0; columnIndex < columns.size(); ++columnIndex) {
        rowStats += GetUnboxedValueSize(row[columnIndex], columns[columnIndex].Type);
    }
    if (columns.empty()) {
        rowStats.AddStatistics({ sizeof(ui64), sizeof(ui64) });
    }
    return rowStats;
}

void FillSystemColumns(NUdf::TUnboxedValue* rowItems, TMaybe<ui64> shardId, const TSmallVec<TKqpComputeContextBase::TColumn>& systemColumns) {
    for (ui32 i = 0; i < systemColumns.size(); ++i) {
        FillSystemColumn(rowItems[i], shardId, systemColumns[i].Tag, systemColumns[i].Type);
    }
}

template <typename TArrayType, typename TValueType = typename TArrayType::value_type>
NUdf::TUnboxedValue MakeUnboxedValue(arrow::Array* column, ui32 row) {
    auto array = reinterpret_cast<TArrayType*>(column);
    return NUdf::TUnboxedValuePod(static_cast<TValueType>(array->Value(row)));
}

TKqpScanComputeContext::TScanData::EReadType ReadTypeFromProto(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::EReadType& type) {
    switch (type) {
        case NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::ROWS:
            return TKqpScanComputeContext::TScanData::EReadType::Rows;
        case NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::BLOCKS:
            return TKqpScanComputeContext::TScanData::EReadType::Blocks;
        default:
            YQL_ENSURE(false, "Invalid read type from TScanTaskMeta protobuf.");
    }
}
} // namespace

ui32 TKqpScanComputeContext::TScanData::TRowBatchReader::TRowBatch::FillUnboxedCells(NUdf::TUnboxedValue* const* result) {
    ui32 resultColumnsCount = 0;
    if (ColumnsCount) {
        auto* data = &Cells[CurrentRow * CellsCountForRow];
        for (ui32 i = 0; i < CellsCountForRow; ++i) {
            if (result[i]) {
                *result[i] = std::move(*(data + i));
                ++resultColumnsCount;
            }
        }
    }
    ++CurrentRow;
    return resultColumnsCount;
}

ui32 TKqpScanComputeContext::TScanData::TBlockBatchReader::TBlockBatch::FillBlockValues(NUdf::TUnboxedValue* const* result) {
    for (ui32 i = 0; i < ColumnsCount; ++i) {
        *result[i] = std::move(BatchValues[i]);
    }
    return ColumnsCount;
}

namespace {

class TDefaultStatAccumulator {
private:
    ui32 NullsCount = 0;
    TBytesStatistics BytesAllocated;
    NScheme::TTypeInfo TypeInfo;
public:
    TDefaultStatAccumulator(const NScheme::TTypeInfo& tInfo)
        : TypeInfo(tInfo)
    {

    }

    void AddNull() {
        ++NullsCount;
    }
    void AddValue(const NYql::NUdf::TUnboxedValue& value) {
        BytesAllocated += GetUnboxedValueSize(value, TypeInfo);
    }

    TBytesStatistics Finish() const {
        return BytesAllocated + GetUnboxedValueSize(NYql::NUdf::TUnboxedValue(), TypeInfo) * NullsCount;
    }
};

class TFixedWidthStatAccumulator {
private:
    ui32 NullsCount = 0;
    ui32 ValuesCount = 0;
    TBytesStatistics RowSize;
    NScheme::TTypeInfo TypeInfo;
public:
    TFixedWidthStatAccumulator(const NScheme::TTypeInfo& tInfo)
        : TypeInfo(tInfo) {

    }

    void AddNull() {
        ++NullsCount;
    }
    void AddValue(const NYql::NUdf::TUnboxedValue& value) {
        if (++ValuesCount == 1) {
            RowSize = GetUnboxedValueSize(value, TypeInfo);
        }
    }

    TBytesStatistics Finish() const {
        return RowSize * ValuesCount + GetUnboxedValueSize(NYql::NUdf::TUnboxedValue(), TypeInfo) * NullsCount;
    }
};

template <class TArrayTypeExt, class TValueType = typename TArrayTypeExt::value_type>
class TElementAccessor {
public:
    using TArrayType = TArrayTypeExt;
    static NYql::NUdf::TUnboxedValue ExtractValue(const TArrayType& array, const ui32 rowIndex) {
        return NUdf::TUnboxedValuePod(static_cast<TValueType>(array.Value(rowIndex)));
    }

    static void Validate(const TArrayType& /*array*/) {

    }

    static TFixedWidthStatAccumulator BuildStatAccumulator(const NScheme::TTypeInfo& typeInfo) {
        return TFixedWidthStatAccumulator(typeInfo);
    }
};

template <>
class TElementAccessor<arrow::Decimal128Array, NYql::NDecimal::TInt128> {
public:
    using TArrayType = arrow::Decimal128Array;
    static void Validate(const arrow::Decimal128Array& array) {
        const auto& type = arrow::internal::checked_cast<const arrow::Decimal128Type&>(*array.type());
        YQL_ENSURE(type.precision() == NScheme::DECIMAL_PRECISION, "Unsupported Decimal precision.");
        YQL_ENSURE(type.scale() == NScheme::DECIMAL_SCALE, "Unsupported Decimal scale.");
    }

    static NYql::NUdf::TUnboxedValue ExtractValue(const arrow::Decimal128Array& array, const ui32 rowIndex) {
        auto data = array.GetView(rowIndex);
        YQL_ENSURE(data.size() == sizeof(NYql::NDecimal::TInt128), "Wrong data size");
        NYql::NDecimal::TInt128 val;
        std::memcpy(reinterpret_cast<char*>(&val), data.data(), data.size());
        return NUdf::TUnboxedValuePod(val);
    }
    static TFixedWidthStatAccumulator BuildStatAccumulator(const NScheme::TTypeInfo& typeInfo) {
        return TFixedWidthStatAccumulator(typeInfo);
    }
};

template <>
class TElementAccessor<arrow::BinaryArray, NUdf::TStringRef> {
public:
    using TArrayType = arrow::BinaryArray;
    static void Validate(const arrow::BinaryArray& /*array*/) {
    }

    static NYql::NUdf::TUnboxedValue ExtractValue(const arrow::BinaryArray& array, const ui32 rowIndex) {
        auto data = array.GetView(rowIndex);
        return MakeString(NUdf::TStringRef(data.data(), data.size()));
    }
    static TDefaultStatAccumulator BuildStatAccumulator(const NScheme::TTypeInfo& typeInfo) {
        return TDefaultStatAccumulator(typeInfo);
    }
};

template <>
class TElementAccessor<arrow::FixedSizeBinaryArray, NUdf::TStringRef> {
public:
    using TArrayType = arrow::FixedSizeBinaryArray;
    static void Validate(const arrow::FixedSizeBinaryArray& /*array*/) {
    }

    static NYql::NUdf::TUnboxedValue ExtractValue(const arrow::FixedSizeBinaryArray& array, const ui32 rowIndex) {
        auto data = array.GetView(rowIndex);
        return MakeString(NUdf::TStringRef(data.data(), data.size() - 1));
    }
    static TFixedWidthStatAccumulator BuildStatAccumulator(const NScheme::TTypeInfo& typeInfo) {
        return TFixedWidthStatAccumulator(typeInfo);
    }
};

}

template <class TElementAccessor, class TAccessor>
TBytesStatistics WriteColumnValuesFromArrowSpecImpl(TAccessor editAccessor,
    const TBatchDataAccessor& batch, const ui32 columnIndex, const std::shared_ptr<arrow::ChunkedArray>& chunkedArrayExt, NScheme::TTypeInfo columnType) {
    auto statAccumulator = TElementAccessor::BuildStatAccumulator(columnType);

    auto trivialChunkedArray = std::make_shared<NArrow::NAccessor::TTrivialChunkedArray>(chunkedArrayExt);
    NArrow::NAccessor::IChunkedArray::TReader reader(trivialChunkedArray);

    std::optional<ui32> currentIdxFrom;
    std::optional<NArrow::NAccessor::IChunkedArray::TAddress> address;
    const typename TElementAccessor::TArrayType* currentArray = nullptr;
    const auto applyToIndex = [&](const ui32 rowIndexFrom, const ui32 rowIndexTo) {
        bool changed = false;
        if (!currentIdxFrom) {
            address = reader.GetReadChunk(rowIndexFrom);
            AFL_ENSURE(rowIndexFrom == 0)("real", rowIndexFrom);
            changed = true;
        } else {
            AFL_ENSURE(rowIndexFrom == *currentIdxFrom + 1)("next", rowIndexFrom)("current", *currentIdxFrom);
            if (!address->NextPosition()) {
                address = reader.GetReadChunk(rowIndexFrom);
                changed = true;
            }
        }
        currentIdxFrom = rowIndexFrom;

        if (changed) {
            currentArray = static_cast<const typename TElementAccessor::TArrayType*>(address->GetArray().get());
            TElementAccessor::Validate(*currentArray);
        }

        auto& rowItem = editAccessor(rowIndexTo, columnIndex);
        if (currentArray->IsNull(address->GetPosition())) {
            statAccumulator.AddNull();
            rowItem = NUdf::TUnboxedValue();
        } else {
            rowItem = TElementAccessor::ExtractValue(*currentArray, address->GetPosition());
            statAccumulator.AddValue(rowItem);
        }
    };

    if (batch.HasDataIndexes()) {
        ui32 idx = 0;
        std::map<ui64, ui64> remapIndexes;
        for (const i64 rowIndex : batch.GetDataIndexes()) {
            YQL_ENSURE(remapIndexes.emplace(rowIndex, idx++).second);
        }
        for (auto&& i : remapIndexes) {
            applyToIndex(i.first, i.second);
        }
    } else {
        for (i64 rowIndex = 0; rowIndex < batch.GetRecordsCount(); ++rowIndex) {
            applyToIndex(rowIndex, rowIndex);
        }
    }
    return statAccumulator.Finish();
}


template <class TAccessor>
TBytesStatistics WriteColumnValuesFromArrowImpl(TAccessor editAccessor,
    const TBatchDataAccessor& batch, i64 columnIndex, NScheme::TTypeInfo columnType) {
    const std::shared_ptr<arrow::ChunkedArray> columnPtr = batch.GetBatch()->column(columnIndex);
    namespace NTypeIds = NScheme::NTypeIds;
    switch (columnType.GetTypeId()) {
        case NTypeIds::Bool:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::BooleanArray, bool>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Int8:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Int8Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Int16:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Int16Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Date32:
        case NTypeIds::Int32:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Int32Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Int64:
        case NTypeIds::Timestamp64:
        case NTypeIds::Interval64:
        case NTypeIds::Datetime64:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Int64Array, i64>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Uint8:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::UInt8Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Uint16:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::UInt16Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Uint32:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::UInt32Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Uint64:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::UInt64Array, ui64>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Float:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::FloatArray>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Double:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::DoubleArray>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::String:
        case NTypeIds::Utf8:
        case NTypeIds::Json:
        case NTypeIds::Yson:
        case NTypeIds::JsonDocument:
        case NTypeIds::DyNumber:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::BinaryArray, NUdf::TStringRef>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Date:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::UInt16Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Datetime:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::UInt32Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Timestamp:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::TimestampArray, ui64>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Interval:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::DurationArray, ui64>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Decimal:
        {
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Decimal128Array, NYql::NDecimal::TInt128>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::PairUi64Ui64:
        case NTypeIds::ActorId:
        case NTypeIds::StepOrderId:
        {
            Y_VERIFY_DEBUG_S(false, "Unsupported (deprecated) type: " << NScheme::TypeName(columnType.GetTypeId()));
            return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::FixedSizeBinaryArray, NUdf::TStringRef>>(editAccessor, batch, columnIndex, columnPtr, columnType);
        }
        case NTypeIds::Pg:
            switch (NPg::PgTypeIdFromTypeDesc(columnType.GetTypeDesc())) {
                case INT2OID:
                    return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Int16Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
                case INT4OID:
                    return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Int32Array>>(editAccessor, batch, columnIndex, columnPtr, columnType);
                case INT8OID:
                    return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::Int64Array, i64>>(editAccessor, batch, columnIndex, columnPtr, columnType);
                case FLOAT4OID:
                    return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::FloatArray>>(editAccessor, batch, columnIndex, columnPtr, columnType);
                case FLOAT8OID:
                    return WriteColumnValuesFromArrowSpecImpl<TElementAccessor<arrow::DoubleArray>>(editAccessor, batch, columnIndex, columnPtr, columnType);
                default:
                    break;
            }
            // TODO: support pg types
            YQL_ENSURE(false, "Unsupported pg type at column " << columnIndex);
        default:
            YQL_ENSURE(false, "Unsupported type: " << NScheme::TypeName(columnType.GetTypeId()) << " at column " << columnIndex);
    }
}

TBytesStatistics WriteColumnValuesFromArrow(NUdf::TUnboxedValue* editAccessors,
    const TBatchDataAccessor& batch, i64 columnIndex, const ui32 columnsCount, NScheme::TTypeInfo columnType)
{
    const auto accessor = [editAccessors, columnsCount](const ui32 rowIndex, const ui32 colIndex) -> NUdf::TUnboxedValue& {
        return editAccessors[rowIndex * columnsCount + colIndex];
    };
    return WriteColumnValuesFromArrowImpl(accessor, batch, columnIndex, columnType);
}

TBytesStatistics WriteColumnValuesFromArrow(const TVector<NUdf::TUnboxedValue*>& editAccessors,
    const TBatchDataAccessor& batch, i64 columnIndex, NScheme::TTypeInfo columnType)
{
    const auto accessor = [&editAccessors](const ui32 rowIndex, const ui32 colIndex) -> NUdf::TUnboxedValue& {
        return editAccessors[rowIndex][colIndex];
    };
    return WriteColumnValuesFromArrowImpl(accessor, batch, columnIndex, columnType);
}

TBytesStatistics WriteColumnValuesFromArrow(const TVector<NUdf::TUnboxedValue*>& editAccessors,
    const TBatchDataAccessor& batch, i64 columnIndex, i64 resultColumnIndex, NScheme::TTypeInfo columnType)
{
    const auto accessor = [=, &editAccessors](const ui32 rowIndex, const ui32 colIndex) -> NUdf::TUnboxedValue& {
        YQL_ENSURE(colIndex == columnIndex);
        return editAccessors[rowIndex][resultColumnIndex];
    };
    return WriteColumnValuesFromArrowImpl(accessor, batch, columnIndex, columnType);
}

std::pair<ui64, ui64> GetUnboxedValueSizeForTests(const NUdf::TUnboxedValue& value, NScheme::TTypeInfo type) {
    auto sizes = GetUnboxedValueSize(value, type);
    return {sizes.AllocatedBytes, sizes.DataBytes};
}

ui32 TKqpScanComputeContext::TScanData::FillDataValues(NUdf::TUnboxedValue* const* result) {
    return BatchReader->FillDataValues(result);
}

ui32 TKqpScanComputeContext::TScanData::TRowBatchReader::FillDataValues(NUdf::TUnboxedValue* const* result) {
    YQL_ENSURE(!RowBatches.empty());
    auto& batch = RowBatches.front();
    const ui32 resultColumnsCount = batch.FillUnboxedCells(result);

    StoredBytes -= batch.BytesForRecordEstimation();
    if (batch.IsFinished()) {
        RowBatches.pop();
    }

    YQL_ENSURE(RowBatches.empty() == (StoredBytes < 1), "StoredBytes miscalculated!");
    return resultColumnsCount;
}

ui32 TKqpScanComputeContext::TScanData::TBlockBatchReader::FillDataValues(NUdf::TUnboxedValue* const* result) {
    YQL_ENSURE(!BlockBatches.empty());
    auto& batch = BlockBatches.front();
    const ui32 resultColumnsCount = batch.FillBlockValues(result);

    StoredBytes -= batch.BytesForRecordEstimation();
    BlockBatches.pop();
    YQL_ENSURE(BlockBatches.empty() == (StoredBytes < 1), "StoredBytes miscalculated!");
    return resultColumnsCount;
}

TKqpScanComputeContext::TScanData::TScanData(const TTableId& tableId, const TTableRange& range,
    const TSmallVec<TColumn>& columns, const TSmallVec<TColumn>& systemColumns, const TSmallVec<TColumn>& resultColumns)
    : TBase(tableId, "")
    , Range(range)
    , BatchReader(new TRowBatchReader(columns, systemColumns, resultColumns))
{}

TKqpScanComputeContext::TScanData::TScanData(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta,
    NYql::NDqProto::EDqStatsMode statsMode)
    : TBase(meta)
{
    switch(ReadTypeFromProto(meta.GetReadType())) {
        case TKqpScanComputeContext::TScanData::EReadType::Rows:
            BatchReader.reset(new TRowBatchReader(meta));
            break;
        case TKqpScanComputeContext::TScanData::EReadType::Blocks:
            BatchReader.reset(new TBlockBatchReader(meta));
            break;
    }

    if (statsMode >= NYql::NDqProto::DQ_STATS_MODE_BASIC) {
        BasicStats = std::make_unique<TBasicStats>();
    }

    if (Y_UNLIKELY(statsMode >= NYql::NDqProto::DQ_STATS_MODE_PROFILE)) {
        ProfileStats = std::make_unique<TProfileStats>();
    }
}

TBytesStatistics TKqpScanComputeContext::TScanData::TRowBatchReader::AddData(const TVector<TOwnedCellVec>& batch,
    TMaybe<ui64> shardId, const THolderFactory& holderFactory)
{
    TBytesStatistics stats;
    TUnboxedValueVector cells;
    if (TotalColumnsCount == 0u) {
        cells.resize(batch.size(), holderFactory.GetEmptyContainerLazy());
        stats.AddStatistics({ sizeof(ui64) * batch.size(), sizeof(ui64) * batch.size() });
    } else {
        cells.resize(batch.size() * TotalColumnsCount);

        for (size_t rowIndex = 0; rowIndex < batch.size(); ++rowIndex) {
            auto& row = batch[rowIndex];

            auto* vectorStart = &cells.data()[rowIndex * TotalColumnsCount];
            for (ui32 i = 0; i < ResultColumns.size(); ++i) {
                vectorStart[i] = GetCellValue(row[i], ResultColumns[i].Type);
            }
            FillSystemColumns(vectorStart + ResultColumns.size(), shardId, SystemColumns);

            stats += GetRowSize(vectorStart, ResultColumns, SystemColumns);
        }
    }
    if (cells.size()) {
        RowBatches.emplace(TRowBatch(TotalColumnsCount, batch.size(), std::move(cells), stats.AllocatedBytes));
    }
    StoredBytes += stats.AllocatedBytes;

    return stats;
}

TBytesStatistics TKqpScanComputeContext::TScanData::TBlockBatchReader::AddData(const TVector<TOwnedCellVec>& /*batch*/,
    TMaybe<ui64> /*shardId*/, const THolderFactory& /*holderFactory*/)
{
    Y_ABORT_UNLESS(false, "Batch of TOwnedCellVec should never be called for BlockBatchReader!");
    return TBytesStatistics();
}

ui64 TKqpScanComputeContext::TScanData::AddData(const TVector<TOwnedCellVec>& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) {
    if (Finished || batch.empty()) {
        return 0;
    }

    TBytesStatistics stats = BatchReader->AddData(batch, shardId, holderFactory);
    if (BasicStats) {
        BasicStats->Rows += batch.size();
        BasicStats->Bytes += stats.DataBytes;
    }

    return stats.AllocatedBytes;
}

TBytesStatistics TKqpScanComputeContext::TScanData::TRowBatchReader::AddData(const TBatchDataAccessor& batch, TMaybe<ui64> shardId,
    const THolderFactory& holderFactory)
{
    TBytesStatistics stats;
    TUnboxedValueVector cells;

    if (TotalColumnsCount == 0u) {
        cells.resize(batch.GetRecordsCount(), holderFactory.GetEmptyContainerLazy());
        stats.AddStatistics({ sizeof(ui64) * batch.GetRecordsCount(), sizeof(ui64) * batch.GetRecordsCount() });
    } else {
        cells.resize(batch.GetRecordsCount() * TotalColumnsCount);

        for (size_t columnIndex = 0; columnIndex < ResultColumns.size(); ++columnIndex) {
            stats.AddStatistics(
                WriteColumnValuesFromArrow(cells.data(), batch, columnIndex, TotalColumnsCount, ResultColumns[columnIndex].Type)
            );
        }

        if (!SystemColumns.empty()) {
            for (i64 rowIndex = 0; rowIndex < batch.GetRecordsCount(); ++rowIndex) {
                FillSystemColumns(&cells[rowIndex * TotalColumnsCount + ResultColumns.size()], shardId, SystemColumns);
            }

            stats.AllocatedBytes += batch.GetRecordsCount() * SystemColumns.size() * sizeof(NUdf::TUnboxedValue);
        }
    }

    if (cells.size()) {
        RowBatches.emplace(TRowBatch(TotalColumnsCount, batch.GetRecordsCount(), std::move(cells), stats.AllocatedBytes));
    }

    StoredBytes += stats.AllocatedBytes;

    return stats;
}

std::shared_ptr<arrow::Array> AdoptArrowTypeToYQL(const std::shared_ptr<arrow::Array>& original) {
    if (original->type_id() == arrow::Type::TIMESTAMP) {
        auto timestamps = std::static_pointer_cast<arrow::TimestampArray>(original);
        auto ui64Data = std::make_shared<arrow::ArrayData>(arrow::TypeTraits<arrow::UInt64Type>::type_singleton(), original->length(), timestamps->data()->buffers);
        auto ui64Array = std::make_shared<arrow::UInt64Array>(ui64Data);
        auto timestampType = std::static_pointer_cast<arrow::TimestampType>(original->type());

        static arrow::Datum const1M(std::make_shared<arrow::UInt64Scalar>(1000000));
        static arrow::Datum const1K(std::make_shared<arrow::UInt64Scalar>(1000));

        switch (timestampType->unit()) {
        case arrow::TimeUnit::SECOND:
            return NArrow::TStatusValidator::GetValid(arrow::compute::Multiply(ui64Array, const1M)).make_array();
        case arrow::TimeUnit::MILLI:
            return NArrow::TStatusValidator::GetValid(arrow::compute::Multiply(ui64Array, const1K)).make_array();
        case arrow::TimeUnit::MICRO:
            return ui64Array;
        case arrow::TimeUnit::NANO:
            return NArrow::TStatusValidator::GetValid(arrow::compute::Divide(ui64Array, const1K)).make_array();
        }
    } else {
        return original;
    }
}

TBytesStatistics TKqpScanComputeContext::TScanData::TBlockBatchReader::AddData(const TBatchDataAccessor& dataAccessor, TMaybe<ui64> /*shardId*/,
    const THolderFactory& holderFactory)
{
    TBytesStatistics stats;
    auto totalColsCount = TotalColumnsCount + 1;
    auto batches = NArrow::SliceToRecordBatches(dataAccessor.GetFiltered());
    for (auto&& filtered : batches) {
        TUnboxedValueVector batchValues;
        batchValues.resize(totalColsCount);
        for (int i = 0; i < filtered->num_columns(); ++i) {
            batchValues[i] = holderFactory.CreateArrowBlock(arrow::Datum(AdoptArrowTypeToYQL(filtered->column(i))));
        }
        const ui64 batchByteSize = NArrow::GetBatchDataSize(filtered);
        stats.AddStatistics({batchByteSize, batchByteSize});

        // !!! TODO !!!
        // if (!SystemColumns.empty()) {
        //     for (i64 rowIndex = 0; rowIndex < batch.num_rows(); ++rowIndex) {
        //         FillSystemColumns(&cells[rowIndex * ColumnsCount() + ResultColumns.size()], shardId, SystemColumns);
        //     }

        //     stats.AllocatedBytes += batch.num_rows() * SystemColumns.size() * sizeof(NUdf::TUnboxedValue);
        // }

        batchValues[totalColsCount - 1] = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(filtered->num_rows())));
        stats.AddStatistics({sizeof(ui64) * filtered->num_rows(), sizeof(ui64) * filtered->num_rows()});

        BlockBatches.emplace(TBlockBatch(totalColsCount, filtered->num_rows(), std::move(batchValues), stats.AllocatedBytes));
        StoredBytes += stats.AllocatedBytes;
    }
    return stats;
}

ui64 TKqpScanComputeContext::TScanData::AddData(const TBatchDataAccessor& batch, TMaybe<ui64> shardId,
    const THolderFactory& holderFactory)
{
    // RecordBatch hasn't empty method so check the number of rows
    if (Finished || batch.GetRecordsCount() == 0) {
        return 0;
    }

    TBytesStatistics stats = BatchReader->AddData(batch, shardId, holderFactory);
    if (BasicStats) {
        BasicStats->Rows += batch.GetRecordsCount();
        BasicStats->Bytes += stats.DataBytes;
    }

    return stats.AllocatedBytes;
}

void TKqpScanComputeContext::AddTableScan(ui32, const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta,
    NYql::NDqProto::EDqStatsMode statsMode)
{
    auto scanData = TKqpScanComputeContext::TScanData(meta, statsMode);

    auto result = Scans.emplace(0, std::move(scanData));
    Y_ENSURE(result.second);
}

TKqpScanComputeContext::TScanData& TKqpScanComputeContext::GetTableScan(ui32) {
    auto scanData = Scans.FindPtr(0);
    Y_ENSURE(scanData);

    return *scanData;
}

TMap<ui32, TKqpScanComputeContext::TScanData>& TKqpScanComputeContext::GetTableScans() {
    return Scans;
}

const TMap<ui32, TKqpScanComputeContext::TScanData>& TKqpScanComputeContext::GetTableScans() const {
    return Scans;
}

TIntrusivePtr<IKqpTableReader> TKqpScanComputeContext::ReadTable(ui32) const {
    auto scanData = Scans.FindPtr(0);
    Y_ENSURE(scanData);
    Y_ENSURE(scanData->TableReader);

    return scanData->TableReader;
}

class TKqpTableReader : public IKqpTableReader {
public:
    TKqpTableReader(TKqpScanComputeContext::TScanData& scanData)
        : ScanData(scanData)
    {}

    NUdf::EFetchStatus Next(NUdf::TUnboxedValue& /*result*/) override {
        if (ScanData.IsEmpty()) {
            if (ScanData.IsFinished()) {
                return NUdf::EFetchStatus::Finish;
            }
            return NUdf::EFetchStatus::Yield;
        }

        Y_ABORT_UNLESS(false);
//        result = std::move(ScanData.BuildNextDirectArrayHolder());
        return NUdf::EFetchStatus::Ok;
    }

    EFetchResult Next(NUdf::TUnboxedValue* const* result) override {
        if (ScanData.IsEmpty()) {
            if (ScanData.IsFinished()) {
                return EFetchResult::Finish;
            }
            return EFetchResult::Yield;
        }

        ScanData.FillDataValues(result);
        return EFetchResult::One;
    }

private:
    TKqpScanComputeContext::TScanData& ScanData;
};

TIntrusivePtr<IKqpTableReader> CreateKqpTableReader(TKqpScanComputeContext::TScanData& scanData) {
    return MakeIntrusive<TKqpTableReader>(scanData);
}

} // namespace NMiniKQL
} // namespace NKikimr
