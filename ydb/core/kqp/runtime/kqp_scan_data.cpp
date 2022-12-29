#include "kqp_scan_data.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NKikimr {
namespace NMiniKQL {

TBytesStatistics GetUnboxedValueSize(const NUdf::TUnboxedValue& value, NScheme::TTypeInfo type) {
    namespace NTypeIds = NScheme::NTypeIds;
    if (!value) {
        return {sizeof(NUdf::TUnboxedValue), 8}; // Special value for NULL elements
    }
    switch (type.GetTypeId()) {
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
        case NTypeIds::ActorId:
        case NTypeIds::StepOrderId: {
            YQL_ENSURE(value.IsEmbedded(), "Passed wrong type: " << NScheme::TypeName(type.GetTypeId()));
            return {sizeof(NUdf::TUnboxedValue), sizeof(i64)};
        }
        case NTypeIds::Decimal:
        {
            YQL_ENSURE(value.IsEmbedded(), "Passed wrong type: " << NScheme::TypeName(type.GetTypeId()));
            return {sizeof(NUdf::TUnboxedValue), sizeof(NYql::NDecimal::TInt128)};
        }
        case NTypeIds::String:
        case NTypeIds::Utf8:
        case NTypeIds::Json:
        case NTypeIds::Yson:
        case NTypeIds::JsonDocument:
        case NTypeIds::DyNumber:
        case NTypeIds::PairUi64Ui64: {
            if (value.IsEmbedded()) {
                return {sizeof(NUdf::TUnboxedValue), std::max((ui32) 8, value.AsStringRef().Size())};
            } else {
                Y_VERIFY_DEBUG_S(8 < value.AsStringRef().Size(), "Small string of size " << value.AsStringRef().Size() << " is not embedded.");
                return {sizeof(NUdf::TUnboxedValue) + value.AsStringRef().Size(), value.AsStringRef().Size()};
            }
        }

        case NTypeIds::Pg:
            // TODO: support pg types
            YQL_ENSURE(false, "pg types are not supported");

        default:
            Y_VERIFY_DEBUG_S(false, "Unsupported type " << NScheme::TypeName(type.GetTypeId()));
            if (value.IsEmbedded()) {
                return {sizeof(NUdf::TUnboxedValue), sizeof(NUdf::TUnboxedValue)};
            } else {
                return {sizeof(NUdf::TUnboxedValue) + value.AsStringRef().Size(), value.AsStringRef().Size()};
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
    const TSmallVec<TKqpComputeContextBase::TColumn>& systemColumns)
{
    TBytesStatistics rowStats{systemColumns.size() * sizeof(NUdf::TUnboxedValue), 0};
    for (size_t columnIndex = 0; columnIndex < columns.size(); ++columnIndex) {
        rowStats.AddStatistics(GetUnboxedValueSize(row[columnIndex], columns[columnIndex].Type));
    }
    if (columns.empty()) {
        rowStats.AddStatistics({sizeof(ui64), sizeof(ui64)});
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

NUdf::TUnboxedValue MakeUnboxedValueFromBinaryData(arrow::Array* column, ui32 row) {
    auto array = reinterpret_cast<arrow::BinaryArray*>(column);
    auto data = array->GetView(row);
    return MakeString(NUdf::TStringRef(data.data(), data.size()));
}

NUdf::TUnboxedValue MakeUnboxedValueFromFixedSizeBinaryData(arrow::Array* column, ui32 row) {
    auto array = reinterpret_cast<arrow::FixedSizeBinaryArray*>(column);
    auto data = array->GetView(row);
    return MakeString(NUdf::TStringRef(data.data(), data.size()-1));
}

NUdf::TUnboxedValue MakeUnboxedValueFromDecimal128Array(arrow::Array* column, ui32 row) {
    auto array = reinterpret_cast<arrow::Decimal128Array*>(column);
    auto data = array->GetView(row);
    // It's known that Decimal params are always Decimal(22,9),
    // so we verify Decimal type here before store it in UnboxedValue.
    const auto& type = arrow::internal::checked_cast<const arrow::Decimal128Type&>(*array->type());
    YQL_ENSURE(type.precision() == NScheme::DECIMAL_PRECISION, "Unsupported Decimal precision.");
    YQL_ENSURE(type.scale() == NScheme::DECIMAL_SCALE, "Unsupported Decimal scale.");
    YQL_ENSURE(data.size() == sizeof(NYql::NDecimal::TInt128), "Wrong data size");
    NYql::NDecimal::TInt128 val;
    std::memcpy(reinterpret_cast<char*>(&val), data.data(), data.size());
    return NUdf::TUnboxedValuePod(val);
}

} // namespace

TBytesStatistics WriteColumnValuesFromArrow(const TVector<NUdf::TUnboxedValue*>& editAccessors,
    const arrow::RecordBatch& batch, i64 columnIndex, NScheme::TTypeInfo columnType)
{
    TBytesStatistics columnStats;
    // Hold pointer to column until function end
    std::shared_ptr<arrow::Array> columnSharedPtr = batch.column(columnIndex);
    arrow::Array* columnPtr = columnSharedPtr.get();
    namespace NTypeIds = NScheme::NTypeIds;
    for (i64 rowIndex = 0; rowIndex < batch.num_rows(); ++rowIndex) {
        auto& rowItem = editAccessors[rowIndex][columnIndex];
        if (columnPtr->IsNull(rowIndex)) {
            rowItem = NUdf::TUnboxedValue();
        } else {
            switch(columnType.GetTypeId()) {
                case NTypeIds::Bool: {
                    rowItem = MakeUnboxedValue<arrow::BooleanArray, bool>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Int8: {
                    rowItem = MakeUnboxedValue<arrow::Int8Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Int16: {
                    rowItem = MakeUnboxedValue<arrow::Int16Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Int32: {
                    rowItem = MakeUnboxedValue<arrow::Int32Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Int64: {
                    rowItem = MakeUnboxedValue<arrow::Int64Array, i64>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Uint8: {
                    rowItem = MakeUnboxedValue<arrow::UInt8Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Uint16: {
                    rowItem = MakeUnboxedValue<arrow::UInt16Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Uint32: {
                    rowItem = MakeUnboxedValue<arrow::UInt32Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Uint64: {
                    rowItem = MakeUnboxedValue<arrow::UInt64Array, ui64>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Float: {
                    rowItem = MakeUnboxedValue<arrow::FloatArray>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Double: {
                    rowItem = MakeUnboxedValue<arrow::DoubleArray>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::String:
                case NTypeIds::Utf8:
                case NTypeIds::Json:
                case NTypeIds::Yson:
                case NTypeIds::JsonDocument:
                case NTypeIds::DyNumber: {
                    rowItem = MakeUnboxedValueFromBinaryData(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Date: {
                    rowItem = MakeUnboxedValue<arrow::UInt16Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Datetime: {
                    rowItem = MakeUnboxedValue<arrow::UInt32Array>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Timestamp: {
                    rowItem = MakeUnboxedValue<arrow::TimestampArray, ui64>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Interval: {
                    rowItem = MakeUnboxedValue<arrow::DurationArray, ui64>(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Decimal: {
                    rowItem = MakeUnboxedValueFromDecimal128Array(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::PairUi64Ui64:
                case NTypeIds::ActorId:
                case NTypeIds::StepOrderId: {
                    Y_VERIFY_DEBUG_S(false, "Unsupported (deprecated) type: " << NScheme::TypeName(columnType.GetTypeId()));
                    rowItem = MakeUnboxedValueFromFixedSizeBinaryData(columnPtr, rowIndex);
                    break;
                }
                case NTypeIds::Pg:
                    // TODO: support pg types
                    YQL_ENSURE(false, "Unsupported pg type at column " << columnIndex);

                default:
                    YQL_ENSURE(false, "Unsupported type: " << NScheme::TypeName(columnType.GetTypeId()) << " at column " << columnIndex);
            }
        }
        columnStats.AddStatistics(GetUnboxedValueSize(rowItem, columnType));
    }
    return columnStats;
}



std::pair<ui64, ui64> GetUnboxedValueSizeForTests(const NUdf::TUnboxedValue& value, NScheme::TTypeInfo type) {
    auto sizes = GetUnboxedValueSize(value, type);
    return {sizes.AllocatedBytes, sizes.DataBytes};
}

TKqpScanComputeContext::TScanData::TScanData(const TTableId& tableId, const TTableRange& range,
    const TSmallVec<TColumn>& columns, const TSmallVec<TColumn>& systemColumns, const TSmallVec<bool>& skipNullKeys,
    const TSmallVec<TColumn>& resultColumns)
    : TableId(tableId)
    , Range(range)
    , SkipNullKeys(skipNullKeys)
    , Columns(columns)
    , SystemColumns(systemColumns)
    , ResultColumns(resultColumns)
{}

TKqpScanComputeContext::TScanData::TScanData(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta,
    NYql::NDqProto::EDqStatsMode statsMode)
{
    const auto& tableMeta = meta.GetTable();
    TableId = TTableId(tableMeta.GetTableId().GetOwnerId(), tableMeta.GetTableId().GetTableId(),
                       tableMeta.GetSysViewInfo(), tableMeta.GetSchemaVersion());
    TablePath = meta.GetTable().GetTablePath();

    std::copy(meta.GetSkipNullKeys().begin(), meta.GetSkipNullKeys().end(), std::back_inserter(SkipNullKeys));

    Columns.reserve(meta.GetColumns().size());
    for (const auto& column : meta.GetColumns()) {
        NMiniKQL::TKqpScanComputeContext::TColumn c;
        c.Tag = column.GetId();
        c.Type = NScheme::TypeInfoFromProtoColumnType(column.GetType(),
            column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);

        if (!IsSystemColumn(c.Tag)) {
            Columns.emplace_back(std::move(c));
        } else {
            SystemColumns.emplace_back(std::move(c));
        }
    }

    if (meta.GetResultColumns().empty() && !meta.HasOlapProgram()) {
        // Currently we define ResultColumns just for Olap tables in TKqpQueryCompiler
        ResultColumns = Columns;
    } else {
        ResultColumns.reserve(meta.GetResultColumns().size());
        for (const auto& resColumn : meta.GetResultColumns()) {
            NMiniKQL::TKqpScanComputeContext::TColumn c;
            c.Tag = resColumn.GetId();
            c.Type = NScheme::TypeInfoFromProtoColumnType(resColumn.GetType(),
                resColumn.HasTypeInfo() ? &resColumn.GetTypeInfo() : nullptr);

            if (!IsSystemColumn(c.Tag)) {
                ResultColumns.emplace_back(std::move(c));
            } else {
                SystemColumns.emplace_back(std::move(c));
            }
        }
    }

    if (statsMode >= NYql::NDqProto::DQ_STATS_MODE_BASIC) {
        BasicStats = std::make_unique<TBasicStats>();
    }
    if (Y_UNLIKELY(statsMode >= NYql::NDqProto::DQ_STATS_MODE_PROFILE)) {
        ProfileStats = std::make_unique<TProfileStats>();
    }
}


ui64 TKqpScanComputeContext::TScanData::AddRows(const TVector<TOwnedCellVec>& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) {
    if (Finished || batch.empty()) {
        return 0;
    }

    TBytesStatistics stats;

    TVector<ui64> bytesList;
    bytesList.reserve(batch.size());

    TUnboxedValueVector rows;
    rows.reserve(batch.size());

    for (size_t rowIndex = 0; rowIndex < batch.size(); ++rowIndex) {
        auto& row = batch[rowIndex];

        // Convert row into an UnboxedValue
        NUdf::TUnboxedValue* rowItems = nullptr;
        rows.emplace_back(holderFactory.CreateDirectArrayHolder(ResultColumns.size() + SystemColumns.size(), rowItems));
        for (ui32 i = 0; i < ResultColumns.size(); ++i) {
            rowItems[i] = GetCellValue(row[i], ResultColumns[i].Type);
        }
        FillSystemColumns(&rowItems[ResultColumns.size()], shardId, SystemColumns);

        stats.AddStatistics(GetRowSize(rowItems, ResultColumns, SystemColumns));
    }
    RowBatches.emplace(RowBatch{std::move(rows), shardId});

    StoredBytes += stats.AllocatedBytes;
    if (BasicStats) {
        BasicStats->Rows += batch.size();
        BasicStats->Bytes += stats.DataBytes;
    }

    return stats.AllocatedBytes;
}

ui64 TKqpScanComputeContext::TScanData::AddRows(const arrow::RecordBatch& batch, TMaybe<ui64> shardId,
    const THolderFactory& holderFactory)
{
    // RecordBatch hasn't empty method so check the number of rows
    if (Finished || batch.num_rows() == 0) {
        return 0;
    }

    TBytesStatistics stats;
    TUnboxedValueVector rows;

    if (ResultColumns.empty() && SystemColumns.empty()) {
        rows.resize(batch.num_rows(), holderFactory.GetEmptyContainer());
    } else {
        TVector<NUdf::TUnboxedValue*> editAccessors(batch.num_rows());
        rows.reserve(batch.num_rows());

        for (i64 rowIndex = 0; rowIndex < batch.num_rows(); ++rowIndex) {
            rows.emplace_back(holderFactory.CreateDirectArrayHolder(
                ResultColumns.size() + SystemColumns.size(),
                editAccessors[rowIndex])
            );
        }

        for (size_t columnIndex = 0; columnIndex < ResultColumns.size(); ++columnIndex) {
            stats.AddStatistics(
                WriteColumnValuesFromArrow(editAccessors, batch, columnIndex, ResultColumns[columnIndex].Type)
            );
        }

        if (!SystemColumns.empty()) {
            for (i64 rowIndex = 0; rowIndex < batch.num_rows(); ++rowIndex) {
                FillSystemColumns(&editAccessors[rowIndex][ResultColumns.size()], shardId, SystemColumns);
            }

            stats.AllocatedBytes += batch.num_rows() * SystemColumns.size() * sizeof(NUdf::TUnboxedValue);
        }
    }

    if (ResultColumns.empty()) {
        stats.AddStatistics({sizeof(ui64) * batch.num_rows(), sizeof(ui64) * batch.num_rows()});
    }

    RowBatches.emplace(RowBatch{std::move(rows), shardId});

    StoredBytes += stats.AllocatedBytes;
    if (BasicStats) {
        BasicStats->Rows += batch.num_rows();
        BasicStats->Bytes += stats.DataBytes;
    }

    return stats.AllocatedBytes;
}

NUdf::TUnboxedValue TKqpScanComputeContext::TScanData::TakeRow() {
    YQL_ENSURE(!RowBatches.empty());
    auto& batch = RowBatches.front();
    auto row = std::move(batch.Batch[batch.CurrentRow++]);
    auto rowStats = GetRowSize(row.GetElements(), ResultColumns, SystemColumns);

    StoredBytes -= rowStats.AllocatedBytes;
    if (batch.CurrentRow == batch.Batch.size()) {
        RowBatches.pop();
    }
    YQL_ENSURE(RowBatches.empty() == (StoredBytes == 0), "StoredBytes miscalculated!");
    return row;
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

    NUdf::EFetchStatus Next(NUdf::TUnboxedValue& result) override {
        if (ScanData.IsEmpty()) {
            if (ScanData.IsFinished()) {
                return NUdf::EFetchStatus::Finish;
            }
            return NUdf::EFetchStatus::Yield;
        }

        result = std::move(ScanData.TakeRow());
        return NUdf::EFetchStatus::Ok;
    }

    EFetchResult Next(NUdf::TUnboxedValue* const* result) override {
        if (ScanData.IsEmpty()) {
            if (ScanData.IsFinished()) {
                return EFetchResult::Finish;
            }
            return EFetchResult::Yield;
        }

        auto row = ScanData.TakeRow();
        for (ui32 i = 0; i < ScanData.GetResultColumns().size() + ScanData.GetSystemColumns().size(); ++i) {
            if (result[i]) {
                *result[i] = std::move(row.GetElement(i));
            }
        }

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
