#include "kqp_result_set_builders.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/util/compression.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/result_set_format/kqp_formats_arrow.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr::NKqp::NFormats {

using TArrowSchemaColumns = std::vector<std::pair<TString, NMiniKQL::TType*>>;
using TArrowNotNullColumns = std::set<std::string>;
using TArrowSchema = std::pair<TArrowSchemaColumns, TArrowNotNullColumns>;

namespace {

TArrowSchema GetArrowSchema(const NMiniKQL::TType* mkqlItemType, const TVector<ui32>* columnOrder, const TVector<TString>* columnHints) {
    TArrowSchemaColumns arrowSchemaColumns;
    TArrowNotNullColumns arrowNotNullColumns;

    const auto* mkqlSrcRowStructType = static_cast<const NMiniKQL::TStructType*>(mkqlItemType);
    for (ui32 idx = 0; idx < mkqlSrcRowStructType->GetMembersCount(); ++idx) {
        ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? idx : (*columnOrder)[idx];
        auto columnName = columnHints && columnHints->size()
            ? columnHints->at(idx)
            : TString(mkqlSrcRowStructType->GetMemberName(memberIndex));
        auto* columnType = mkqlSrcRowStructType->GetMemberType(memberIndex);

        if (columnType->GetKind() != NMiniKQL::TType::EKind::Optional) {
            arrowNotNullColumns.insert(columnName);
        }

        arrowSchemaColumns.emplace_back(std::move(columnName), std::move(columnType));
    }

    return {arrowSchemaColumns, arrowNotNullColumns};
}

std::shared_ptr<arrow::RecordBatch> BuildArrowFromUnboxedValue(Ydb::ResultSet* ydbResult, const NMiniKQL::TUnboxedValueBatch& rows,
    const NMiniKQL::TType* mkqlItemType, const TVector<ui32>* columnOrder, const TVector<TString>* columnHints, TMaybe<ui64> rowsLimitPerWrite)
{
    auto [arrowSchemaColumns, arrowNotNullColumns] = GetArrowSchema(mkqlItemType, columnOrder, columnHints);

    NKikimr::NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::UNCOMPRESSED, arrowNotNullColumns);
    batchBuilder.Reserve(rows.RowCount());
    YQL_ENSURE(batchBuilder.Start(arrowSchemaColumns).ok());

    rows.ForEachRow([&](const NUdf::TUnboxedValue& row) -> bool {
        if (rowsLimitPerWrite) {
            if (*rowsLimitPerWrite == 0) {
                ydbResult->set_truncated(true);
                return false;
            }
            --(*rowsLimitPerWrite);
        }
        batchBuilder.AddRow(row, arrowSchemaColumns.size(), columnOrder);
        return true;
    });

    return batchBuilder.FlushBatch(false, /* flushEmpty */ true);
}

std::shared_ptr<arrow::RecordBatch> BuildArrowFromSerializedBatches(const NYql::NDq::TDqDataSerializer& dataSerializer,
    TVector<NYql::NDq::TDqSerializedBatch>&& data, const NMiniKQL::TType* mkqlItemType, const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints)
{
    auto [arrowSchemaColumns, arrowNotNullColumns] = GetArrowSchema(mkqlItemType, columnOrder, columnHints);

    ui32 rowsCount = 0;
    for (const auto& part : data) {
        if (part.ChunkCount()) {
            rowsCount += part.RowCount();
        }
    }

    NKikimr::NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::UNCOMPRESSED, arrowNotNullColumns);
    batchBuilder.Reserve(rowsCount);
    YQL_ENSURE(batchBuilder.Start(arrowSchemaColumns).ok());

    for (auto& part : data) {
        if (!part.ChunkCount()) {
            continue;
        }

        NMiniKQL::TUnboxedValueBatch rows(mkqlItemType);
        dataSerializer.Deserialize(std::move(part), mkqlItemType, rows);

        rows.ForEachRow([&](const NUdf::TUnboxedValue& value) {
            batchBuilder.AddRow(value, arrowSchemaColumns.size(), columnOrder);
        });
    }

    return batchBuilder.FlushBatch(false, /* flushEmpty */ true);
}

void FillValueSchema(Ydb::ResultSet* ydbResult, const NMiniKQL::TType* mkqlItemType, const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints)
{
    const auto* mkqlSrcRowStructType = static_cast<const NMiniKQL::TStructType* >(mkqlItemType);
    for (ui32 idx = 0; idx < mkqlSrcRowStructType->GetMembersCount(); ++idx) {
        auto* column = ydbResult->add_columns();
        ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? idx : (*columnOrder)[idx];

        auto columnName = TString(columnHints && columnHints->size()
            ? columnHints->at(idx)
            : mkqlSrcRowStructType->GetMemberName(memberIndex));
        auto* columnType = mkqlSrcRowStructType->GetMemberType(memberIndex);

        column->set_name(columnName);
        ExportTypeToProto(columnType,*column->mutable_type());
    }
}

void FillValueResultSet(Ydb::ResultSet* ydbResult, const NMiniKQL::TUnboxedValueBatch& rows, NMiniKQL::TType* mkqlItemType, bool fillSchema,
    const TVector<ui32>* columnOrder, const TVector<TString>* columnHints, TMaybe<ui64> rowsLimitPerWrite)
{
    ydbResult->set_format(Ydb::ResultSet::FORMAT_VALUE);

    if (fillSchema) {
        FillValueSchema(ydbResult, mkqlItemType, columnOrder, columnHints);
    }

    rows.ForEachRow([&](const NUdf::TUnboxedValue& value) -> bool {
        if (rowsLimitPerWrite) {
            if (*rowsLimitPerWrite == 0) {
                ydbResult->set_truncated(true);
                return false;
            }
            --(*rowsLimitPerWrite);
        }
        ExportValueToProto(mkqlItemType, value, *ydbResult->add_rows(), columnOrder);
        return true;
    });
}

void FillValueResultSet(Ydb::ResultSet* ydbResult, const NYql::NDq::TDqDataSerializer& dataSerializer,
    TVector<NYql::NDq::TDqSerializedBatch>&& data, NMiniKQL::TType* mkqlItemType, bool fillSchema, const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints)
{
    ydbResult->set_format(Ydb::ResultSet::FORMAT_VALUE);

    if (fillSchema) {
        FillValueSchema(ydbResult, mkqlItemType, columnOrder, columnHints);
    }

    for (auto& part : data) {
        if (!part.ChunkCount()) {
            continue;
        }

        NMiniKQL::TUnboxedValueBatch rows(mkqlItemType);
        dataSerializer.Deserialize(std::move(part), mkqlItemType, rows);

        rows.ForEachRow([&](const NUdf::TUnboxedValue& value) {
            ExportValueToProto(mkqlItemType, value, *ydbResult->add_rows(), columnOrder);
        });
    }
}

void FillArrowResultSet(Ydb::ResultSet* ydbResult, std::shared_ptr<arrow::RecordBatch> batch, const NFormats::TFormatsSettings& settings,
    const NMiniKQL::TType* mkqlItemType, bool fillSchema, const TVector<ui32>* columnOrder, const TVector<TString>* columnHints)
{
    ydbResult->set_format(Ydb::ResultSet::FORMAT_ARROW);

    if (fillSchema) {
        FillValueSchema(ydbResult, mkqlItemType, columnOrder, columnHints);
    }

    auto writeOptions = arrow::ipc::IpcWriteOptions::Defaults();
    writeOptions.use_threads = false;
    if (auto arrowFormatSettings = settings.GetArrowSettings()) {
        arrowFormatSettings->FillWriteOptions(writeOptions);
    }

    TString serializedBatch = NKikimr::NArrow::SerializeBatch(batch, writeOptions);
    ydbResult->set_data(std::move(serializedBatch));

    TString serializedSchema;
    if (fillSchema) {
        serializedSchema = NKikimr::NArrow::SerializeSchema(*batch->schema());
    }
    ydbResult->mutable_arrow_format_meta()->set_schema(std::move(serializedSchema));
}

} // namespace

void BuildResultSetFromRows(Ydb::ResultSet* ydbResult, const NFormats::TFormatsSettings& settings, bool fillSchema,
    NMiniKQL::TType* mkqlItemType, const NMiniKQL::TUnboxedValueBatch& rows, const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints,TMaybe<ui64> rowsLimitPerWrite)
{
    YQL_ENSURE(ydbResult);
    YQL_ENSURE(!rows.IsWide());
    YQL_ENSURE(mkqlItemType->GetKind() == NMiniKQL::TType::EKind::Struct);

    if (settings.IsValueFormat()) {
        return FillValueResultSet(ydbResult, rows, mkqlItemType, fillSchema, columnOrder, columnHints, rowsLimitPerWrite);
    }

    if (settings.IsArrowFormat()) {
        auto batch = BuildArrowFromUnboxedValue(ydbResult, rows, mkqlItemType, columnOrder, columnHints, rowsLimitPerWrite);
        return FillArrowResultSet(ydbResult, std::move(batch), settings, mkqlItemType, fillSchema, columnOrder, columnHints);
    }

    YQL_ENSURE(false, "Unknown output format");
}

void BuildResultSetFromBatches(Ydb::ResultSet* ydbResult, const NFormats::TFormatsSettings& settings, bool fillSchema,
    NMiniKQL::TType* mkqlItemType, const NYql::NDq::TDqDataSerializer& dataSerializer, TVector<NYql::NDq::TDqSerializedBatch>&& data,
    const TVector<ui32>* columnOrder, const TVector<TString>* columnHints)
{
    YQL_ENSURE(ydbResult);
    YQL_ENSURE(mkqlItemType && mkqlItemType->GetKind() == NMiniKQL::TType::EKind::Struct);

    if (settings.IsValueFormat()) {
        return FillValueResultSet(ydbResult, dataSerializer, std::move(data), mkqlItemType, fillSchema, columnOrder, columnHints);
    }

    if (settings.IsArrowFormat()) {
        auto batch = BuildArrowFromSerializedBatches(dataSerializer, std::move(data), mkqlItemType, columnOrder, columnHints);
        return FillArrowResultSet(ydbResult, std::move(batch), settings, mkqlItemType, fillSchema, columnOrder, columnHints);
    }

    YQL_ENSURE(false, "Unknown output format");
}

} // namespace NKikimr::NKqp::NFormats
