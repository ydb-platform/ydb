#include "arrow_batch_builder.h"
#include "switch/switch_type.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
namespace NKikimr::NArrow {

namespace {

template <typename T>
arrow::Status AppendCell(arrow::NumericBuilder<T>& builder, const TCell& cell) {
    if (cell.IsNull()) {
        return builder.AppendNull();
    }
    return builder.Append(cell.AsValue<typename T::c_type>());
}

arrow::Status AppendCell(arrow::BooleanBuilder& builder, const TCell& cell) {
    if (cell.IsNull()) {
        return builder.AppendNull();
    }
    return builder.Append(cell.AsValue<ui8>());
}

arrow::Status AppendCell(arrow::BinaryBuilder& builder, const TCell& cell) {
    if (cell.IsNull()) {
        return builder.AppendNull();
    }
    return builder.Append(cell.Data(), cell.Size());
}

arrow::Status AppendCell(arrow::StringBuilder& builder, const TCell& cell) {
    if (cell.IsNull()) {
        return builder.AppendNull();
    }
    return builder.Append(cell.Data(), cell.Size());
}

arrow::Status AppendCell(arrow::Decimal128Builder& builder, const TCell& cell) {
    if (cell.IsNull()) {
        return builder.AppendNull();
    }

    /// @warning There's no conversion for special YQL Decimal valies here,
    /// so we could convert them to Arrow and back but cannot calculate anything on them.
    /// We need separate Arrow.Decimal, YQL.Decimal, CH.Decimal and YDB.Decimal in future.
    return builder.Append(cell.Data());
}

template <typename TDataType>
arrow::Status AppendCell(arrow::RecordBatchBuilder& builder, const TCell& cell, ui32 colNum) {
    using TBuilderType = typename arrow::TypeTraits<TDataType>::BuilderType;
    return AppendCell(*builder.GetFieldAs<TBuilderType>(colNum), cell);
}

arrow::Status AppendCell(arrow::RecordBatchBuilder& builder, const TCell& cell, ui32 colNum, NScheme::TTypeInfo type) {
    arrow::Status result;
    auto callback = [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
        Y_UNUSED(typeHolder);
        result = AppendCell<TType>(builder, cell, colNum);
        return true;
    };
    auto success = SwitchYqlTypeToArrowType(type, std::move(callback));
    if(!success) {
        return arrow::Status::TypeError("Unsupported type");
    }
    return result;
}

}

NKikimr::NArrow::TRecordBatchConstructor::TRecordConstructor& TRecordBatchConstructor::TRecordConstructor::AddRecordValue(
    const std::shared_ptr<arrow::Scalar>& value)
{
    Y_ABORT_UNLESS(CurrentBuilder != Owner.Builders.end());
    AddValueToBuilder(**CurrentBuilder, value, WithCast);
    ++CurrentBuilder;
    return *this;
}

void TRecordBatchConstructor::AddValueToBuilder(arrow::ArrayBuilder& builder,
    const std::shared_ptr<arrow::Scalar>& value, const bool withCast) {
    if (!value) {
        Y_ABORT_UNLESS(builder.AppendNull().ok());
    } else if (!withCast) {
        Y_ABORT_UNLESS(builder.AppendScalar(*value).ok());
    } else {
        auto castStatus = value->CastTo(builder.type());
        Y_ABORT_UNLESS(castStatus.ok());
        Y_ABORT_UNLESS(builder.AppendScalar(*castStatus.ValueUnsafe()).ok());
    }
}

NKikimr::NArrow::TRecordBatchConstructor& TRecordBatchConstructor::AddRecordsBatchSlow(
    const std::shared_ptr<arrow::RecordBatch>& value, const bool withCast /*= false*/, const bool withRemap /*= false*/)
{
    Y_ABORT_UNLESS(!!value);
    Y_ABORT_UNLESS(!!Schema);
    Y_ABORT_UNLESS(!InConstruction);
    std::vector<std::shared_ptr<arrow::Array>> batchColumns;
    if (withRemap) {
        for (auto&& f : Schema->fields()) {
            batchColumns.emplace_back(value->GetColumnByName(f->name()));
        }
    } else {
        Y_ABORT_UNLESS(value->num_columns() <= Schema->num_fields());
        for (auto&& i : value->columns()) {
            batchColumns.emplace_back(i);
        }
        for (i32 i = value->num_columns(); i < Schema->num_fields(); ++i) {
            batchColumns.emplace_back(nullptr);
        }
    }
    Y_ABORT_UNLESS((int)batchColumns.size() == Schema->num_fields());
    Y_ABORT_UNLESS((int)Builders.size() == Schema->num_fields());
    std::vector<std::unique_ptr<arrow::ArrayBuilder>>::const_iterator currentBuilder = Builders.begin();
    for (auto&& c : batchColumns) {
        if (!c) {
            Y_ABORT_UNLESS((*currentBuilder)->AppendNulls(value->num_rows()).ok());
        } else {
            for (ui32 r = 0; r < value->num_rows(); ++r) {
                std::shared_ptr<arrow::Scalar> value;
                if (c->IsNull(r)) {
                    value = nullptr;
                } else {
                    auto statusGet = c->GetScalar(r);
                    Y_ABORT_UNLESS(statusGet.ok());
                    value = statusGet.ValueUnsafe();
                }
                AddValueToBuilder(**currentBuilder, value, withCast);
            }
        }
        ++currentBuilder;
    }
    RecordsCount += value->num_rows();
    return *this;
}

NKikimr::NArrow::TRecordBatchConstructor& TRecordBatchConstructor::InitColumns(const std::shared_ptr<arrow::Schema>& schema) {
    Schema = schema;
    Builders.clear();
    Builders.reserve(Schema->num_fields());
    for (auto&& f : Schema->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> arrayBuilder;
        Y_ABORT_UNLESS(arrow::MakeBuilder(arrow::default_memory_pool(), f->type(), &arrayBuilder).ok());
        Builders.emplace_back(std::move(arrayBuilder));
    }
    return *this;
}

NKikimr::NArrow::TRecordBatchReader TRecordBatchConstructor::Finish() {
    Y_ABORT_UNLESS(!InConstruction);
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(Builders.size());
    for (auto&& i : Builders) {
        arrow::Result<std::shared_ptr<arrow::Array>> aData = i->Finish();
        Y_ABORT_UNLESS(aData.ok());
        columns.emplace_back(aData.ValueUnsafe());
    }
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(Schema, RecordsCount, columns);
#if !defined(NDEBUG)
    auto statusValidation = batch->ValidateFull();
    if (!statusValidation.ok()) {
        Cerr << statusValidation.ToString() << "/" << statusValidation.message() << Endl;
        Y_ABORT_UNLESS(false);
    }
#endif

    return TRecordBatchReader(batch);
}

void TRecordBatchReader::SerializeToStrings(TString& schema, TString& data) const {
    Y_ABORT_UNLESS(!!Batch);
    schema = NArrow::SerializeSchema(*Batch->schema());
    data = NArrow::SerializeBatchNoCompression(Batch);
}

bool TRecordBatchReader::DeserializeFromStrings(const TString& schemaString, const TString& dataString) {
    std::shared_ptr<arrow::Schema> schema = NArrow::DeserializeSchema(schemaString);
    if (!schema) {
        return false;
    }
    Batch = NArrow::DeserializeBatch(dataString, schema);
    return true;
}

TArrowBatchBuilder::TArrowBatchBuilder(arrow::Compression::type codec, const std::set<std::string>& notNullColumns)
    : WriteOptions(arrow::ipc::IpcWriteOptions::Defaults())
    , NotNullColumns(notNullColumns)
{
    Y_ABORT_UNLESS(arrow::util::Codec::IsAvailable(codec));
    auto resCodec = arrow::util::Codec::Create(codec);
    Y_ABORT_UNLESS(resCodec.ok());

    WriteOptions.codec.reset((*resCodec).release());
    WriteOptions.use_threads = false;
}

arrow::Status TArrowBatchBuilder::Start(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbColumns) {
    YdbSchema = ydbColumns;
    auto schema = MakeArrowSchema(ydbColumns, NotNullColumns);
    if (!schema.ok()) {
        return arrow::Status::FromArgs(schema.status().code(), "Cannot make arrow schema: ", schema.status().ToString());
    }
    auto status = arrow::RecordBatchBuilder::Make(*schema, arrow::default_memory_pool(), RowsToReserve, &BatchBuilder);
    NumRows = NumBytes = 0;
    if (!status.ok()) {
        return arrow::Status::FromArgs(schema.status().code(), "Cannot make arrow builder: ", status.ToString());
    }
    return arrow::Status::OK();
}

void TArrowBatchBuilder::AppendCell(const TCell& cell, ui32 colNum) {
    NumBytes += cell.Size();
    auto ydbType = YdbSchema[colNum].second;
    auto status = NKikimr::NArrow::AppendCell(*BatchBuilder, cell, colNum, ydbType);
    Y_ABORT_UNLESS(status.ok(), "Faield to append cell: %s", status.ToString().c_str());
}

void TArrowBatchBuilder::AddRow(const TDbTupleRef& key, const TDbTupleRef& value) {
    ++NumRows;

    auto fnAppendTuple = [&] (const TDbTupleRef& tuple, size_t offsetInRow) {
        for (size_t i = 0; i < tuple.ColumnCount; ++i) {
            auto ydbType = tuple.Types[i];
            const ui32 colNum =  offsetInRow + i;
            Y_ABORT_UNLESS(ydbType == YdbSchema[colNum].second);
            auto& cell = tuple.Columns[i];
            AppendCell(cell, colNum);
        }
    };

    fnAppendTuple(key, 0);
    fnAppendTuple(value, key.ColumnCount);
}

void TArrowBatchBuilder::AddRow(const TConstArrayRef<TCell>& key, const TConstArrayRef<TCell>& value) {
    ++NumRows;

    size_t offset = 0;
    for (size_t i = 0; i < key.size(); ++i, ++offset) {
        auto& cell = key[i];
        AppendCell(cell, offset);
    }
    for (size_t i = 0; i < value.size(); ++i, ++offset) {
        auto& cell = value[i];
        AppendCell(cell, offset);
    }
}

void TArrowBatchBuilder::AddRow(const TConstArrayRef<TCell>& row) {
    ++NumRows;

    size_t offset = 0;
    for (size_t i = 0; i < row.size(); ++i, ++offset) {
        auto& cell = row[i];
        AppendCell(cell, offset);
    }
}

void TArrowBatchBuilder::ReserveData(ui32 columnNo, size_t size) {
    if (!BatchBuilder || columnNo >= (ui32)BatchBuilder->num_fields()) {
        return;
    }

    Y_ABORT_UNLESS(columnNo < YdbSchema.size());
    auto type = YdbSchema[columnNo].second;

    Y_ABORT_UNLESS(SwitchYqlTypeToArrowType(type, [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;

        if constexpr (std::is_same_v<typename TWrap::T, arrow::StringType> ||
                      std::is_same_v<typename TWrap::T, arrow::BinaryType>)
        {
            auto status = BatchBuilder->GetFieldAs<TBuilder>(columnNo)->ReserveData(size);
            Y_ABORT_UNLESS(status.ok());
        }
        return true;
    }));
}

std::shared_ptr<arrow::RecordBatch> TArrowBatchBuilder::FlushBatch(bool reinitialize) {
    if (NumRows) {
        auto status = BatchBuilder->Flush(reinitialize, &Batch);
        Y_ABORT_UNLESS(status.ok(), "Failed to flush batch: %s", status.ToString().c_str());
    }
    NumRows = NumBytes = 0;
    return Batch;
}

TString TArrowBatchBuilder::Finish() {
    if (!Batch) {
        FlushBatch(false);
    }

    TString str = NArrow::SerializeBatch(Batch, WriteOptions);
    Batch.reset();
    return str;
}

std::shared_ptr<arrow::RecordBatch> CreateNoColumnsBatch(ui64 rowsCount) {
    auto field = std::make_shared<arrow::Field>("", std::make_shared<arrow::NullType>());
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>({field}));
    std::unique_ptr<arrow::RecordBatchBuilder> batchBuilder;
    auto status = arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), &batchBuilder);
    Y_DEBUG_ABORT_UNLESS(status.ok(), "Failed to create BatchBuilder: %s", status.ToString().c_str());
    status = batchBuilder->GetFieldAs<arrow::NullBuilder>(0)->AppendNulls(rowsCount);
    Y_DEBUG_ABORT_UNLESS(status.ok(), "Failed to Append nulls: %s", status.ToString().c_str());
    std::shared_ptr<arrow::RecordBatch> batch;
    status = batchBuilder->Flush(&batch);
    Y_DEBUG_ABORT_UNLESS(status.ok(), "Failed to Flush Batch: %s", status.ToString().c_str());
    return batch;
}

}
