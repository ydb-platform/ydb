#include "value_consumer.h"
#include "helpers.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <util/string/cast.h>

namespace NYT::NTableClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool ConvertToBooleanValue(TStringBuf stringValue)
{
    if (stringValue == "true") {
        return true;
    } else if (stringValue == "false") {
        return false;
    } else {
        THROW_ERROR_EXCEPTION("Unable to convert value to boolean")
            << TErrorAttribute("value", stringValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

TValueConsumerBase::TValueConsumerBase(
    TTableSchemaPtr schema,
    TTypeConversionConfigPtr typeConversionConfig)
    : Schema_(std::move(schema))
    , TypeConversionConfig_(std::move(typeConversionConfig))
{ }

void TValueConsumerBase::InitializeIdToTypeMapping()
{
    const auto& nameTable = GetNameTable();
    for (const auto& column : Schema_->Columns()) {
        int id = nameTable->GetIdOrRegisterName(column.Name());
        if (id >= static_cast<int>(NameTableIdToType_.size())) {
            NameTableIdToType_.resize(id + 1, EValueType::Any);
        }
        NameTableIdToType_[id] = column.GetWireType();
    }
}

template <typename T>
void TValueConsumerBase::ProcessIntegralValue(const TUnversionedValue& value, EValueType columnType)
{
    auto integralValue = FromUnversionedValue<T>(value);
    if (TypeConversionConfig_->EnableAllToStringConversion && columnType == EValueType::String) {
        char buf[64];
        char* end = buf + 64;
        char* start = WriteDecIntToBufferBackwards(end, integralValue);
        OnMyValue(MakeUnversionedStringValue(TStringBuf(start, end), value.Id));
    } else if (TypeConversionConfig_->EnableIntegralToDoubleConversion && columnType == EValueType::Double) {
        OnMyValue(MakeUnversionedDoubleValue(static_cast<double>(integralValue), value.Id));
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::ProcessInt64Value(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableIntegralTypeConversion && columnType == EValueType::Uint64) {
        i64 integralValue = value.Data.Int64;
        if (integralValue < 0) {
            ThrowConversionException(
                value,
                columnType,
                TError("Unable to convert negative int64 to uint64")
                    << TErrorAttribute("value", integralValue));
        } else {
            OnMyValue(MakeUnversionedUint64Value(static_cast<ui64>(integralValue), value.Id));
        }
    } else {
        ProcessIntegralValue<i64>(value, columnType);
    }
}

void TValueConsumerBase::ProcessUint64Value(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableIntegralTypeConversion && columnType == EValueType::Int64) {
        ui64 integralValue = value.Data.Uint64;
        if (integralValue > std::numeric_limits<i64>::max()) {
            ThrowConversionException(
                value,
                columnType,
                TError("Unable to convert uint64 to int64 as it leads to an overflow")
                    << TErrorAttribute("value", integralValue));
        } else {
            OnMyValue(MakeUnversionedInt64Value(static_cast<i64>(integralValue), value.Id));
        }
    } else {
        ProcessIntegralValue<ui64>(value, columnType);
    }
}

void TValueConsumerBase::ProcessBooleanValue(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableAllToStringConversion && columnType == EValueType::String) {
        TStringBuf stringValue = value.Data.Boolean ? "true" : "false";
        OnMyValue(MakeUnversionedStringValue(stringValue, value.Id));
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::ProcessDoubleValue(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableAllToStringConversion && columnType == EValueType::String) {
        char buf[64];
        auto length = FloatToString(value.Data.Double, buf, sizeof(buf));
        TStringBuf stringValue(buf, buf + length);
        OnMyValue(MakeUnversionedStringValue(stringValue, value.Id));
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::ProcessStringValue(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableStringToAllConversion) {
        TUnversionedValue convertedValue;
        TStringBuf stringValue(value.Data.String, value.Length);
        try {
            switch (columnType) {
                case EValueType::Int64:
                case EValueType::Uint64: {
                    auto adjustedStringValue = stringValue;
                    if (!stringValue.empty() && stringValue.back() == 'u') {
                        adjustedStringValue = TStringBuf(value.Data.String, value.Length - 1);
                    }
                    if (columnType == EValueType::Int64) {
                        convertedValue = MakeUnversionedInt64Value(FromString<i64>(adjustedStringValue), value.Id);
                    } else {
                        convertedValue = MakeUnversionedUint64Value(FromString<ui64>(adjustedStringValue), value.Id);
                    }
                    break;
                }
                case EValueType::Double:
                    convertedValue = MakeUnversionedDoubleValue(FromString<double>(stringValue), value.Id);
                    break;
                case EValueType::Boolean:
                    convertedValue = MakeUnversionedBooleanValue(ConvertToBooleanValue(stringValue), value.Id);
                    break;
                default:
                    convertedValue = value;
                    break;
            }
        } catch (const std::exception& ex) {
            ThrowConversionException(value, columnType, TError(ex));
        }
        OnMyValue(convertedValue);
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::OnValue(const TUnversionedValue& value)
{
    EValueType columnType;
    if (NameTableIdToType_.size() <= value.Id) {
        columnType = EValueType::Any;
    } else {
        columnType = NameTableIdToType_[value.Id];
    }

    switch (value.Type) {
        case EValueType::Int64:
            ProcessInt64Value(value, columnType);
            break;
        case EValueType::Uint64:
            ProcessUint64Value(value, columnType);
            break;
        case EValueType::Boolean:
            ProcessBooleanValue(value, columnType);
            break;
        case EValueType::Double:
            ProcessDoubleValue(value, columnType);
            break;
        case EValueType::String:
            ProcessStringValue(value, columnType);
            break;
        default:
            OnMyValue(value);
            break;
    }
}

const TTableSchemaPtr& TValueConsumerBase::GetSchema() const
{
    return Schema_;
}

void TValueConsumerBase::ThrowConversionException(const TUnversionedValue& value, EValueType columnType, const TError& ex)
{
    THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation, "Error while performing type conversion")
        << ex
        << TErrorAttribute("column", GetNameTable()->GetName(value.Id))
        << TErrorAttribute("value_type", value.Type)
        << TErrorAttribute("column_type", columnType);
}

////////////////////////////////////////////////////////////////////////////////

TBuildingValueConsumer::TBuildingValueConsumer(
    TTableSchemaPtr schema,
    NLogging::TLogger logger,
    bool convertNullToEntity,
    TTypeConversionConfigPtr typeConversionConfig)
    : TValueConsumerBase(std::move(schema), std::move(typeConversionConfig))
    , Logger(std::move(logger))
    , NameTable_(TNameTable::FromSchema(*Schema_))
    , ConvertNullToEntity_(convertNullToEntity)
    , WrittenFlags_(NameTable_->GetSize())
{
    InitializeIdToTypeMapping();
}

std::vector<TUnversionedRow> TBuildingValueConsumer::GetRows() const
{
    std::vector<TUnversionedRow> result;
    result.reserve(Rows_.size());
    for (const auto& row : Rows_) {
        result.push_back(row);
    }
    return result;
}

void TBuildingValueConsumer::SetAggregate(bool value)
{
    Aggregate_ = value;
}

void TBuildingValueConsumer::SetTreatMissingAsNull(bool value)
{
    TreatMissingAsNull_ = value;
}

void TBuildingValueConsumer::SetAllowMissingKeyColumns(bool value)
{
    AllowMissingKeyColumns_ = value;
}

const TNameTablePtr& TBuildingValueConsumer::GetNameTable() const
{
    return NameTable_;
}

bool TBuildingValueConsumer::GetAllowUnknownColumns() const
{
    return false;
}

void TBuildingValueConsumer::OnBeginRow()
{
    // Do nothing.
}

void TBuildingValueConsumer::OnMyValue(const TUnversionedValue& value)
{
    if (value.Id >= Schema_->GetColumnCount()) {
        return;
    }
    auto valueCopy = value;
    const auto& columnSchema = Schema_->Columns()[valueCopy.Id];
    if (columnSchema.Aggregate() && Aggregate_) {
        valueCopy.Flags |= EValueFlags::Aggregate;
    }
    if (columnSchema.IsOfV1Type(ESimpleLogicalValueType::Any) &&
        valueCopy.Type != EValueType::Any &&
        (valueCopy.Type != EValueType::Null || ConvertNullToEntity_))
    {
        if (valueCopy.Type == EValueType::Null && LogNullToEntity_) {
            YT_LOG_DEBUG("Detected conversion of null to YSON entity");
            LogNullToEntity_ = false;
        }
        Builder_.AddValue(EncodeUnversionedAnyValue(valueCopy, &MemoryPool_));
        MemoryPool_.Clear();
    } else {
        Builder_.AddValue(valueCopy);
    }
    WrittenFlags_[valueCopy.Id] = true;
}

void TBuildingValueConsumer::OnEndRow()
{
    for (int id = 0; id < std::ssize(WrittenFlags_); ++id) {
        if (WrittenFlags_[id]) {
            WrittenFlags_[id] = false;
        } else if ((TreatMissingAsNull_ || id < Schema_->GetKeyColumnCount()) &&
            !(AllowMissingKeyColumns_ && id < Schema_->GetKeyColumnCount()) &&
            !Schema_->Columns()[id].Expression())
        {
            auto flags = EValueFlags::None;
            if (Schema_->Columns()[id].Aggregate() && Aggregate_) {
                flags |= EValueFlags::Aggregate;
            }
            Builder_.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id, flags));
        }
    }
    Rows_.emplace_back(Builder_.FinishRow());
}

////////////////////////////////////////////////////////////////////////////////

struct TWritingValueConsumerBufferTag
{ };

TWritingValueConsumer::TWritingValueConsumer(
    IUnversionedWriterPtr writer,
    TTypeConversionConfigPtr typeConversionConfig,
    i64 maxRowBufferSize)
    : TValueConsumerBase(writer->GetSchema(), std::move(typeConversionConfig))
    , Writer_(std::move(writer))
    , MaxRowBufferSize_(maxRowBufferSize)
    , RowBuffer_(New<TRowBuffer>(TWritingValueConsumerBufferTag()))
{
    YT_VERIFY(Writer_);
    InitializeIdToTypeMapping();
}

TFuture<void> TWritingValueConsumer::Flush()
{
    if (RowBuffer_->GetSize() == 0) {
        return VoidFuture;
    }

    // We could have multiple value consumers writing into the same Writer.
    // It means, that there could be multiple subscribers waiting to flush data on the same ready event.
    // To make writing safe, we must double check that the writer is really ready, before flushing rows.

    return
        BIND([writer = Writer_, rowBuffer = RowBuffer_, rows = std::move(Rows_)] {
            while (!writer->GetReadyEvent().IsSet() || !writer->GetReadyEvent().Get().IsOK()) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }

            Y_UNUSED(writer->Write(rows));
            rowBuffer->Clear();
            return writer->GetReadyEvent();
        })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

const TNameTablePtr& TWritingValueConsumer::GetNameTable() const
{
    return Writer_->GetNameTable();
}

bool TWritingValueConsumer::GetAllowUnknownColumns() const
{
    return true;
}

void TWritingValueConsumer::OnBeginRow()
{
    YT_ASSERT(Values_.empty());
}

void TWritingValueConsumer::OnMyValue(const TUnversionedValue& value)
{
    Values_.push_back(RowBuffer_->CaptureValue(value));
}

void TWritingValueConsumer::OnEndRow()
{
    auto row = RowBuffer_->CaptureRow(TRange(Values_), false);
    Values_.clear();
    Rows_.push_back(row);

    if (RowBuffer_->GetSize() >= MaxRowBufferSize_) {
        auto error = WaitFor(Flush());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Table writer failed")
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
