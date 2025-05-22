#include "arrow_parser.h"

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/library/numeric/util.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

#include <util/generic/buffer.h>

#include <util/stream/buffer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/decimal.h>

namespace NYT::NFormats {

using namespace NTableClient;
using TUnversionedRowValues = std::vector<NTableClient::TUnversionedValue>;
using namespace NDecimal;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 SecondsToMicroCoefficient = 1'000'000;
static constexpr i64 MilliToMicroCoefficient = 1'000;
static constexpr i64 MicroToNanoCoefficient = 1'000;
static constexpr i64 SecondsToMilliCoefficient = 1'000;

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("Arrow error [%v]: %Qv", status.CodeAsString(), status.message());
    }
}

void CheckArrowType(
    auto ytTypeOrMetatype,
    const std::initializer_list<arrow::Type::type>& allowedArrowTypes,
    const std::string& arrowTypeName,
    arrow::Type::type arrowType)
{
    if (std::find(allowedArrowTypes.begin(), allowedArrowTypes.end(), arrowType) == allowedArrowTypes.end()) {
        THROW_ERROR_EXCEPTION("Unexpected arrow type %Qv for YT metatype %Qlv",
            arrowTypeName,
            ytTypeOrMetatype);
    }
}

void CheckArrowTypeMatch(
    const ESimpleLogicalValueType& columnType,
    const std::string& arrowTypeName,
    arrow::Type::type arrowTypeId)
{
    switch (columnType) {
        case ESimpleLogicalValueType::Int8:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT8,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Int16:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT8,
                    arrow::Type::INT16,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;
        case ESimpleLogicalValueType::Int32:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT8,
                    arrow::Type::INT16,
                    arrow::Type::INT32,
                    arrow::Type::DATE32,
                    arrow::Type::TIME32,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Int64:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT8,
                    arrow::Type::INT16,
                    arrow::Type::INT32,
                    arrow::Type::INT64,
                    arrow::Type::DATE32,
                    arrow::Type::DATE64,
                    arrow::Type::TIMESTAMP,
                    arrow::Type::TIME32,
                    arrow::Type::TIME64,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Interval:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT8,
                    arrow::Type::INT16,
                    arrow::Type::INT32,
                    arrow::Type::INT64,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Uint8:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::UINT8,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Uint16:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::UINT8,
                    arrow::Type::UINT16,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Uint32:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::UINT8,
                    arrow::Type::UINT16,
                    arrow::Type::UINT32,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Uint64:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::UINT8,
                    arrow::Type::UINT16,
                    arrow::Type::UINT32,
                    arrow::Type::UINT64,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Date:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::UINT32,
                    arrow::Type::DICTIONARY,
                    arrow::Type::DATE32,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Datetime:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::UINT32,
                    arrow::Type::UINT64,
                    arrow::Type::DICTIONARY,
                    arrow::Type::DATE64,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Timestamp:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::UINT64,
                    arrow::Type::DICTIONARY,
                    arrow::Type::TIMESTAMP,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Date32:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT32,
                    arrow::Type::DICTIONARY,
                    arrow::Type::DATE32,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Datetime64:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT32,
                    arrow::Type::INT64,
                    arrow::Type::DICTIONARY,
                    arrow::Type::DATE64,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Timestamp64:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT32,
                    arrow::Type::INT64,
                    arrow::Type::DICTIONARY,
                    arrow::Type::TIMESTAMP,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::String:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::STRING,
                    arrow::Type::BINARY,
                    arrow::Type::LARGE_STRING,
                    arrow::Type::LARGE_BINARY,
                    arrow::Type::FIXED_SIZE_BINARY,
                    arrow::Type::DICTIONARY,
                    arrow::Type::DECIMAL128,
                    arrow::Type::DECIMAL256,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Utf8:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::STRING,
                    arrow::Type::LARGE_STRING,
                    arrow::Type::BINARY,
                    arrow::Type::LARGE_BINARY,
                    arrow::Type::DICTIONARY,
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Float:
        case ESimpleLogicalValueType::Double:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::HALF_FLOAT,
                    arrow::Type::FLOAT,
                    arrow::Type::DOUBLE,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Boolean:
            CheckArrowType(
                columnType,
                {arrow::Type::BOOL, arrow::Type::DICTIONARY},
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Any:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::INT8,
                    arrow::Type::INT16,
                    arrow::Type::INT32,
                    arrow::Type::INT64,
                    arrow::Type::DATE32,
                    arrow::Type::DATE64,
                    arrow::Type::TIMESTAMP,
                    arrow::Type::TIME32,
                    arrow::Type::TIME64,

                    arrow::Type::UINT8,
                    arrow::Type::UINT16,
                    arrow::Type::UINT32,
                    arrow::Type::UINT64,

                    arrow::Type::HALF_FLOAT,
                    arrow::Type::FLOAT,
                    arrow::Type::DOUBLE,

                    arrow::Type::STRING,
                    arrow::Type::BINARY,
                    arrow::Type::LARGE_STRING,
                    arrow::Type::LARGE_BINARY,
                    arrow::Type::FIXED_SIZE_BINARY,

                    arrow::Type::BOOL,

                    arrow::Type::NA,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::NA,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Uuid:
            CheckArrowType(
                columnType,
                {
                    arrow::Type::STRING,
                    arrow::Type::BINARY,
                    arrow::Type::LARGE_STRING,
                    arrow::Type::LARGE_BINARY,
                    arrow::Type::FIXED_SIZE_BINARY,
                    arrow::Type::DICTIONARY
                },
                arrowTypeName,
                arrowTypeId);
            break;

        case ESimpleLogicalValueType::Interval64:
            THROW_ERROR_EXCEPTION("Unexpected column type %Qv",
                columnType);
    }
}

void CheckArrowTypeMatch(
    const ESimpleLogicalValueType& columnType,
    const std::shared_ptr<arrow::Array>& column)
{
    CheckArrowTypeMatch(columnType, column->type()->name(), column->type_id());
}

template <class TUnderlyingValueType>
TStringBuf SerializeDecimalBinary(TStringBuf value, int precision, char* buffer, size_t bufferLength)
{
    // NB: Arrow wire representation of Decimal128 is little-endian and (obviously) 128 bit,
    // while YT in-memory representation of Decimal is big-endian, variadic-length of either 32 bit, 64 bit or 128 bit,
    // and MSB-flipped to ensure lexical sorting order.
    // Representation of Decimal256 is similar, but the upper limit for a length is 256 bit.
    TUnderlyingValueType decimalValue;
    YT_VERIFY(value.size() == sizeof(decimalValue));
    std::memcpy(&decimalValue, value.data(), value.size());

    TStringBuf decimalBinary;
    if constexpr (std::is_same_v<TUnderlyingValueType, TDecimal::TValue128>) {
        decimalBinary = TDecimal::WriteBinary128Variadic(precision, decimalValue, buffer, bufferLength);
    } else if constexpr (std::is_same_v<TUnderlyingValueType, TDecimal::TValue256>) {
        decimalBinary = TDecimal::WriteBinary256Variadic(precision, decimalValue, buffer, bufferLength);
    } else {
        static_assert(std::is_same_v<TUnderlyingValueType, TDecimal::TValue256>, "Unexpected decimal type");
    }
    return decimalBinary;
}

////////////////////////////////////////////////////////////////////////////////

i64 CheckAndTransformDate(i64 arrowValue, i64 minAllowedDate, i64 maxAllowedDate)
{
    if (arrowValue < minAllowedDate || arrowValue > maxAllowedDate) {
        THROW_ERROR_EXCEPTION(
            "Arrow date32 value %v is incompatible with the YT date type, value should be in range [%v, %v]",
            arrowValue,
            minAllowedDate,
            maxAllowedDate);
    }
    return arrowValue;
}

i64 CheckAndTransformDatetime(i64 arrowValue, i64 minAllowedDate, i64 maxAllowedDate)
{
    auto minarrowValue = SignedSaturationArithmeticMultiply(minAllowedDate, SecondsToMilliCoefficient);
    auto maxarrowValue = SignedSaturationArithmeticMultiply(maxAllowedDate, SecondsToMilliCoefficient);
    if (arrowValue < minarrowValue || arrowValue > maxarrowValue) {
        THROW_ERROR_EXCEPTION(
            "Arrow date64 value %v is incompatible with the YT datetime type, value should be in range [%v, %v]",
            arrowValue,
            minarrowValue,
            maxarrowValue);
    }
    // Сonverting from seconds to milliseconds.
    return arrowValue / SecondsToMilliCoefficient;
}

i64 CheckAndTransformTimestamp(i64 arrowValue, arrow::TimeUnit::type timeUnit, i64 minAllowedTimestamp, i64 maxAllowedTimestamp)
{
    i64 resultValue;
    i64 minArrowAllowedTimestamp;
    i64 maxArrowAllowedTimestamp;

    switch (timeUnit) {
        case arrow::TimeUnit::type::NANO:
            resultValue = arrowValue / MicroToNanoCoefficient;
            minArrowAllowedTimestamp = SignedSaturationArithmeticMultiply(minAllowedTimestamp, MicroToNanoCoefficient);
            maxArrowAllowedTimestamp = SignedSaturationArithmeticMultiply(minAllowedTimestamp, MicroToNanoCoefficient);
            break;

        case arrow::TimeUnit::type::SECOND:
            resultValue = SignedSaturationArithmeticMultiply(arrowValue, SecondsToMicroCoefficient);
            minArrowAllowedTimestamp = minAllowedTimestamp / SecondsToMicroCoefficient;
            maxArrowAllowedTimestamp = maxAllowedTimestamp /SecondsToMicroCoefficient;
            break;

        case arrow::TimeUnit::type::MILLI:
            resultValue = SignedSaturationArithmeticMultiply(arrowValue, MilliToMicroCoefficient);
            minArrowAllowedTimestamp = minAllowedTimestamp / MilliToMicroCoefficient;
            maxArrowAllowedTimestamp = maxAllowedTimestamp /MilliToMicroCoefficient;
            break;

        case arrow::TimeUnit::type::MICRO:
            resultValue = arrowValue;
            minArrowAllowedTimestamp = minAllowedTimestamp;
            maxArrowAllowedTimestamp = maxAllowedTimestamp;
            break;

        default:
            THROW_ERROR_EXCEPTION("Unexpected arrow time unit %Qv", static_cast<int>(timeUnit));
    }

    if (resultValue < minAllowedTimestamp || resultValue > maxAllowedTimestamp) {
        THROW_ERROR_EXCEPTION(
            "Arrow timestamp value %v is incompatible with the YT timestamp type, value should be in range [%v, %v]",
            arrowValue,
            minArrowAllowedTimestamp,
            maxArrowAllowedTimestamp);
    }

    return resultValue;
}

////////////////////////////////////////////////////////////////////////////////

class TArraySimpleVisitor
    : public arrow::TypeVisitor
{
public:
    TArraySimpleVisitor(
        std::optional<ESimpleLogicalValueType> columnType,
        int columnId,
        std::shared_ptr<arrow::Array> array,
        std::shared_ptr<TChunkedOutputStream> bufferForStringLikeValues,
        TUnversionedRowValues* rowValues)
        : ColumnType_(columnType)
        , ColumnId_(columnId)
        , Array_(std::move(array))
        , BufferForStringLikeValues_(std::move(bufferForStringLikeValues))
        , RowValues_(rowValues)
    { }

    // Signed int types.
    arrow::Status Visit(const arrow::Int8Type& /*type*/) override
    {
        return ParseInt64<arrow::Int8Array>();
    }

    arrow::Status Visit(const arrow::Int16Type& /*type*/) override
    {
        return ParseInt64<arrow::Int16Array>();
    }

    arrow::Status Visit(const arrow::Int32Type& /*type*/) override
    {
        return ParseInt64<arrow::Int32Array>();
    }

    arrow::Status Visit(const arrow::Int64Type& /*type*/) override
    {
        return ParseInt64<arrow::Int64Array>();
    }

    arrow::Status Visit(const arrow::Time32Type& /*type*/) override
    {
        return ParseInt64<arrow::Time32Array>();
    }

    arrow::Status Visit(const arrow::Time64Type& /*type*/) override
    {
        return ParseInt64<arrow::Time64Array>();
    }

    arrow::Status Visit(const arrow::Date32Type& /*type*/) override
    {
        if (ColumnType_ && *ColumnType_ == ESimpleLogicalValueType::Date32) {
            return ParseDate32<arrow::Date32Array>();
        } else if (ColumnType_ && *ColumnType_ == ESimpleLogicalValueType::Date) {
            return ParseDate<arrow::Date32Array>();
        } else {
            return ParseInt64<arrow::Date32Array>();
        }
    }

    arrow::Status Visit(const arrow::Date64Type& /*type*/) override
    {
        if (ColumnType_ && *ColumnType_ == ESimpleLogicalValueType::Datetime64) {
            return ParseDate64<arrow::Date64Array>();
        } else if (ColumnType_ && *ColumnType_ == ESimpleLogicalValueType::Datetime) {
            return ParseDatetime<arrow::Date64Array>();
        } else {
            return ParseInt64<arrow::Date64Array>();
        }
    }

    arrow::Status Visit(const arrow::TimestampType& type) override
    {
        if (ColumnType_ && *ColumnType_ == ESimpleLogicalValueType::Timestamp64) {
            return ParseTimestamp64<arrow::TimestampArray>(type.unit());
        } else if (ColumnType_ && *ColumnType_ == ESimpleLogicalValueType::Timestamp) {
            return ParseTimestamp<arrow::TimestampArray>(type.unit());
        } else {
            return ParseInt64<arrow::TimestampArray>();
        }
    }

    // Unsigned int types.
    arrow::Status Visit(const arrow::UInt8Type& /*type*/) override
    {
        return ParseUInt64<arrow::UInt8Array>();
    }

    arrow::Status Visit(const arrow::UInt16Type& /*type*/) override
    {
        return ParseUInt64<arrow::UInt16Array>();
    }

    arrow::Status Visit(const arrow::UInt32Type& /*type*/) override
    {
        return ParseUInt64<arrow::UInt32Array>();
    }

    arrow::Status Visit(const arrow::UInt64Type& /*type*/) override
    {
        return ParseUInt64<arrow::UInt64Array>();
    }

    // Float types.
    arrow::Status Visit(const arrow::HalfFloatType& /*type*/) override
    {
        return ParseDouble<arrow::HalfFloatArray>();
    }

    arrow::Status Visit(const arrow::FloatType& /*type*/) override
    {
        return ParseDouble<arrow::FloatArray>();
    }

    arrow::Status Visit(const arrow::DoubleType& /*type*/) override
    {
        return ParseDouble<arrow::DoubleArray>();
    }

    // String types.
    arrow::Status Visit(const arrow::StringType& /*type*/) override
    {
        return ParseStringLikeArray<arrow::StringArray>();
    }

    arrow::Status Visit(const arrow::BinaryType& /*type*/) override
    {
        return ParseStringLikeArray<arrow::BinaryArray>();
    }

    // Boolean type.
    arrow::Status Visit(const arrow::BooleanType& /*type*/) override
    {
        return ParseBoolean();
    }

    // Null type.
    arrow::Status Visit(const arrow::NullType& /*type*/) override
    {
        return ParseNull();
    }

    arrow::Status Visit(const arrow::Decimal128Type& type) override
    {
        return ParseStringLikeArray<arrow::Decimal128Array>([&] (TStringBuf value, i64 columnId) {
            return MakeDecimalBinaryValue<TDecimal::TValue128>(value, columnId, type.precision());
        });
    }

    arrow::Status Visit(const arrow::Decimal256Type& type) override
    {
        return ParseStringLikeArray<arrow::Decimal256Array>([&] (TStringBuf value, i64 columnId) {
            return MakeDecimalBinaryValue<TDecimal::TValue256>(value, columnId, type.precision());
        });
    }

private:
    const std::optional<ESimpleLogicalValueType> ColumnType_;
    const i64 ColumnId_;

    std::shared_ptr<arrow::Array> Array_;
    std::shared_ptr<TChunkedOutputStream> BufferForStringLikeValues_;
    TUnversionedRowValues* RowValues_;

    template <typename ArrayType>
    arrow::Status ParseDate()
    {
        auto makeUnversionedValue = [] (i64 value, i64 columnId) {
            return MakeUnversionedUint64Value(
                static_cast<ui64>(CheckAndTransformDate(value, /*minAllowedDate*/ 0, DateUpperBound)),
                columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDate32()
    {
        auto makeUnversionedValue = [] (i64 value, i64 columnId) {
            return MakeUnversionedInt64Value(CheckAndTransformDate(value, Date32LowerBound, Date32UpperBound), columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDatetime()
    {
        auto makeUnversionedValue = [] (i64 value, i64 columnId) {
            return MakeUnversionedUint64Value(
                static_cast<ui64>(CheckAndTransformDatetime(value, /*minAllowedDate*/ 0, DatetimeUpperBound)),
                columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDate64()
    {
        auto makeUnversionedValue = [] (i64 value, i64 columnId) {
            return MakeUnversionedInt64Value(CheckAndTransformDatetime(value, Datetime64LowerBound, DatetimeUpperBound), columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseTimestamp(arrow::TimeUnit::type timeUnit)
    {
        auto makeUnversionedValue = [timeUnit] (i64 value, i64 columnId) {
            return MakeUnversionedUint64Value(
                static_cast<ui64>(CheckAndTransformTimestamp(value, timeUnit, /*minAllowedTimestamp*/ 0, TimestampUpperBound)),
                columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseTimestamp64(arrow::TimeUnit::type timeUnit)
    {
        auto makeUnversionedValue = [timeUnit] (i64 value, i64 columnId) {
            return MakeUnversionedInt64Value(CheckAndTransformTimestamp(value, timeUnit, Timestamp64LowerBound, Timestamp64UpperBound), columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseInt64()
    {
        auto makeUnversionedValue = [] (i64 value, i64 columnId) {
            return MakeUnversionedInt64Value(value, columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseUInt64()
    {
        auto makeUnversionedValue = [] (ui64 value, i64 columnId) {
            return MakeUnversionedUint64Value(value, columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDouble()
    {
        auto makeUnversionedValue = [] (double value, i64 columnId) {
            return MakeUnversionedDoubleValue(value, columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType, typename FuncType>
    void ParseSimpleNumeric(FuncType makeUnversionedValueFunc)
    {
        auto array = std::static_pointer_cast<ArrayType>(Array_);
        YT_VERIFY(array->length() <= std::ssize(*RowValues_));
        for (int rowIndex = 0; rowIndex < array->length(); ++rowIndex) {
            if (array->IsNull(rowIndex)) {
                (*RowValues_)[rowIndex] = MakeUnversionedNullValue(ColumnId_);
            } else {
                (*RowValues_)[rowIndex] = makeUnversionedValueFunc(array->Value(rowIndex), ColumnId_);
            }
        }
    }

    template <typename ArrayType>
    arrow::Status ParseStringLikeArray(auto makeUnversionedValueFunc)
    {
        auto array = std::static_pointer_cast<ArrayType>(Array_);
        YT_VERIFY(array->length() <= std::ssize(*RowValues_));
        for (int rowIndex = 0; rowIndex < array->length(); ++rowIndex) {
            if (array->IsNull(rowIndex)) {
                (*RowValues_)[rowIndex] = MakeUnversionedNullValue(ColumnId_);
            } else {
                auto element = array->GetView(rowIndex);
                char* buffer = BufferForStringLikeValues_->Preallocate(element.size());
                std::memcpy(
                    buffer,
                    element.data(),
                    element.size());
                BufferForStringLikeValues_->Advance(element.size());
                auto value = TStringBuf(buffer, element.size());

                (*RowValues_)[rowIndex] = makeUnversionedValueFunc(value, ColumnId_);
            }
        }
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseStringLikeArray()
    {
        // Note that MakeUnversionedValue actually has third argument in its signature,
        // which leads to a "too few arguments" in the point of its invocation if we try to pass
        // it directly to ParseStringLikeArray.
        return ParseStringLikeArray<ArrayType>([this] (TStringBuf value, i64 columnId) {
            if (ColumnType_  && *ColumnType_ == ESimpleLogicalValueType::Any) {
                return MakeUnversionedAnyValue(value, columnId);
            } else {
                return MakeUnversionedStringValue(value, columnId);
            }
        });
    }

    arrow::Status ParseBoolean()
    {
        auto array = std::static_pointer_cast<arrow::BooleanArray>(Array_);
        YT_VERIFY(array->length() <= std::ssize(*RowValues_));
        for (int rowIndex = 0; rowIndex < array->length(); rowIndex++) {
            if (array->IsNull(rowIndex)) {
                (*RowValues_)[rowIndex] = MakeUnversionedNullValue(ColumnId_);
            } else {
                (*RowValues_)[rowIndex] = MakeUnversionedBooleanValue(array->Value(rowIndex), ColumnId_);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseNull()
    {
        auto array = std::static_pointer_cast<arrow::NullArray>(Array_);
        YT_VERIFY(array->length() <= std::ssize(*RowValues_));
        for (int rowIndex = 0; rowIndex < array->length(); rowIndex++) {
            (*RowValues_)[rowIndex] = MakeUnversionedNullValue(ColumnId_);
        }
        return arrow::Status::OK();
    }

    template <class TUnderlyingValueType>
    TUnversionedValue MakeDecimalBinaryValue(TStringBuf arrowValue, i64 columnId, int precision)
    {
        const auto maxByteCount = sizeof(TUnderlyingValueType);
        char* buffer = BufferForStringLikeValues_->Preallocate(maxByteCount);
        auto decimalBinary = SerializeDecimalBinary<TUnderlyingValueType>(arrowValue, precision, buffer, maxByteCount);
        BufferForStringLikeValues_->Advance(decimalBinary.size());
        return MakeUnversionedStringValue(decimalBinary, columnId);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TArrayCompositeVisitor
    : public arrow::TypeVisitor
{
public:
    TArrayCompositeVisitor(
        TLogicalTypePtr ytType,
        const std::shared_ptr<arrow::Array>& array,
        NYson::TCheckedInDebugYsonTokenWriter* writer,
        int rowIndex)
        : YTType_(DenullifyLogicalType(ytType))
        , RowIndex_(rowIndex)
        , Array_(array)
        , Writer_(writer)
    {
        YT_VERIFY(writer != nullptr);
    }

    // Signed integer types.
    arrow::Status Visit(const arrow::Int8Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseInt64<arrow::Int8Array>();
    }

    arrow::Status Visit(const arrow::Int16Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseInt64<arrow::Int16Array>();
    }

    arrow::Status Visit(const arrow::Int32Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseInt64<arrow::Int32Array>();
    }

    arrow::Status Visit(const arrow::Int64Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseInt64<arrow::Int64Array>();
    }

    // Date types.
    arrow::Status Visit(const arrow::Time32Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseInt64<arrow::Time32Array>();
    }

    arrow::Status Visit(const arrow::Time64Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseInt64<arrow::Time64Array>();
    }

    arrow::Status Visit(const arrow::Date32Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        if (YTType_->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Date32) {
            return ParseDate32<arrow::Date32Array>();
        } else if (YTType_->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Date) {
            return ParseDate<arrow::Date32Array>();
        } else {
            return ParseInt64<arrow::Date32Array>();
        }
    }

    arrow::Status Visit(const arrow::Date64Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        if (YTType_->AsSimpleTypeRef().GetElement()== ESimpleLogicalValueType::Datetime64) {
            return ParseDate64<arrow::Date64Array>();
        } else if (YTType_->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Datetime) {
            return ParseDatetime<arrow::Date64Array>();
        } else {
            return ParseInt64<arrow::Date64Array>();
        }
    }

    arrow::Status Visit(const arrow::TimestampType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        if (YTType_->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Timestamp64) {
            return ParseTimestamp64<arrow::TimestampArray>(type.unit());
        } else if (YTType_->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Timestamp) {
            return ParseTimestamp<arrow::TimestampArray>(type.unit());
        } else {
            return ParseInt64<arrow::TimestampArray>();
        }
    }

    // Unsigned integer types.
    arrow::Status Visit(const arrow::UInt8Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseUInt64<arrow::UInt8Array>();
    }

    arrow::Status Visit(const arrow::UInt16Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseUInt64<arrow::UInt16Array>();
    }

    arrow::Status Visit(const arrow::UInt32Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseUInt64<arrow::UInt32Array>();
    }

    arrow::Status Visit(const arrow::UInt64Type& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseUInt64<arrow::UInt64Array>();
    }

    // Float types.
    arrow::Status Visit(const arrow::HalfFloatType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseDouble<arrow::HalfFloatArray>();
    }

    arrow::Status Visit(const arrow::FloatType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseDouble<arrow::FloatArray>();
    }

    arrow::Status Visit(const arrow::DoubleType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseDouble<arrow::DoubleArray>();
    }

    // Binary types.
    arrow::Status Visit(const arrow::StringType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseStringLikeArray<arrow::StringArray>();
    }

    arrow::Status Visit(const arrow::BinaryType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseStringLikeArray<arrow::BinaryArray>();
    }

    // Boolean types.
    arrow::Status Visit(const arrow::BooleanType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseBoolean();
    }

    // Null types.
    arrow::Status Visit(const arrow::NullType& type) override
    {
        CheckArrowTypeMatch(YTType_->AsSimpleTypeRef().GetElement(), type.type_name(), type.id());
        return ParseNull();
    }

    // Complex types.
    arrow::Status Visit(const arrow::ListType& /*type*/) override
    {
        return ParseList();
    }

    arrow::Status Visit(const arrow::MapType& /*type*/) override
    {
        return ParseMap();
    }

    arrow::Status Visit(const arrow::StructType& /*type*/) override
    {
        return ParseStruct();
    }

    arrow::Status Visit(const arrow::Decimal128Type& type) override
    {
        return ParseStringLikeArray<arrow::Decimal128Array>([&] (TStringBuf value) {
            WriteDecimalBinary<TDecimal::TValue128>(value, type.precision());
        });
    }

    arrow::Status Visit(const arrow::Decimal256Type& type) override
    {
        return ParseStringLikeArray<arrow::Decimal256Array>([&] (TStringBuf value) {
            WriteDecimalBinary<TDecimal::TValue256>(value, type.precision());
        });
    }

private:
    const TLogicalTypePtr YTType_;
    const int RowIndex_;

    std::shared_ptr<arrow::Array> Array_;
    NYson::TCheckedInDebugYsonTokenWriter* Writer_ = nullptr;

    template <typename ArrayType>
    arrow::Status ParseInt64()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, i64 value) {
            writer->WriteBinaryInt64(value);
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseUInt64()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, ui64 value) {
            writer->WriteBinaryUint64(value);
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDouble()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, double value) {
            writer->WriteBinaryDouble(value);
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType, typename FuncType>
    void ParseComplexNumeric(FuncType writeNumericValue)
    {
        auto array = std::static_pointer_cast<ArrayType>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            writeNumericValue(Writer_, array->Value(RowIndex_));
        }
    }

    template <typename ArrayType>
    arrow::Status ParseStringLikeArray()
    {
        return ParseStringLikeArray<ArrayType>([&] (TStringBuf value) {
            Writer_->WriteBinaryString(value);
        });
    }

    template <typename ArrayType>
    arrow::Status ParseStringLikeArray(auto writeStringValue)
    {
        auto array = std::static_pointer_cast<ArrayType>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            auto element = array->GetView(RowIndex_);
            writeStringValue(TStringBuf(element.data(), element.size()));
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseBoolean()
    {
        auto array = std::static_pointer_cast<arrow::BooleanArray>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBinaryBoolean(array->Value(RowIndex_));
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseNull()
    {
        Writer_->WriteEntity();
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDate()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, i64 value) {
            writer->WriteBinaryUint64(CheckAndTransformDate(value, /*minAllowedDate*/ 0, DateUpperBound));
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDate32()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, i64 value) {
            writer->WriteBinaryInt64(CheckAndTransformDate(value, Date32LowerBound, Date32UpperBound));
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDatetime()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, i64 value) {
            writer->WriteBinaryUint64(CheckAndTransformDatetime(value, /*minAllowedDate*/ 0, DatetimeUpperBound));
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDate64()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, i64 value) {
            writer->WriteBinaryInt64(CheckAndTransformDatetime(value, Datetime64LowerBound, Datetime64UpperBound));
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseTimestamp(arrow::TimeUnit::type timeUnit)
    {
        auto writeNumericValue = [timeUnit] (NYson::TCheckedInDebugYsonTokenWriter* writer, i64 value) {
            writer->WriteBinaryUint64(CheckAndTransformTimestamp(value, timeUnit, /*minAllowedTimestamp*/ 0, TimestampUpperBound));
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseTimestamp64(arrow::TimeUnit::type timeUnit)
    {
        auto writeNumericValue = [timeUnit] (NYson::TCheckedInDebugYsonTokenWriter* writer, i64 value) {
            writer->WriteBinaryInt64(CheckAndTransformTimestamp(value, timeUnit, Timestamp64LowerBound, Timestamp64UpperBound));
        };
        ParseComplexNumeric<ArrayType, decltype(writeNumericValue)>(writeNumericValue);
        return arrow::Status::OK();
    }

    arrow::Status ParseList()
    {
        if (YTType_->GetMetatype() != ELogicalMetatype::List) {
            THROW_ERROR_EXCEPTION("Unexpected arrow type \"list\" for YT metatype %Qlv",
                YTType_->GetMetatype());
        }
        auto array = std::static_pointer_cast<arrow::ListArray>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBeginList();

            auto listValue = array->value_slice(RowIndex_);
            for (int offset = 0; offset < listValue->length(); ++offset) {
                TArrayCompositeVisitor visitor(YTType_->AsListTypeRef().GetElement(), listValue, Writer_, offset);
                try {
                    ThrowOnError(listValue->type()->Accept(&visitor));
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to parse arrow type \"list\"")
                        << TErrorAttribute("offset", offset)
                        << ex;
                }
                Writer_->WriteItemSeparator();
            }

            Writer_->WriteEndList();
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseMap()
    {
        if (YTType_->GetMetatype() != ELogicalMetatype::Dict) {
            THROW_ERROR_EXCEPTION("Unexpected arrow type \"map\" for YT metatype %Qlv",
                YTType_->GetMetatype());
        }
        auto array = std::static_pointer_cast<arrow::MapArray>(Array_);
        auto allKeys = array->keys();
        auto allValues = array->items();

        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            auto offset = array->value_offset(RowIndex_);
            auto length = array->value_length(RowIndex_);

            auto keyList = allKeys->Slice(offset, length);
            auto valueList = allValues->Slice(offset, length);

            Writer_->WriteBeginList();

            for (int offset = 0; offset < keyList->length(); ++offset) {
                Writer_->WriteBeginList();

                TArrayCompositeVisitor keyVisitor(YTType_->AsDictTypeRef().GetKey(), keyList, Writer_, offset);
                try {
                    ThrowOnError(keyList->type()->Accept(&keyVisitor));
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to parse arrow key field of type \"map\"")
                        << TErrorAttribute("offset", offset)
                        << ex;
                }

                Writer_->WriteItemSeparator();

                TArrayCompositeVisitor valueVisitor(YTType_->AsDictTypeRef().GetValue(), valueList, Writer_, offset);
                try {
                    ThrowOnError(valueList->type()->Accept(&valueVisitor));
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to parse arrow value field type \"map\"")
                        << TErrorAttribute("offset", offset)
                        << ex;
                }

                Writer_->WriteItemSeparator();

                Writer_->WriteEndList();
                Writer_->WriteItemSeparator();
            }

            Writer_->WriteEndList();
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseStruct()
    {
        if (YTType_->GetMetatype() != ELogicalMetatype::Struct) {
            THROW_ERROR_EXCEPTION("Unexpected arrow type \"struct\" for YT metatype %Qlv",
                YTType_->GetMetatype());
        }
        auto array = std::static_pointer_cast<arrow::StructArray>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBeginList();
            auto structFields = YTType_->AsStructTypeRef().GetFields();
            if (std::ssize(structFields) != array->num_fields()) {
                THROW_ERROR_EXCEPTION("The number of fields in the Arrow \"struct\" type does not match the number of fields in the YT \"struct\" type")
                    << TErrorAttribute("arrow_field_count", array->num_fields())
                    << TErrorAttribute("yt_field_count", std::ssize(structFields));
            }
            for (const auto& field : structFields) {
                auto arrowField = array->GetFieldByName(field.Name);
                if (!arrowField) {
                    THROW_ERROR_EXCEPTION("Field %Qv is not found in arrow type \"struct\"", field.Name);
                }
                TArrayCompositeVisitor visitor(field.Type, arrowField, Writer_, RowIndex_);
                try {
                    ThrowOnError(arrowField->type()->Accept(&visitor));
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to parse arrow struct field %Qv", field.Name)
                        << ex;
                }

                Writer_->WriteItemSeparator();
            }

            Writer_->WriteEndList();
        }
        return arrow::Status::OK();
    }

    template <class TUnderlyingType>
    void WriteDecimalBinary(TStringBuf arrowValue, int precision)
    {
        const auto maxByteCount = sizeof(TUnderlyingType);
        char buffer[maxByteCount];
        auto decimalBinary = SerializeDecimalBinary<TUnderlyingType>(arrowValue, precision, buffer, maxByteCount);
        Writer_->WriteBinaryString(decimalBinary);
    }
};

////////////////////////////////////////////////////////////////////////////////

void PrepareArrayForSimpleLogicalType(
    ESimpleLogicalValueType columnType,
    const std::shared_ptr<TChunkedOutputStream>&  bufferForStringLikeValues,
    const std::shared_ptr<arrow::Array>& column,
    std::vector<TUnversionedRowValues>& rowsValues,
    int columnIndex,
    int columnId)
{
    CheckArrowTypeMatch(columnType, column);
    if (column->type()->id() == arrow::Type::DICTIONARY) {
        auto dictionaryArrayColumn = std::static_pointer_cast<arrow::DictionaryArray>(column);
        auto dictionary = dictionaryArrayColumn->dictionary();
        TUnversionedRowValues dictionaryValues(dictionary->length());
        CheckArrowTypeMatch(columnType, dictionary);

        TArraySimpleVisitor visitor(columnType, columnId, dictionary, bufferForStringLikeValues, &dictionaryValues);
        ThrowOnError(dictionaryArrayColumn->dictionary()->type()->Accept(&visitor));

        for (int offset = 0; offset < std::ssize(rowsValues[columnIndex]); offset++) {
            if (dictionaryArrayColumn->IsNull(offset)) {
                rowsValues[columnIndex][offset] = MakeUnversionedNullValue(columnId);
            } else {
                auto dictionaryValueIndex = dictionaryArrayColumn->GetValueIndex(offset);
                YT_VERIFY(dictionaryValueIndex < std::ssize(dictionaryValues));
                rowsValues[columnIndex][offset] = dictionaryValues[dictionaryValueIndex];
            }
        }
    } else {
        TArraySimpleVisitor visitor(columnType, columnId, column, bufferForStringLikeValues, &rowsValues[columnIndex]);
        ThrowOnError(column->type()->Accept(&visitor));
    }
}

void PrepareArrayForComplexType(
    const TLogicalTypePtr& denullifiedLogicalType,
    const std::shared_ptr<TChunkedOutputStream>& bufferForStringLikeValues,
    const std::shared_ptr<arrow::Array>& column,
    std::vector<TUnversionedRowValues>& rowsValues,
    int columnIndex,
    int columnId)
{
    switch (auto metatype = denullifiedLogicalType->GetMetatype()) {
        case ELogicalMetatype::List:
            CheckArrowType(
                metatype,
                {
                    arrow::Type::LIST,
                    arrow::Type::BINARY
                },
                column->type()->name(),
                column->type_id());
            break;

        case ELogicalMetatype::Dict:
            CheckArrowType(
                metatype,
                {
                    arrow::Type::MAP,
                    arrow::Type::BINARY
                },
                column->type()->name(),
                column->type_id());
            break;

        case ELogicalMetatype::Struct:
            CheckArrowType(
                metatype,
                {
                    arrow::Type::STRUCT,
                    arrow::Type::BINARY
                },
                column->type()->name(),
                column->type_id());
            break;

        case ELogicalMetatype::Decimal:
            CheckArrowType(
                metatype,
                {
                    arrow::Type::DECIMAL128,
                    arrow::Type::DECIMAL256
                },
                column->type()->name(),
                column->type_id());
            break;

        case ELogicalMetatype::Optional:
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantTuple:
        case ELogicalMetatype::VariantStruct:
            CheckArrowType(metatype, {arrow::Type::BINARY}, column->type()->name(), column->type_id());
            break;

        default:
            THROW_ERROR_EXCEPTION("Unexpected arrow type in complex type %Qv", column->type()->name());
    }

    if (column->type()->id() == arrow::Type::BINARY ||
        column->type()->id() == arrow::Type::DECIMAL128 ||
        column->type()->id() == arrow::Type::DECIMAL256)
    {
        TUnversionedRowValues stringValues(rowsValues[columnIndex].size());
        TArraySimpleVisitor visitor(/*columnType*/ std::nullopt, columnId, column, bufferForStringLikeValues, &stringValues);
        ThrowOnError(column->type()->Accept(&visitor));
        for (int offset = 0; offset < std::ssize(rowsValues[columnIndex]); offset++) {
            if (column->IsNull(offset)) {
                rowsValues[columnIndex][offset] = MakeUnversionedNullValue(columnId);
            } else if (column->type()->id() == arrow::Type::DECIMAL128 || column->type()->id() == arrow::Type::DECIMAL256) {
                rowsValues[columnIndex][offset] = MakeUnversionedStringValue(stringValues[offset].AsStringBuf(), columnId);
            } else {
                // TODO(max): is it even correct? Binary is not necessarily a correct YSON...
                rowsValues[columnIndex][offset] = MakeUnversionedCompositeValue(stringValues[offset].AsStringBuf(), columnId);
            }
        }
    } else {
        for (int rowIndex = 0; rowIndex < std::ssize(rowsValues[columnIndex]); rowIndex++) {
            if (column->IsNull(rowIndex)) {
                rowsValues[columnIndex][rowIndex] = MakeUnversionedNullValue(columnId);
            } else {
                TBuffer valueBuffer;
                TBufferOutput out(valueBuffer);
                NYson::TCheckedInDebugYsonTokenWriter writer(&out);

                TArrayCompositeVisitor visitor(denullifiedLogicalType, column, &writer, rowIndex);

                ThrowOnError(column->type()->Accept(&visitor));

                writer.Finish();

                char* buffer =  bufferForStringLikeValues->Preallocate(valueBuffer.Size());
                std::memcpy(
                    buffer,
                    valueBuffer.Data(),
                    valueBuffer.Size());
                bufferForStringLikeValues->Advance(valueBuffer.Size());

                auto value = TStringBuf(buffer, valueBuffer.Size());

                rowsValues[columnIndex][rowIndex] = MakeUnversionedCompositeValue(value, columnId);
            }
        }
    }
}

void PrepareArray(
    const TLogicalTypePtr& denullifiedLogicalType,
    const std::shared_ptr<TChunkedOutputStream>& bufferForStringLikeValues,
    const std::shared_ptr<arrow::Array>& column,
    std::vector<TUnversionedRowValues>& rowsValues,
    int columnIndex,
    int columnId)
{
    switch (denullifiedLogicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return PrepareArrayForSimpleLogicalType(
                denullifiedLogicalType->AsSimpleTypeRef().GetElement(),
                bufferForStringLikeValues,
                column,
                rowsValues,
                columnIndex,
                columnId);

        case ELogicalMetatype::List:
        case ELogicalMetatype::Dict:
        case ELogicalMetatype::Struct:

        case ELogicalMetatype::Decimal:

        case ELogicalMetatype::Optional:
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantTuple:
        case ELogicalMetatype::VariantStruct:
            return PrepareArrayForComplexType(
                denullifiedLogicalType,
                bufferForStringLikeValues,
                column,
                rowsValues,
                columnIndex,
                columnId);

        case ELogicalMetatype::Tagged:
            // Denullified type should not contain tagged type.
            YT_ABORT();
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

enum class EListenerState
{
    EOS,
    RecordBatch,
    InProgress,
    Empty,
};

class TListener
    : public arrow::ipc::Listener
{
public:
    explicit TListener(IValueConsumer* valueConsumer)
        : Consumer_(valueConsumer)
    { }

    arrow::Status OnEOS() override
    {
        CurrentState_ = EListenerState::EOS;
        return arrow::Status::OK();
    }

    arrow::Status OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> batch) override
    {
        CurrentState_ = EListenerState::RecordBatch;

        struct TArrowParserTag
        { };

        auto bufferForStringLikeValues = std::make_shared<TChunkedOutputStream>(
            GetRefCountedTypeCookie<TArrowParserTag>(),
            GetNullMemoryUsageTracker(),
            256_KB,
            1_MB);

        auto numColumns = batch->num_columns();
        auto numRows = batch->num_rows();
        std::vector<TUnversionedRowValues> rowsValues(numColumns, TUnversionedRowValues(numRows));

        for (int columnIndex = 0; columnIndex < numColumns; ++columnIndex) {
            auto columnName = batch->column_name(columnIndex);

            auto columnId = Consumer_->GetNameTable()->GetIdOrRegisterName(columnName);
            auto columnSchema = Consumer_->GetSchema()->FindColumn(columnName);

            auto columnType = columnSchema
                ? columnSchema->LogicalType()
                : OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
            auto denullifiedColumnType = DenullifyLogicalType(columnType);
            try {
                PrepareArray(
                    denullifiedColumnType,
                    bufferForStringLikeValues,
                    batch->column(columnIndex),
                    rowsValues,
                    columnIndex,
                    columnId);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to parse column %Qv", columnName)
                    << ex;
            }
        }

        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            Consumer_->OnBeginRow();
            for (int columnIndex = 0; columnIndex < numColumns; ++columnIndex) {
                Consumer_->OnValue(rowsValues[columnIndex][rowIndex]);
            }
            Consumer_->OnEndRow();
        }
        return arrow::Status::OK();
    }

    void Reset()
    {
        CurrentState_ = EListenerState::InProgress;
    }

    EListenerState GetState()
    {
        return CurrentState_;
    }

private:
    IValueConsumer* const Consumer_;

    EListenerState CurrentState_ = EListenerState::InProgress;
};

std::shared_ptr<arrow::Buffer> MakeBuffer(const char* data, i64 size)
{
    arrow::BufferBuilder bufferBuilder;
    ThrowOnError(bufferBuilder.Reserve(size));
    ThrowOnError(bufferBuilder.Append(reinterpret_cast<const uint8_t*>(data), size));
    auto bufferResult = bufferBuilder.Finish();
    ThrowOnError(bufferResult.status());
    return *bufferResult;
}

////////////////////////////////////////////////////////////////////////////////

class TArrowParser
    : public IParser
{
public:
    TArrowParser(IValueConsumer* valueConsumer)
        : Listener_(std::make_shared<TListener>(valueConsumer))
        , Decoder_(std::make_shared<arrow::ipc::StreamDecoder>(Listener_))
    { }

    void Read(TStringBuf data) override
    {
        i64 restSize = data.size();
        const char* currentPtr = data.data();
        while (restSize > 0) {
            i64 nextRequiredSize = Decoder_->next_required_size();
            auto currentSize = std::min(nextRequiredSize, restSize);

            ThrowOnError(Decoder_->Consume(MakeBuffer(currentPtr, currentSize)));

            LastState_ = Listener_->GetState();

            switch (LastState_) {
                case EListenerState::InProgress:
                    break;

                case EListenerState::EOS:
                    Decoder_ = std::make_shared<arrow::ipc::StreamDecoder>(Listener_);
                    Listener_->Reset();
                    break;

                case EListenerState::RecordBatch:
                    Listener_->Reset();
                    break;

                case EListenerState::Empty:
                    YT_ABORT();
            }

            currentPtr += currentSize;
            restSize -= currentSize;
        }
    }

    void Finish() override
    {
        if (LastState_ == EListenerState::InProgress) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream");
        }
    }

private:
    const std::shared_ptr<TListener> Listener_;

    std::shared_ptr<arrow::ipc::StreamDecoder> Decoder_;
    EListenerState LastState_ = EListenerState::Empty;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForArrow(IValueConsumer* consumer)
{
    return std::make_unique<TArrowParser>(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
