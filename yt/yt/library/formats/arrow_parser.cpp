#include "arrow_parser.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/formats/parser.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

#include <util/stream/buffer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

namespace NYT::NFormats {

using namespace NTableClient;
using TUnversionedRowValues = std::vector<NTableClient::TUnversionedValue>;

namespace {

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("Arrow error occurred: %Qv", status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TArraySimpleVisitor
    : public arrow::TypeVisitor
{
public:
    TArraySimpleVisitor(
        int columnId,
        std::shared_ptr<arrow::Array> array,
        std::shared_ptr<TChunkedOutputStream> bufferForStringLikeValues,
        TUnversionedRowValues* rowValues)
        : ColumnId_(columnId)
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

    arrow::Status Visit(const arrow::Date32Type& /*type*/) override
    {
        return ParseInt64<arrow::Date32Array>();
    }

    arrow::Status Visit(const arrow::Time32Type& /*type*/) override
    {
        return ParseInt64<arrow::Time32Array>();
    }

    arrow::Status Visit(const arrow::Date64Type& /*type*/) override
    {
        return ParseInt64<arrow::Date64Array>();
    }

    arrow::Status Visit(const arrow::Time64Type& /*type*/) override
    {
        return ParseInt64<arrow::Time64Array>();
    }

    arrow::Status Visit(const arrow::TimestampType& /*type*/) override
    {
        return ParseInt64<arrow::TimestampArray>();
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

private:
    const i64 ColumnId_;

    std::shared_ptr<arrow::Array> Array_;
    std::shared_ptr<TChunkedOutputStream> BufferForStringLikeValues_;
    TUnversionedRowValues* RowValues_;

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
        auto makeUnversionedValue = [] (i64 value, i64 columnId) {
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
        for (int rowIndex = 0; rowIndex < array->length(); ++rowIndex) {
            if (array->IsNull(rowIndex)) {
                (*RowValues_)[rowIndex] = MakeUnversionedNullValue(ColumnId_);
            } else {
                (*RowValues_)[rowIndex] = makeUnversionedValueFunc(array->Value(rowIndex), ColumnId_);
            }
        }
    }

    template <typename ArrayType>
    arrow::Status ParseStringLikeArray()
    {
        auto array = std::static_pointer_cast<ArrayType>(Array_);
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

                (*RowValues_)[rowIndex] = MakeUnversionedStringValue(value, ColumnId_);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseBoolean()
    {
        auto array = std::static_pointer_cast<arrow::BooleanArray>(Array_);
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
        for (int rowIndex = 0; rowIndex < array->length(); rowIndex++) {
            (*RowValues_)[rowIndex] = MakeUnversionedNullValue(ColumnId_);
        }
        return arrow::Status::OK();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TArrayCompositeVisitor
    : public arrow::TypeVisitor
{
public:
    TArrayCompositeVisitor(
        const std::shared_ptr<arrow::Array>& array,
        NYson::TCheckedInDebugYsonTokenWriter* writer,
        int rowIndex)
        : RowIndex_(rowIndex)
        , Array_(array)
        , Writer_(writer)
    {
        YT_VERIFY(writer != nullptr);
    }

    // Signed integer types.
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

    arrow::Status Visit(const arrow::Date32Type& /*type*/) override
    {
        return ParseInt64<arrow::Date32Array>();
    }

    arrow::Status Visit(const arrow::Time32Type& /*type*/) override
    {
        return ParseInt64<arrow::Time32Array>();
    }

    arrow::Status Visit(const arrow::Date64Type& /*type*/) override
    {
        return ParseInt64<arrow::Date64Array>();
    }

    arrow::Status Visit(const arrow::Time64Type& /*type*/) override
    {
        return ParseInt64<arrow::Time64Array>();
    }

    arrow::Status Visit(const arrow::TimestampType& /*type*/) override
    {
        return ParseInt64<arrow::TimestampArray>();
    }

    // Unsigned integer types.
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

    // Binary types.
    arrow::Status Visit(const arrow::StringType& /*type*/) override
    {
        return ParseStringLikeArray<arrow::StringArray>();
    }

    arrow::Status Visit(const arrow::BinaryType& /*type*/) override
    {
        return ParseStringLikeArray<arrow::BinaryArray>();
    }

    // Boolean types.
    arrow::Status Visit(const arrow::BooleanType& /*type*/) override
    {
        return ParseBoolean();
    }

    // Null types.
    arrow::Status Visit(const arrow::NullType& /*type*/) override
    {
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

private:
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
        auto array = std::static_pointer_cast<ArrayType>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            auto element = array->GetView(RowIndex_);
            Writer_->WriteBinaryString(TStringBuf(element.data(), element.size()));
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

    arrow::Status ParseList()
    {
        auto array = std::static_pointer_cast<arrow::ListArray>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBeginList();

            auto listValue = array->value_slice(RowIndex_);
            for (int offset = 0; offset < listValue->length(); ++offset) {
                TArrayCompositeVisitor visitor(listValue, Writer_, offset);
                ThrowOnError(listValue->type()->Accept(&visitor));

                Writer_->WriteItemSeparator();
            }

            Writer_->WriteEndList();
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseMap()
    {
        auto array = std::static_pointer_cast<arrow::MapArray>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            auto element = std::static_pointer_cast<arrow::StructArray>(
                array->value_slice(RowIndex_));

            auto keyList = element->GetFieldByName("key");
            auto valueList = element->GetFieldByName("value");

            Writer_->WriteBeginList();

            for (int offset = 0; offset < keyList->length(); ++offset) {
                Writer_->WriteBeginList();

                TArrayCompositeVisitor keyVisitor(keyList, Writer_, offset);
                ThrowOnError(keyList->type()->Accept(&keyVisitor));

                Writer_->WriteItemSeparator();

                TArrayCompositeVisitor valueVisitor(valueList, Writer_, offset);
                ThrowOnError(valueList->type()->Accept(&valueVisitor));

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
        auto array = std::static_pointer_cast<arrow::StructArray>(Array_);
        if (array->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBeginList();

            for (int offset = 0; offset < array->num_fields(); ++offset) {
                auto element = array->field(offset);
                TArrayCompositeVisitor visitor(element, Writer_, RowIndex_);
                ThrowOnError(element->type()->Accept(&visitor));

                Writer_->WriteItemSeparator();
            }

            Writer_->WriteEndList();
        }
        return arrow::Status::OK();
    }
};

////////////////////////////////////////////////////////////////////////////////

void CheckArrowType(
    const std::shared_ptr<arrow::DataType>& arrowType,
    std::initializer_list<arrow::Type::type> allowedTypes)
{
    if (std::find(allowedTypes.begin(), allowedTypes.end(), arrowType->id()) == allowedTypes.end()) {
        THROW_ERROR_EXCEPTION("Unexpected arrow type %Qv",
            arrowType->name());
    }
}

void CheckMatchingArrowTypes(
    const ESimpleLogicalValueType& columnType,
    const std::shared_ptr<arrow::Array>& column)
{
    switch (columnType) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Int64:

        case ESimpleLogicalValueType::Interval:
            CheckArrowType(
                column->type(),
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
                });
            break;

        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Uint32:
        case ESimpleLogicalValueType::Uint64:

        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::UINT8,
                    arrow::Type::UINT16,
                    arrow::Type::UINT32,
                    arrow::Type::UINT64,
                    arrow::Type::DICTIONARY
                });
            break;

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Utf8:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::STRING,
                    arrow::Type::BINARY,
                    arrow::Type::LARGE_STRING,
                    arrow::Type::LARGE_BINARY,
                    arrow::Type::FIXED_SIZE_BINARY,
                    arrow::Type::DICTIONARY
                });
            break;

        case ESimpleLogicalValueType::Float:
        case ESimpleLogicalValueType::Double:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::HALF_FLOAT,
                    arrow::Type::FLOAT,
                    arrow::Type::DOUBLE,
                    arrow::Type::DICTIONARY
                });
            break;

        case ESimpleLogicalValueType::Boolean:
            CheckArrowType(
                column->type(),
                {arrow::Type::BOOL, arrow::Type::DICTIONARY});
            break;

        case ESimpleLogicalValueType::Any:
            CheckArrowType(
                column->type(),
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
                });
            break;

        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::NA,
                    arrow::Type::DICTIONARY
                });
            break;

        case ESimpleLogicalValueType::Uuid:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::STRING,
                    arrow::Type::BINARY,
                    arrow::Type::LARGE_STRING,
                    arrow::Type::LARGE_BINARY,
                    arrow::Type::FIXED_SIZE_BINARY,
                    arrow::Type::DICTIONARY
                });
            break;

        case ESimpleLogicalValueType::Date32:
        case ESimpleLogicalValueType::Datetime64:
        case ESimpleLogicalValueType::Timestamp64:
        case ESimpleLogicalValueType::Interval64:
            THROW_ERROR_EXCEPTION("Unexpected column type %Qv",
                columnType);
    }
}

////////////////////////////////////////////////////////////////////////////////

void PrepareArrayForSimpleLogicalType(
    ESimpleLogicalValueType columnType,
    const std::shared_ptr<TChunkedOutputStream>&  bufferForStringLikeValues,
    const std::shared_ptr<arrow::Array>& column,
    std::vector<TUnversionedRowValues>& rowsValues,
    int columnIndex,
    int columnId)
{
    CheckMatchingArrowTypes(columnType, column);
    if (column->type()->id() == arrow::Type::DICTIONARY) {
        auto dictionaryColumn = std::static_pointer_cast<arrow::DictionaryArray>(column);
        TUnversionedRowValues dictionaryValues(rowsValues[columnIndex].size());
        auto dictionaryValuesColumn = dictionaryColumn->dictionary();
        CheckMatchingArrowTypes(columnType, dictionaryValuesColumn);

        TArraySimpleVisitor visitor(columnId, dictionaryValuesColumn, bufferForStringLikeValues, &dictionaryValues);
        ThrowOnError(dictionaryColumn->dictionary()->type()->Accept(&visitor));

        for (int offset = 0; offset < std::ssize(rowsValues[columnIndex]); offset++) {
            if (dictionaryColumn->IsNull(offset)) {
                rowsValues[columnIndex][offset] = MakeUnversionedNullValue(columnId);
            } else {
                rowsValues[columnIndex][offset] = dictionaryValues[dictionaryColumn->GetValueIndex(offset)];
            }
        }
    } else {
        TArraySimpleVisitor visitor(columnId, column, bufferForStringLikeValues, &rowsValues[columnIndex]);
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
    switch (denullifiedLogicalType->GetMetatype()) {
        case ELogicalMetatype::List:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::LIST,
                    arrow::Type::BINARY
                });
            break;

        case ELogicalMetatype::Dict:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::MAP,
                    arrow::Type::BINARY
                });
            break;

        case ELogicalMetatype::Struct:
            CheckArrowType(
                column->type(),
                {
                    arrow::Type::STRUCT,
                    arrow::Type::BINARY
                });
            break;
        case ELogicalMetatype::Decimal:
        case ELogicalMetatype::Optional:
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantTuple:
        case ELogicalMetatype::VariantStruct:
            CheckArrowType(column->type(), {arrow::Type::BINARY});
            break;

        default:
            THROW_ERROR_EXCEPTION("Unexpected arrow type in complex type %Qv", column->type()->name());
    }

    if (column->type()->id() == arrow::Type::BINARY) {
        TUnversionedRowValues stringValues(rowsValues[columnIndex].size());
        TArraySimpleVisitor visitor(columnId, column, bufferForStringLikeValues, &stringValues);
        ThrowOnError(column->type()->Accept(&visitor));
        for (int offset = 0; offset < std::ssize(rowsValues[columnIndex]); offset++) {
            if (column->IsNull(offset)) {
                rowsValues[columnIndex][offset] = MakeUnversionedNullValue(columnId);
            } else {
                rowsValues[columnIndex][offset] = MakeUnversionedCompositeValue(stringValues[offset].AsStringBuf(), columnId);
            }
        }
    } else {
        for (int rowIndex = 0; rowIndex < std::ssize(rowsValues[columnIndex]); rowIndex++) {
            if (column->IsNull(rowIndex)) {
                rowsValues[rowIndex][columnIndex] = MakeUnversionedNullValue(columnId);
            } else {
                TBuffer valueBuffer;
                TBufferOutput out(valueBuffer);
                NYson::TCheckedInDebugYsonTokenWriter writer(&out);

                TArrayCompositeVisitor visitor(column, &writer, rowIndex);

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
            break;

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
            break;

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

            PrepareArray(
                denullifiedColumnType,
                bufferForStringLikeValues,
                batch->column(columnIndex),
                rowsValues,
                columnIndex,
                columnId);
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
        i64 restSize = data.Size();
        const char* currentPtr = data.Data();
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
