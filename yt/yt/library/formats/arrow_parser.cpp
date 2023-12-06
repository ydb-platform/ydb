#include "arrow_parser.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/formats/parser.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

#include <util/stream/buffer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NYT::NFormats {

using namespace NTableClient;
using TUnversionedRowValues = std::vector<NTableClient::TUnversionedValue>;

namespace {

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("%Qlv", status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

class ArraySimpleVisitor
    : public arrow::TypeVisitor
{
public:
    ArraySimpleVisitor(
        int columnId,
        const std::shared_ptr<arrow::Array>& array,
        const std::shared_ptr<TChunkedOutputStream>&  bufferForStringLikeValues,
        TUnversionedRowValues& rowValues)
        : ColumnId_(columnId)
        , Array_(array)
        ,  bufferForStringLikeValues_( bufferForStringLikeValues)
        , RowValues_(rowValues)
    { };

    // Signed int types.
    arrow::Status Visit(const arrow::Int8Type&) override
    {
        return ParseInt64<arrow::Int8Array>();
    }

    arrow::Status Visit(const arrow::Int16Type&) override
    {
        return ParseInt64<arrow::Int16Array>();
    }

    arrow::Status Visit(const arrow::Int32Type&) override
    {
        return ParseInt64<arrow::Int32Array>();
    }

    arrow::Status Visit(const arrow::Int64Type&) override
    {
        return ParseInt64<arrow::Int64Array>();
    }

    arrow::Status Visit(const arrow::Date32Type&) override
    {
        return ParseInt64<arrow::Date32Array>();
    }

    arrow::Status Visit(const arrow::Time32Type&) override
    {
        return ParseInt64<arrow::Time32Array>();
    }

    arrow::Status Visit(const arrow::Date64Type&) override
    {
        return ParseInt64<arrow::Date64Array>();
    }

    arrow::Status Visit(const arrow::Time64Type&) override
    {
        return ParseInt64<arrow::Time64Array>();
    }

    arrow::Status Visit(const arrow::TimestampType&) override
    {
        return ParseInt64<arrow::TimestampArray>();
    }

    // Unsigned int types.
    arrow::Status Visit(const arrow::UInt8Type&) override
    {
        return ParseUInt64<arrow::UInt8Array>();
    }

    arrow::Status Visit(const arrow::UInt16Type&) override
    {
        return ParseUInt64<arrow::UInt16Array>();
    }

    arrow::Status Visit(const arrow::UInt32Type&) override
    {
        return ParseUInt64<arrow::UInt32Array>();
    }

    arrow::Status Visit(const arrow::UInt64Type&) override
    {
        return ParseUInt64<arrow::UInt64Array>();
    }

    // Float types.
    arrow::Status Visit(const arrow::HalfFloatType&) override
    {
        return ParseDouble<arrow::HalfFloatArray>();
    }

    arrow::Status Visit(const arrow::FloatType&) override
    {
        return ParseDouble<arrow::FloatArray>();
    }

    arrow::Status Visit(const arrow::DoubleType&) override
    {
        return ParseDouble<arrow::DoubleArray>();
    }

    // String types.
    arrow::Status Visit(const arrow::StringType&) override
    {
        return ParseString<arrow::StringArray>();
    }

    arrow::Status Visit(const arrow::BinaryType&) override
    {
        return ParseString<arrow::BinaryArray>();
    }

    // Boolean type.
    arrow::Status Visit(const arrow::BooleanType&) override
    {
        return ParseBoolean();
    }

    // Null type.
    arrow::Status Visit(const arrow::NullType&) override
    {
        return ParseNull();
    }

private:
    template <typename ArrayType>
    arrow::Status ParseInt64()
    {
        auto makeUnversionedValue = [] (int64_t value, int64_t columnId) {
            return MakeUnversionedInt64Value(value, columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseUInt64()
    {
        auto makeUnversionedValue = [] (int64_t value, int64_t columnId) {
            return MakeUnversionedUint64Value(value, columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType>
    arrow::Status ParseDouble()
    {
        auto makeUnversionedValue = [] (double value, int64_t columnId) {
            return MakeUnversionedDoubleValue(value, columnId);
        };
        ParseSimpleNumeric<ArrayType, decltype(makeUnversionedValue)>(makeUnversionedValue);
        return arrow::Status::OK();
    }

    template <typename ArrayType, typename FuncType>
    void ParseSimpleNumeric(FuncType makeUnversionedValue)
    {
        auto intArray = std::static_pointer_cast<ArrayType>(Array_);
        for (int rowIndex = 0; rowIndex < intArray->length(); rowIndex++) {
            if (intArray->IsNull(rowIndex)) {
                RowValues_[rowIndex] = MakeUnversionedNullValue(ColumnId_);
            } else {
                RowValues_[rowIndex] = makeUnversionedValue(intArray->Value(rowIndex), ColumnId_);
            }
        }
    }

    template <typename ArrayType>
    arrow::Status ParseString()
    {
        auto stringArray = std::static_pointer_cast<ArrayType>(Array_);
        for (int rowIndex = 0; rowIndex < stringArray->length(); rowIndex++) {
            if (stringArray->IsNull(rowIndex)) {
                RowValues_[rowIndex] = MakeUnversionedNullValue(ColumnId_);
            } else {
                auto stringElement = stringArray->GetView(rowIndex);
                char* buffer =  bufferForStringLikeValues_->Preallocate(stringElement.size());
                std::memcpy(
                    buffer,
                    stringElement.data(),
                    stringElement.size());
                 bufferForStringLikeValues_->Advance(stringElement.size());
                auto value = TStringBuf(buffer, stringElement.size());

                RowValues_[rowIndex] = MakeUnversionedStringValue(value, ColumnId_);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseBoolean()
    {
        auto boolArray = std::static_pointer_cast<arrow::BooleanArray>(Array_);
        for (int rowIndex = 0; rowIndex < boolArray->length(); rowIndex++) {
            if (boolArray->IsNull(rowIndex)) {
                RowValues_[rowIndex] = MakeUnversionedNullValue(ColumnId_);
            } else {
                RowValues_[rowIndex] = MakeUnversionedBooleanValue(boolArray->Value(rowIndex), ColumnId_);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseNull()
    {
        auto nullArray = std::static_pointer_cast<arrow::NullArray>(Array_);
        for (int rowIndex = 0; rowIndex < nullArray->length(); rowIndex++) {
            RowValues_[rowIndex] = MakeUnversionedNullValue(ColumnId_);
        }
        return arrow::Status::OK();
    }

private:
    const int64_t ColumnId_;
    const std::shared_ptr<arrow::Array>& Array_;
    std::shared_ptr<TChunkedOutputStream>  bufferForStringLikeValues_;
    TUnversionedRowValues& RowValues_;
};

////////////////////////////////////////////////////////////////////////////////

class ArrayCompositeVisitor
    : public arrow::TypeVisitor
{
public:
    ArrayCompositeVisitor(
        const std::shared_ptr<arrow::Array>& array,
        NYson::TCheckedInDebugYsonTokenWriter* writer,
        int rowIndex)
        : RowIndex_(rowIndex)
        , Array_(array)
        , Writer_(writer)
    { };

    // Signed integer types.
    arrow::Status Visit(const arrow::Int8Type&) override
    {
        return ParseInt64<arrow::Int8Array>();
    }

    arrow::Status Visit(const arrow::Int16Type&) override
    {
        return ParseInt64<arrow::Int16Array>();
    }

    arrow::Status Visit(const arrow::Int32Type&) override
    {
        return ParseInt64<arrow::Int32Array>();
    }

    arrow::Status Visit(const arrow::Int64Type&) override
    {
        return ParseInt64<arrow::Int64Array>();
    }

    arrow::Status Visit(const arrow::Date32Type&) override
    {
        return ParseInt64<arrow::Date32Array>();
    }

    arrow::Status Visit(const arrow::Time32Type&) override
    {
        return ParseInt64<arrow::Time32Array>();
    }

    arrow::Status Visit(const arrow::Date64Type&) override
    {
        return ParseInt64<arrow::Date64Array>();
    }

    arrow::Status Visit(const arrow::Time64Type&) override
    {
        return ParseInt64<arrow::Time64Array>();
    }

    arrow::Status Visit(const arrow::TimestampType&) override
    {
        return ParseInt64<arrow::TimestampArray>();
    }

    // Unsigned integer types.
    arrow::Status Visit(const arrow::UInt8Type&) override
    {
        return ParseUInt64<arrow::UInt8Array>();
    }

    arrow::Status Visit(const arrow::UInt16Type&) override
    {
        return ParseUInt64<arrow::UInt16Array>();
    }

    arrow::Status Visit(const arrow::UInt32Type&) override
    {
        return ParseUInt64<arrow::UInt32Array>();
    }

    arrow::Status Visit(const arrow::UInt64Type&) override
    {
        return ParseUInt64<arrow::UInt64Array>();
    }

    // Float types.
    arrow::Status Visit(const arrow::HalfFloatType&) override
    {
        return ParseDouble<arrow::HalfFloatArray>();
    }

    arrow::Status Visit(const arrow::FloatType&) override
    {
        return ParseDouble<arrow::FloatArray>();
    }

    arrow::Status Visit(const arrow::DoubleType&) override
    {
        return ParseDouble<arrow::DoubleArray>();
    }

    // Binary types.
    arrow::Status Visit(const arrow::StringType&) override
    {
        return ParseString<arrow::StringArray>();
    }
    arrow::Status Visit(const arrow::BinaryType&) override
    {
        return ParseString<arrow::BinaryArray>();
    }

    // Boolean types.
    arrow::Status Visit(const arrow::BooleanType&) override
    {
        return ParseBoolean();
    }

    // Null types.
    arrow::Status Visit(const arrow::NullType&) override
    {
        return ParseNull();
    }

    // Complex types.
    arrow::Status Visit(const arrow::ListType&) override
    {
        return ParseList();
    }

    arrow::Status Visit(const arrow::MapType&) override
    {
        return ParseMap();
    }
    arrow::Status Visit(const arrow::StructType&) override
    {
        return ParseStruct();
    }

private:
    template <typename ArrayType>
    arrow::Status ParseInt64()
    {
        auto writeNumericValue = [] (NYson::TCheckedInDebugYsonTokenWriter* writer, int64_t value) {
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
        auto intArray = std::static_pointer_cast<ArrayType>(Array_);
        if (intArray->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            writeNumericValue(Writer_, intArray->Value(RowIndex_));
        }
    }

    template <typename ArrayType>
    arrow::Status ParseString()
    {
        auto stringArray = std::static_pointer_cast<ArrayType>(Array_);
        if (stringArray->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            auto stringElement = stringArray->GetView(RowIndex_);
            auto value = TStringBuf(stringElement.data(), stringElement.size());
            Writer_->WriteBinaryString(value);
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseBoolean()
    {
        auto boolArray = std::static_pointer_cast<arrow::BooleanArray>(Array_);
        if (boolArray->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBinaryBoolean(boolArray->Value(RowIndex_));
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
        auto listArray = std::static_pointer_cast<arrow::ListArray>(Array_);
        if (listArray->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBeginList();

            auto column = listArray->value_slice(RowIndex_);

            for (int RowIndex_ = 0; RowIndex_ < column->length(); RowIndex_++) {
                ArrayCompositeVisitor visitor(column, Writer_, RowIndex_);
                ThrowOnError(column->type()->Accept(&visitor));

                Writer_->WriteItemSeparator();
            }

            Writer_->WriteEndList();
        }
        return arrow::Status::OK();
    }

    arrow::Status ParseMap()
    {
        auto mapArray = std::static_pointer_cast<arrow::MapArray>(Array_);
        if (mapArray->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            auto mapArrayElement = std::static_pointer_cast<arrow::StructArray>(mapArray->value_slice(RowIndex_));

            auto keyColumn = mapArrayElement->GetFieldByName("key");
            auto valueColumn = mapArrayElement->GetFieldByName("value");

            Writer_->WriteBeginList();
            for (int index = 0; index < keyColumn->length(); index++) {
                Writer_->WriteBeginList();

                ArrayCompositeVisitor keyVisitor(keyColumn, Writer_, index);

                ThrowOnError(keyColumn->type()->Accept(&keyVisitor));

                Writer_->WriteItemSeparator();

                ArrayCompositeVisitor valueVisitor(valueColumn, Writer_, index);
                ThrowOnError(valueColumn->type()->Accept(&valueVisitor));

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
        auto structArray = std::static_pointer_cast<arrow::StructArray>(Array_);

        if (structArray->IsNull(RowIndex_)) {
            Writer_->WriteEntity();
        } else {
            Writer_->WriteBeginList();
            for (int elementIndex = 0; elementIndex < structArray->num_fields(); elementIndex++) {
                auto elementColumn = structArray->field(RowIndex_);
                ArrayCompositeVisitor elementVisitor(elementColumn, Writer_, RowIndex_);
                ThrowOnError(elementColumn->type()->Accept(&elementVisitor));

                Writer_->WriteItemSeparator();
            }
            Writer_->WriteEndList();
        }
        return arrow::Status::OK();
    }

private:
    const int RowIndex_;
    std::shared_ptr<arrow::Array> Array_;
    NYson::TCheckedInDebugYsonTokenWriter* Writer_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

void CheckArrowType(
    const std::shared_ptr<arrow::DataType>& arrowType,
    const std::initializer_list<arrow::Type::type>& allowedTypes)
{
    if (std::find(allowedTypes.begin(), allowedTypes.end(), arrowType->id()) == allowedTypes.end()) {
        THROW_ERROR_EXCEPTION("Unexpected arrow type %Qlv",
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
            CheckArrowType(column->type(), {arrow::Type::NA, arrow::Type::DICTIONARY});
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

        ArraySimpleVisitor visitor(columnId, dictionaryValuesColumn,  bufferForStringLikeValues, dictionaryValues);
        ThrowOnError(dictionaryColumn->dictionary()->type()->Accept(&visitor));

        for (int offset = 0; offset < std::ssize(rowsValues[columnIndex]); offset++) {
            if (dictionaryColumn->IsNull(offset)) {
                rowsValues[columnIndex][offset] = MakeUnversionedNullValue(columnId);
            } else {
                rowsValues[columnIndex][offset] = dictionaryValues[dictionaryColumn->GetValueIndex(offset)];
            }
        }
    } else {
        ArraySimpleVisitor visitor(columnId, column,  bufferForStringLikeValues, rowsValues[columnIndex]);
        ThrowOnError(column->type()->Accept(&visitor));
    }
}

void PrepareArrayForComplexType(
    TLogicalTypePtr denullifiedLogicalType,
    const std::shared_ptr<TChunkedOutputStream>&  bufferForStringLikeValues,
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
            THROW_ERROR_EXCEPTION("Unexpected arrow type in complex type %Qlv", column->type()->name());
    }

    if (column->type()->id() == arrow::Type::BINARY) {
        TUnversionedRowValues stringValues(rowsValues[columnIndex].size());
        ArraySimpleVisitor visitor(columnId, column,  bufferForStringLikeValues, stringValues);
        ThrowOnError(column->type()->Accept(&visitor));
        for (int offset = 0; offset < std::ssize(rowsValues[columnIndex]); offset++) {
            if (column->IsNull(offset)) {
                rowsValues[columnIndex][offset] = MakeUnversionedNullValue(columnId);
            } else {
                rowsValues[columnIndex][offset] =  MakeUnversionedCompositeValue(stringValues[offset].AsStringBuf(), columnId);
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

                ArrayCompositeVisitor visitor(column, &writer, rowIndex);

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
    TLogicalTypePtr denullifiedLogicalType,
    const std::shared_ptr<TChunkedOutputStream>&  bufferForStringLikeValues,
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
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

enum class ListenerState {
    EOS,
    RecordBatch,
    InProgress
};

class Listener
    : public arrow::ipc::Listener
{
public:
    Listener(IValueConsumer* valueConsumer)
        : Consumer_(valueConsumer)
    { }

    arrow::Status OnEOS() override
    {
        CurrentState_ = ListenerState::EOS;
        return arrow::Status::OK();
    }

    arrow::Status OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> batch) override
    {
        CurrentState_ = ListenerState::RecordBatch;

        struct TArrowParserTag
        { };
        auto  bufferForStringLikeValues = std::make_shared<TChunkedOutputStream>(
            GetRefCountedTypeCookie<TArrowParserTag>(),
            256_KB,
            1_MB);

        std::vector<TUnversionedRowValues> rowsValues(batch->num_columns(), TUnversionedRowValues(batch->num_rows()));
        for (int columnIndex = 0; columnIndex < batch->num_columns(); columnIndex++) {
            const auto columnId = Consumer_->GetNameTable()->GetIdOrRegisterName(batch->column_name(columnIndex));
            auto columnSchema = Consumer_->GetSchema()->FindColumn(batch->column_name(columnIndex));
            const auto columnType = columnSchema ? columnSchema->LogicalType() : OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));

            const auto denullifiedLogicalType = DenullifyLogicalType(columnType);
            PrepareArray(
                denullifiedLogicalType,
                 bufferForStringLikeValues,
                batch->column(columnIndex),
                rowsValues,
                columnIndex,
                columnId);
        }

        for (int rowIndex = 0; rowIndex < batch->num_rows(); rowIndex++) {
            Consumer_->OnBeginRow();
            for (int columnIndex = 0; columnIndex < batch->num_columns(); columnIndex++) {
                Consumer_->OnValue(rowsValues[columnIndex][rowIndex]);
            }
            Consumer_->OnEndRow();
        }
        return arrow::Status::OK();
    }

    void Reset()
    {
        CurrentState_ = ListenerState::InProgress;
    }

    ListenerState GetState()
    {
        return CurrentState_;
    }

private:
    ListenerState CurrentState_ = ListenerState::InProgress;
    IValueConsumer* Consumer_;
};

std::shared_ptr<arrow::Buffer> MakeBuffer(const char* data, int64_t size)
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
    {
        Listener_ = std::make_shared<Listener>(valueConsumer);
        Decoder_ = std::make_shared<arrow::ipc::StreamDecoder>(Listener_);
    }

    void Read(TStringBuf data) override
    {
        int64_t restDataSize = data.Size();
        auto currentPtr = data.Data();
        while (restDataSize > 0) {
            auto nextRequiredSize = Decoder_->next_required_size();

            auto currentSize = std::min(reinterpret_cast<int64_t>(nextRequiredSize), restDataSize);

            ThrowOnError(Decoder_->Consume(MakeBuffer(currentPtr, currentSize)));

            LastState_ = Listener_->GetState();

            switch (LastState_) {
                case ListenerState::InProgress:
                    break;

                case ListenerState::EOS:
                    Decoder_ = std::make_shared<arrow::ipc::StreamDecoder>(Listener_);
                    Listener_->Reset();
                    break;

                case ListenerState::RecordBatch:
                    Listener_->Reset();
                    break;
            }

            currentPtr += currentSize;
            restDataSize -= currentSize;
        }
    }

    void Finish() override
    {
        if (LastState_ == ListenerState::InProgress) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream");
        }
    }


private:
    std::shared_ptr<Listener> Listener_;
    std::shared_ptr<arrow::ipc::StreamDecoder> Decoder_;
    ListenerState LastState_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForArrow(IValueConsumer* consumer)
{
    return std::make_unique<TArrowParser>(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
