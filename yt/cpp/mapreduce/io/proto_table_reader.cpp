#include "proto_table_reader.h"

#include "node_table_reader.h"

#include "proto_helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <util/string/escape.h>
#include <util/string/printf.h>

namespace NYT {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::EnumValueDescriptor;

using NYT::FromProto;

TString GetFieldColumnName(const FieldDescriptor* fieldDesc)
{
    auto columnName = FromProto<TString>(fieldDesc->options().GetExtension(column_name));
    if (!columnName.empty()) {
        return columnName;
    }
    auto keyColumnName = FromProto<TString>(fieldDesc->options().GetExtension(key_column_name));
    if (!keyColumnName.empty()) {
        return keyColumnName;
    }
    return FromProto<TString>(fieldDesc->name());
}

void ReadMessageFromNode(const TNode& node, Message* row)
{
    auto* descriptor = row->GetDescriptor();
    auto* reflection = row->GetReflection();

    int count = descriptor->field_count();
    for (int i = 0; i < count; ++i) {
        auto* fieldDesc = descriptor->field(i);

        const auto& columnName = GetFieldColumnName(fieldDesc);

        const auto& nodeMap = node.AsMap();
        auto it = nodeMap.find(columnName);
        if (it == nodeMap.end()) {
            continue; // no such column
        }
        auto actualType = it->second.GetType();
        if (actualType == TNode::Null) {
            continue; // null field
        }

        auto checkType = [fieldDesc] (TNode::EType expected, TNode::EType actual) {
            if (expected != actual) {
                ythrow TNode::TTypeError() << "expected node type " << expected
                    << ", actual " << actual << " for node " << GetFieldColumnName(fieldDesc);
            }
        };

        switch (fieldDesc->type()) {
            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                checkType(TNode::String, actualType);
                reflection->SetString(row, fieldDesc, it->second.AsString());
                break;
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                checkType(TNode::Int64, actualType);
                reflection->SetInt64(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                checkType(TNode::Int64, actualType);
                reflection->SetInt32(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                checkType(TNode::Uint64, actualType);
                reflection->SetUInt64(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                checkType(TNode::Uint64, actualType);
                reflection->SetUInt32(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                checkType(TNode::Double, actualType);
                reflection->SetDouble(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_FLOAT:
                checkType(TNode::Double, actualType);
                reflection->SetFloat(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_BOOL:
                checkType(TNode::Bool, actualType);
                reflection->SetBool(row, fieldDesc, it->second.AsBool());
                break;
            case FieldDescriptor::TYPE_ENUM: {
                TNode::EType columnType = TNode::String;
                for (const auto& flag : fieldDesc->options().GetRepeatedExtension(flags)) {
                    if (flag == EWrapperFieldFlag::ENUM_INT) {
                        columnType = TNode::Int64;
                        break;
                    }
                }
                checkType(columnType, actualType);

                const EnumValueDescriptor* valueDesc = nullptr;
                TString stringValue;
                if (columnType == TNode::String) {
                    const auto& value = it->second.AsString();
                    valueDesc = fieldDesc->enum_type()->FindValueByName(value);
                    stringValue = value;
                } else if (columnType == TNode::Int64) {
                    const auto& value = it->second.AsInt64();
                    valueDesc = fieldDesc->enum_type()->FindValueByNumber(value);
                    stringValue = ToString(value);
                } else {
                    Y_ABORT();
                }

                if (valueDesc == nullptr) {
                    ythrow yexception() << "Failed to parse value '" << EscapeC(stringValue) << "' as " << fieldDesc->enum_type()->full_name();
                }

                reflection->SetEnum(row, fieldDesc, valueDesc);

                break;
            }
            case FieldDescriptor::TYPE_MESSAGE: {
                checkType(TNode::String, actualType);
                Message* message = reflection->MutableMessage(row, fieldDesc);
                if (!message->ParseFromArray(it->second.AsString().data(), it->second.AsString().size())) {
                    ythrow yexception() << "Failed to parse protobuf message";
                }
                break;
            }
            default:
                ythrow yexception() << "Incorrect protobuf type";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TProtoTableReader::TProtoTableReader(
    ::TIntrusivePtr<TRawTableReader> input,
    TVector<const Descriptor*>&& descriptors)
    : NodeReader_(new TNodeTableReader(std::move(input)))
    , Descriptors_(std::move(descriptors))
{ }

void TProtoTableReader::ReadRow(Message* row)
{
    const auto& node = NodeReader_->GetRow();
    ReadMessageFromNode(node, row);
}

bool TProtoTableReader::IsValid() const
{
    return NodeReader_->IsValid();
}

void TProtoTableReader::Next()
{
    NodeReader_->Next();
}

ui32 TProtoTableReader::GetTableIndex() const
{
    return NodeReader_->GetTableIndex();
}

ui32 TProtoTableReader::GetRangeIndex() const
{
    return NodeReader_->GetRangeIndex();
}

ui64 TProtoTableReader::GetRowIndex() const
{
    return NodeReader_->GetRowIndex();
}

void TProtoTableReader::NextKey()
{
    NodeReader_->NextKey();
}

TMaybe<size_t> TProtoTableReader::GetReadByteCount() const
{
    return NodeReader_->GetReadByteCount();
}

bool TProtoTableReader::IsEndOfStream() const
{
    return NodeReader_->IsEndOfStream();
}

bool TProtoTableReader::IsRawReaderExhausted() const
{
    return NodeReader_->IsRawReaderExhausted();
}

////////////////////////////////////////////////////////////////////////////////

TLenvalProtoTableReader::TLenvalProtoTableReader(
    ::TIntrusivePtr<TRawTableReader> input,
    TVector<const Descriptor*>&& descriptors)
    : TLenvalTableReader(std::move(input))
    , ValidateProtoDescriptor_(true)
    , Descriptors_(std::move(descriptors))
{ }

TLenvalProtoTableReader::TLenvalProtoTableReader(
    ::TIntrusivePtr<TRawTableReader> input)
    : TLenvalTableReader(std::move(input))
    , ValidateProtoDescriptor_(false)
{ }

void TLenvalProtoTableReader::ReadRow(Message* row)
{
    if (ValidateProtoDescriptor_) {
        ValidateProtoDescriptor(*row, GetTableIndex(), Descriptors_, true);
    }

    while (true) {
        try {
            ParseFromArcadiaStream(&Input_, *row, Length_);
            RowTaken_ = true;

            // We successfully parsed one more row from the stream,
            // so reset retry count to their initial value.
            Input_.ResetRetries();

            break;
        } catch (const std::exception& ex) {
            if (!TLenvalTableReader::Retry(std::make_exception_ptr(ex))) {
                throw;
            }
        }
    }
}

bool TLenvalProtoTableReader::IsValid() const
{
    return TLenvalTableReader::IsValid();
}

void TLenvalProtoTableReader::Next()
{
    TLenvalTableReader::Next();
}

ui32 TLenvalProtoTableReader::GetTableIndex() const
{
    return TLenvalTableReader::GetTableIndex();
}

ui32 TLenvalProtoTableReader::GetRangeIndex() const
{
    return TLenvalTableReader::GetRangeIndex();
}

ui64 TLenvalProtoTableReader::GetRowIndex() const
{
    return TLenvalTableReader::GetRowIndex();
}

void TLenvalProtoTableReader::NextKey()
{
    TLenvalTableReader::NextKey();
}

TMaybe<size_t> TLenvalProtoTableReader::GetReadByteCount() const
{
    return TLenvalTableReader::GetReadByteCount();
}

bool TLenvalProtoTableReader::IsEndOfStream() const
{
    return TLenvalTableReader::IsEndOfStream();
}

bool TLenvalProtoTableReader::IsRawReaderExhausted() const
{
    return TLenvalTableReader::IsRawReaderExhausted();
}

void TLenvalProtoTableReader::SkipRow()
{
    while (true) {
        try {
            size_t skipped = Input_.Skip(Length_);
            if (skipped != Length_) {
                ythrow yexception() << "Premature end of stream";
            }
            break;
        } catch (const std::exception& ex) {
            if (!TLenvalTableReader::Retry(std::make_exception_ptr(ex))) {
                throw;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
