#include "proto_table_writer.h"

#include "node_table_writer.h"
#include "proto_helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/cpp/mapreduce/common/node_builder.h>

#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/cpp/mapreduce/io/job_writer.h>

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <google/protobuf/unknown_field_set.h>

namespace NYT {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;

////////////////////////////////////////////////////////////////////////////////

TNode MakeNodeFromMessage(const Message& row)
{
    TNode node;
    TNodeBuilder builder(&node);
    builder.OnBeginMap();

    auto* descriptor = row.GetDescriptor();
    auto* reflection = row.GetReflection();

    int count = descriptor->field_count();
    for (int i = 0; i < count; ++i) {
        auto* fieldDesc = descriptor->field(i);
        if (fieldDesc->is_repeated()) {
            Y_ENSURE(reflection->FieldSize(row, fieldDesc) == 0, "Storing repeated protobuf fields is not supported yet");
            continue;
        } else if (!reflection->HasField(row, fieldDesc)) {
            continue;
        }

        auto columnName = fieldDesc->options().GetExtension(column_name);
        if (columnName.empty()) {
            const auto& keyColumnName = fieldDesc->options().GetExtension(key_column_name);
            columnName = keyColumnName.empty() ? fieldDesc->name() : keyColumnName;
        }

        builder.OnKeyedItem(columnName);

        switch (fieldDesc->type()) {
            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                builder.OnStringScalar(reflection->GetString(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                builder.OnInt64Scalar(reflection->GetInt64(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                builder.OnInt64Scalar(reflection->GetInt32(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                builder.OnUint64Scalar(reflection->GetUInt64(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                builder.OnUint64Scalar(reflection->GetUInt32(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                builder.OnDoubleScalar(reflection->GetDouble(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_FLOAT:
                builder.OnDoubleScalar(reflection->GetFloat(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_BOOL:
                builder.OnBooleanScalar(reflection->GetBool(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_ENUM:
                builder.OnStringScalar(reflection->GetEnum(row, fieldDesc)->name());
                break;
            case FieldDescriptor::TYPE_MESSAGE:
                builder.OnStringScalar(reflection->GetMessage(row, fieldDesc).SerializeAsString());
                break;
            default:
                ythrow yexception() << "Invalid field type for column: " << columnName;
                break;
        }
    }

    builder.OnEndMap();
    return node;
}

////////////////////////////////////////////////////////////////////////////////

TProtoTableWriter::TProtoTableWriter(
    THolder<IProxyOutput> output,
    TVector<const Descriptor*>&& descriptors)
    : NodeWriter_(new TNodeTableWriter(std::move(output)))
    , Descriptors_(std::move(descriptors))
{ }

TProtoTableWriter::~TProtoTableWriter()
{ }

size_t TProtoTableWriter::GetBufferMemoryUsage() const
{
    return NodeWriter_->GetBufferMemoryUsage();
}

size_t TProtoTableWriter::GetTableCount() const
{
    return NodeWriter_->GetTableCount();
}

void TProtoTableWriter::FinishTable(size_t tableIndex)
{
    NodeWriter_->FinishTable(tableIndex);
}

void TProtoTableWriter::AddRow(const Message& row, size_t tableIndex)
{
    NodeWriter_->AddRow(MakeNodeFromMessage(row), tableIndex);
}

void TProtoTableWriter::AddRow(Message&& row, size_t tableIndex)
{
    TProtoTableWriter::AddRow(row, tableIndex);
}


void TProtoTableWriter::Abort()
{
    NodeWriter_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

TLenvalProtoTableWriter::TLenvalProtoTableWriter(
    THolder<IProxyOutput> output,
    TVector<const Descriptor*>&& descriptors)
    : Output_(std::move(output))
    , Descriptors_(std::move(descriptors))
{ }

TLenvalProtoTableWriter::~TLenvalProtoTableWriter()
{ }

size_t TLenvalProtoTableWriter::GetBufferMemoryUsage() const
{
    return Output_->GetBufferMemoryUsage();
}

size_t TLenvalProtoTableWriter::GetTableCount() const
{
    return Output_->GetStreamCount();
}

void TLenvalProtoTableWriter::FinishTable(size_t tableIndex)
{
    Output_->GetStream(tableIndex)->Finish();
}

void TLenvalProtoTableWriter::AddRow(const Message& row, size_t tableIndex)
{
    ValidateProtoDescriptor(row, tableIndex, Descriptors_, false);

    Y_ABORT_UNLESS(row.GetReflection()->GetUnknownFields(row).empty(),
        "Message has unknown fields. This probably means bug in client code.\n"
        "Message: %s", row.DebugString().data());

    auto* stream = Output_->GetStream(tableIndex);
    i32 size = row.ByteSizeLong();
    stream->Write(&size, sizeof(size));

    // NB: Scope is essential here since output stream adaptor flushes in destructor.
    {
        TProtobufOutputStreamAdaptor streamAdaptor(stream);
        auto result = row.SerializeToZeroCopyStream(&streamAdaptor);
        Y_ENSURE(result && !streamAdaptor.HasError(), "Failed to serialize protobuf message");
    }

    Output_->OnRowFinished(tableIndex);
}

void TLenvalProtoTableWriter::AddRow(Message&& row, size_t tableIndex)
{
    TLenvalProtoTableWriter::AddRow(row, tableIndex);
}

void TLenvalProtoTableWriter::Abort()
{
    Output_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

TLenvalProtoSingleTableWriter::TLenvalProtoSingleTableWriter(
    THolder<IProxyOutput> output,
    const Descriptor* descriptor)
    : TLenvalProtoTableWriter(std::move(output), {descriptor})
{ }

void TLenvalProtoSingleTableWriter::AddRow(const Message& row, size_t tableIndex)
{
    ValidateProtoDescriptor(row, 0, Descriptors_, false);

    Y_ABORT_UNLESS(row.GetReflection()->GetUnknownFields(row).empty(),
        "Message has unknown fields. This probably means bug in client code.\n"
        "Message: %s", row.DebugString().data());

    auto* stream = Output_->GetStream(tableIndex);
    i32 size = row.ByteSizeLong();
    stream->Write(&size, sizeof(size));

    // NB: Scope is essential here since output stream adaptor flushes in destructor.
    {
        TProtobufOutputStreamAdaptor streamAdaptor(stream);
        auto result = row.SerializeToZeroCopyStream(&streamAdaptor);
        Y_ENSURE(result && !streamAdaptor.HasError(), "Failed to serialize protobuf message");
    }

    Output_->OnRowFinished(tableIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
