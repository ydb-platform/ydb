#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

class IProxyOutput;
class TNodeTableWriter;

////////////////////////////////////////////////////////////////////////////////

class TProtoTableWriter
    : public IProtoWriterImpl
{
public:
    TProtoTableWriter(
        THolder<IProxyOutput> output,
        TVector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;
    void AddRow(Message&& row, size_t tableIndex) override;

    size_t GetBufferMemoryUsage() const override;
    size_t GetTableCount() const override;
    void FinishTable(size_t) override;
    void Abort() override;

private:
    THolder<TNodeTableWriter> NodeWriter_;
    TVector<const ::google::protobuf::Descriptor*> Descriptors_;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoTableWriter
    : public IProtoWriterImpl
{
public:
    TLenvalProtoTableWriter(
        THolder<IProxyOutput> output,
        TVector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TLenvalProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;
    void AddRow(Message&& row, size_t tableIndex) override;

    size_t GetBufferMemoryUsage() const override;
    size_t GetTableCount() const override;
    void FinishTable(size_t) override;
    void Abort() override;

protected:
    THolder<IProxyOutput> Output_;
    TVector<const ::google::protobuf::Descriptor*> Descriptors_;
};

/// [Deprecated] Create TNode from protobuf message
///
/// @deprecated This function works only in simple cases. On nontrivial messages its interpretation
/// of protobuf messages might significantly differ from YT interpretation.
///
/// This function doesn't support composite types and many attributes supported by protobuf Reader/Writer.
TNode MakeNodeFromMessage(const ::google::protobuf::Message& row);

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoSingleTableWriter
    : public TLenvalProtoTableWriter
{
public:
    TLenvalProtoSingleTableWriter(
        THolder<IProxyOutput> output,
        const ::google::protobuf::Descriptor* descriptor);
    ~TLenvalProtoSingleTableWriter() override = default;

    void AddRow(const Message& row, size_t tableIndex) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
