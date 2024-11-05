#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TInputStreamProxy
    : public TRawTableReader
{
public:
    TInputStreamProxy(IInputStream* stream)
        : Stream_(stream)
    { }

    bool Retry(
        const TMaybe<ui32>& /*rangeIndex*/,
        const TMaybe<ui64>& /*rowIndex*/,
        const std::exception_ptr& /*error*/) override
    {
        return false;
    }

    void ResetRetries() override
    { }

    bool HasRangeIndices() const override
    {
        return false;
    }

protected:
    size_t DoRead(void* buf, size_t len) override
    {
        return Stream_->Read(buf, len);
    }

private:
    IInputStream* Stream_;
};

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
    IInputStream* stream,
    const TTableReaderOptions& /* options */,
    const ::google::protobuf::Descriptor* descriptor);

::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
    IInputStream* stream,
    const TTableReaderOptions& /* options */,
    TVector<const ::google::protobuf::Descriptor*> descriptors);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <>
TTableReaderPtr<TNode> CreateTableReader<TNode>(
    IInputStream* stream, const TTableReaderOptions& options);

template <>
TTableReaderPtr<TYaMRRow> CreateTableReader<TYaMRRow>(
    IInputStream* stream, const TTableReaderOptions& /*options*/);

} // namespace NYT
