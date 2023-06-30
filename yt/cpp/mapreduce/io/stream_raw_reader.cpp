#include "stream_table_reader.h"

#include "node_table_reader.h"
#include "proto_table_reader.h"
#include "skiff_table_reader.h"
#include "yamr_table_reader.h"

#include <util/system/env.h>
#include <util/string/type.h>

namespace NYT {

template <>
TTableReaderPtr<TNode> CreateTableReader<TNode>(
    IInputStream* stream, const TTableReaderOptions& /*options*/)
{
    auto impl = ::MakeIntrusive<TNodeTableReader>(
        ::MakeIntrusive<NDetail::TInputStreamProxy>(stream));
    return new TTableReader<TNode>(impl);
}

template <>
TTableReaderPtr<TYaMRRow> CreateTableReader<TYaMRRow>(
    IInputStream* stream, const TTableReaderOptions& /*options*/)
{
    auto impl = ::MakeIntrusive<TYaMRTableReader>(
        ::MakeIntrusive<NDetail::TInputStreamProxy>(stream));
    return new TTableReader<TYaMRRow>(impl);
}


namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
    IInputStream* stream,
    const TTableReaderOptions& /* options */,
    const ::google::protobuf::Descriptor* descriptor)
{
    return new TLenvalProtoTableReader(
        ::MakeIntrusive<TInputStreamProxy>(stream),
        {descriptor});
}

::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
    IInputStream* stream,
    const TTableReaderOptions& /* options */,
    TVector<const ::google::protobuf::Descriptor*> descriptors)
{
    return new TLenvalProtoTableReader(
        ::MakeIntrusive<TInputStreamProxy>(stream),
        std::move(descriptors));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
