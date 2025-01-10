#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

ITableWriterPtr CreateTableWriter(
    NConcurrency::IAsyncZeroCopyOutputStreamPtr outputStream,
    NTableClient::TTableSchemaPtr tableSchema);

// NB(arkady-e1ppa): Promise is expected to be set
// whenever outputStream is closed.
// TODO(arkady-e1ppa): Introduce a separate output stream
// which has this logic built in to make this code less fragile.
ITableFragmentWriterPtr CreateTableFragmentWriter(
    NConcurrency::IAsyncZeroCopyOutputStreamPtr outputStream,
    NTableClient::TTableSchemaPtr tableSchema,
    TFuture<NTableClient::TSignedWriteFragmentResultPtr> asyncResult);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

