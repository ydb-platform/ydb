#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

ITableWriterPtr CreateTableWriter(
    NConcurrency::IAsyncZeroCopyOutputStreamPtr outputStream,
    NTableClient::TTableSchemaPtr tableSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

