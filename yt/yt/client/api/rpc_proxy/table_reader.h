#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> CreateTableReader(
    NConcurrency::IAsyncZeroCopyInputStreamPtr inputStream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
