#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

//! #totalTimer must be started at the moment the reader was requested.
TFuture<ITableReaderPtr> CreateTableReader(
    NConcurrency::IAsyncZeroCopyInputStreamPtr inputStream,
    const NProfiling::TWallTimer& totalTimer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
