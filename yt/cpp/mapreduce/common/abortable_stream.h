#pragma once

#include <yt/cpp/mapreduce/interface/abortable_stream.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <memory>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IAbortableInputStream> CreateAbortableInputStreamAdapter(
    NConcurrency::IAsyncInputStreamPtr underlyingStream);

std::unique_ptr<IAbortableInputStream> CreateAbortableInputStreamAdapterFallback(
    IInputStream* underlyingStream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
