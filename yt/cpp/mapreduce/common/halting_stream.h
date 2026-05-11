#pragma once

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IAsyncInputStreamPtr CreateHaltingAsyncStream(
    NConcurrency::IAsyncInputStreamPtr underlying,
    i64 bytesBeforeHalt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
