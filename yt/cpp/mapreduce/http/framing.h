#pragma once

#include <yt/cpp/mapreduce/interface/abortable_stream.h>

#include <memory>

namespace NYT::NHttpClient {

////////////////////////////////////////////////////////////////////////////////

//! Strips YT HTTP framing (see `X-YT-Framing` in the HTTP proxy reference):
//! returns the payload of data frames and silently skips keep-alive frames.
std::unique_ptr<IAbortableInputStream> CreateUnframingInputStream(
    std::unique_ptr<IAbortableInputStream> underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpClient
