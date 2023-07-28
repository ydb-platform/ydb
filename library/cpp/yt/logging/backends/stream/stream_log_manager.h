#pragma once

#include <library/cpp/yt/logging/public.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! Creates a dead-simple implementation that synchronously logs
//! all events to #output.
std::unique_ptr<ILogManager> CreateStreamLogManager(IOutputStream* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
