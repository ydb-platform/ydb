#pragma once

#include <library/cpp/yt/logging/public.h>

#include <util/generic/ptr.h>

class TLogBackend;

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! Create TLogBackend which redirects log messages to |logger|.
THolder<TLogBackend> CreateArcadiaLogBackend(const TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
