#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SolomonLogger, "Solomon");

inline const int DefaultProducerCollectionBatchSize = 100;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
