#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SolomonLogger, "Solomon");

inline const int DefaultProducerCollectionBatchSize = 100;

inline static const TString IsSolomonPullHeaderName = "X-YT-IsSolomonPull";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
