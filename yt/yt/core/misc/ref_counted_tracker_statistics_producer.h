#pragma once

#include "ref_counted_tracker.h"

#include <yt/yt/core/yson/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Creates a producer for constructing the most up-to-date state of RCT.
NYson::TYsonProducer CreateRefCountedTrackerStatisticsProducer();

//! Returns a producer that applies a (reasonable) caching to RCT data.
NYson::TYsonProducer GetCachingRefCountedTrackerStatisticsProducer();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
