#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TQueryStatistics;

} // namespace NProto

struct TQueryStatistics;

constexpr i64 DefaultRowsetProcessingBatchSize = 256;
constexpr i64 DefaultWriteRowsetSize = 256 * DefaultRowsetProcessingBatchSize;
constexpr i64 DefaultMaxJoinBatchSize = 512 * DefaultRowsetProcessingBatchSize;

DEFINE_ENUM(EStatisticsAggregation,
    (None)
    (Depth)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
