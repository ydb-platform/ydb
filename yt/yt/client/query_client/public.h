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
    (DepthOmitNode)
);

// Engine's EScanOrder plus Mixed from dissimilar Merge.
DEFINE_ENUM(EReportedScanOrder,
    ((Unknown)   (0))
    ((Unordered) (1))
    ((Ordered)   (2))
    ((Reversed)  (3))
    ((Mixed)     (4))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
