#pragma once

#include "stream.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

namespace NKikimr::NCdcAggregator {

NKikimrCdcAggregator::TEvConfigureTrackingResult ConfigureTracking(
    TStream& stream, const NKikimrCdcAggregator::TEvConfigureTracking& request, TStreamChanges& changes);

NKikimrCdcAggregator::TEvWriteHeartbeatsResult WriteHeartbeats(
    TStream& stream, const NKikimrCdcAggregator::TEvWriteHeartbeats& request, TStreamChanges& changes);

NKikimrCdcAggregator::TEvPartitioningChangedAck PartitioningChanged(
    TStream& stream, const NKikimrCdcAggregator::TEvPartitioningChanged& request, TStreamChanges& changes);

NKikimrCdcAggregator::TEvDescribeTrackingResult DescribeTracking(const TStream& stream);

} // namespace NKikimr::NCdcAggregator
