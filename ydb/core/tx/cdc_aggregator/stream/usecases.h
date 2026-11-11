#pragma once

#include "stream.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NCdcAggregator {

constexpr size_t MaxHeartbeatsPerBatch = 10000;
constexpr size_t MaxHeartbeatDataSize = 64_KB;

NKikimrCdcAggregator::TEvConfigureTrackingResult ConfigureTracking(
    TStream& stream, const NKikimrCdcAggregator::TEvConfigureTracking& request, TStreamChanges& changes);

NKikimrCdcAggregator::TEvWriteHeartbeatsResult WriteHeartbeats(
    TStream& stream, const NKikimrCdcAggregator::TEvWriteHeartbeats& request, TStreamChanges& changes);

NKikimrCdcAggregator::TEvPartitioningChangedAck PartitioningChanged(
    TStream& stream, const NKikimrCdcAggregator::TEvPartitioningChanged& request, TStreamChanges& changes);

NKikimrCdcAggregator::TEvDescribeTrackingResult DescribeTracking(const TStream& stream);

} // namespace NKikimr::NCdcAggregator
