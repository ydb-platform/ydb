#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NCdcAggregator {

namespace TEvCdcAggregator {
    enum EEv {
        EvConfigureTracking = EventSpaceBegin(TKikimrEvents::ES_CDC_AGGREGATOR),
        EvConfigureTrackingResult,
        EvWriteHeartbeats,
        EvWriteHeartbeatsResult,
        EvPartitioningChanged,
        EvPartitioningChangedAck,
        EvDescribeTracking,
        EvDescribeTrackingResult,

        EvEnd
    };

    struct TEvConfigureTracking
        : public TEventPB<TEvConfigureTracking, NKikimrCdcAggregator::TEvConfigureTracking, EvConfigureTracking>
    {};

    struct TEvConfigureTrackingResult
        : public TEventPB<TEvConfigureTrackingResult, NKikimrCdcAggregator::TEvConfigureTrackingResult, EvConfigureTrackingResult>
    {};

    struct TEvWriteHeartbeats
        : public TEventPB<TEvWriteHeartbeats, NKikimrCdcAggregator::TEvWriteHeartbeats, EvWriteHeartbeats>
    {};

    struct TEvWriteHeartbeatsResult
        : public TEventPB<TEvWriteHeartbeatsResult, NKikimrCdcAggregator::TEvWriteHeartbeatsResult, EvWriteHeartbeatsResult>
    {};

    struct TEvPartitioningChanged
        : public TEventPB<TEvPartitioningChanged, NKikimrCdcAggregator::TEvPartitioningChanged, EvPartitioningChanged>
    {};

    struct TEvPartitioningChangedAck
        : public TEventPB<TEvPartitioningChangedAck, NKikimrCdcAggregator::TEvPartitioningChangedAck, EvPartitioningChangedAck>
    {};

    struct TEvDescribeTracking
        : public TEventPB<TEvDescribeTracking, NKikimrCdcAggregator::TEvDescribeTracking, EvDescribeTracking>
    {};

    struct TEvDescribeTrackingResult
        : public TEventPB<TEvDescribeTrackingResult, NKikimrCdcAggregator::TEvDescribeTrackingResult, EvDescribeTrackingResult>
    {};
} // namespace TEvCdcAggregator

namespace TEvCdcAggregatorService {
    enum EEv {
        EvWriteHeartbeat = EventSpaceBegin(TKikimrEvents::ES_CDC_AGGREGATOR) + 512,
        EvWriteHeartbeatResult,

        EvEnd
    };

    struct TEvWriteHeartbeat
        : public TEventLocal<TEvWriteHeartbeat, EvWriteHeartbeat>
    {
        ui64 RecipientTabletId = 0;
        ui64 SourceDataShardId = 0;
        NPQ::THeartbeat Heartbeat;

        TString ToString() const override;
    };

    struct TEvWriteHeartbeatResult
        : public TEventLocal<TEvWriteHeartbeatResult, EvWriteHeartbeatResult>
    {
        enum class EStatus {
            Written,
            Retry,
            Rejected,
        };

        EStatus Status = EStatus::Retry;
        TRowVersion WrittenVersion = TRowVersion::Min();
        TString Error;

        TString ToString() const override;
    };
} // namespace TEvCdcAggregatorService

static_assert(static_cast<ui32>(TEvCdcAggregator::EvEnd) <= static_cast<ui32>(TEvCdcAggregatorService::EvWriteHeartbeat),
    "Expected TEvCdcAggregator::EvEnd <= TEvCdcAggregatorService::EvWriteHeartbeat");

static_assert(TEvCdcAggregatorService::EvEnd < EventSpaceEnd(TKikimrEvents::ES_CDC_AGGREGATOR),
    "Expected TEvCdcAggregatorService::EvEnd < EventSpaceEnd(TKikimrEvents::ES_CDC_AGGREGATOR)");

inline TActorId MakeCdcAggregatorServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("cdcaggr_svc"));
}

} // namespace NKikimr::NCdcAggregator
