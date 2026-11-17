#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/persqueue/common/heartbeat.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/vector.h>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorHeartbeatPublisherPartition {
    ui32 PartitionId = 0;
    ui64 TabletId = 0;
};

namespace TEvCdcAggregatorHeartbeatPublisher {
    enum EEv {
        EvPublishHeartbeat = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvEnd,
    };

    struct TEvPublishHeartbeat : public TEventLocal<TEvPublishHeartbeat, EvPublishHeartbeat> {
        explicit TEvPublishHeartbeat(NPQ::THeartbeat heartbeat)
            : Heartbeat(std::move(heartbeat))
        {
        }

        NPQ::THeartbeat Heartbeat;
    };
}

IActor* CreateCdcAggregatorHeartbeatPublisher(TString streamPath, const TVector<TCdcAggregatorHeartbeatPublisherPartition>& partitions);

} // namespace NKikimr::NCdcAggregator
