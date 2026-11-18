#pragma once

#include "cdc_aggregator_stream.h"

#include <ydb/core/base/events.h>
#include <ydb/core/persqueue/common/heartbeat.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NCdcAggregator {

namespace TEvCdcAggregatorHeartbeatBroadcaster {
    enum EEv {
        EvBroadcastHeartbeat = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvBroadcastHeartbeat : public TEventLocal<TEvBroadcastHeartbeat, EvBroadcastHeartbeat> {
        explicit TEvBroadcastHeartbeat(NPQ::THeartbeat heartbeat)
            : Heartbeat(std::move(heartbeat))
        {
        }

        NPQ::THeartbeat Heartbeat;
    };
}

IActor* CreateCdcAggregatorHeartbeatBroadcaster(const TCdcAggregatorStreamTopic& topic);

} // namespace NKikimr::NCdcAggregator
