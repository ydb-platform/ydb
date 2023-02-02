#pragma once
#include "defs.h"
#include "events.h"
#include "counters.h"

#include <ydb/core/base/tablet_pipe.h>
#include <library/cpp/actors/core/event_local.h>
#include <util/stream/str.h>

namespace NKikimr {

struct TEvPipeCache {
    enum EEv {
        EvForward = EventSpaceBegin(TKikimrEvents::ES_PIPECACHE),
        EvUnlink,
        EvGetTabletNode,
        EvForcePipeReconnect,

        EvDeliveryProblem = EvForward + 1 * 512,
        EvGetTabletNodeResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PIPECACHE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PIPECACHE)");

    struct TEvForward : public TEventLocal<TEvForward, EvForward> {
    public:
        THolder<IEventBase> Ev;
        const ui64 TabletId;
        const bool Subscribe;

        TEvForward(IEventBase *ev, ui64 tabletId, bool subscribe = true)
            : Ev(ev)
            , TabletId(tabletId)
            , Subscribe(subscribe)
        {}
    };

    struct TEvUnlink : public TEventLocal<TEvUnlink, EvUnlink> {
        const ui64 TabletId;

        TEvUnlink(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TEvDeliveryProblem : public TEventLocal<TEvDeliveryProblem, EvDeliveryProblem> {
        const ui64 TabletId;
        const bool NotDelivered;

        TEvDeliveryProblem(ui64 tabletId, bool notDelivered)
            : TabletId(tabletId)
            , NotDelivered(notDelivered)
        {}
    };

    /**
     * Requests node id of the given tablet id, where pipe cache is connected
     */
    struct TEvGetTabletNode : public TEventLocal<TEvGetTabletNode, EvGetTabletNode> {
        const ui64 TabletId;

        explicit TEvGetTabletNode(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    /**
     * Invalidate tablet node cache
     */
    struct TEvForcePipeReconnect : public TEventLocal<TEvForcePipeReconnect, EvForcePipeReconnect> {
        const ui64 TabletId;

        explicit TEvForcePipeReconnect(ui64 tabletId) 
            : TabletId(tabletId)
        {
        }
    };

    /**
     * Returns node id of the given tablet id, or zero if there's a connection error
     */
    struct TEvGetTabletNodeResult : public TEventLocal<TEvGetTabletNodeResult, EvGetTabletNodeResult> {
        const ui64 TabletId;
        const ui32 NodeId;

        TEvGetTabletNodeResult(ui64 tabletId, ui32 nodeId)
            : TabletId(tabletId)
            , NodeId(nodeId)
        {}
    };
};

struct TPipePeNodeCacheConfig : public TAtomicRefCount<TPipePeNodeCacheConfig>{
    ui64 TabletCacheLimit;
    TDuration PipeRefreshTime;
    NTabletPipe::TClientConfig PipeConfig;
    ::NMonitoring::TDynamicCounterPtr Counters;

    TPipePeNodeCacheConfig()
        : TabletCacheLimit(500000)
        , PipeRefreshTime(TDuration::Seconds(30))
    {}
};

IActor* CreatePipePeNodeCache(const TIntrusivePtr<TPipePeNodeCacheConfig> &config);
TActorId MakePipePeNodeCacheID(bool allowFollower);

}
