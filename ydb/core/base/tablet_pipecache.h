#pragma once
#include "defs.h"
#include "events.h"
#include "counters.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/event_local.h>
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

    struct TEvForwardOptions {
        // When true (the default) pipe cache will establish a new connection
        // automatically as needed. Specifying false is useful when precise
        // connection management is required, e.g. when client subscribes with
        // the first message and wants all subsequent messages to use the same
        // pipe.
        bool AutoConnect = true;
        // When true (the default) pipe cache will subscribe sender to the pipe
        // state and will send TEvDeliveryProblem with the specified
        // SubscribeCookie on pipe connection failure. Note that only one
        // subscription per sender/tablet pair may be active at any given time,
        // and new subscription overrides any previous subscription. Sender must
        // make sure to send TEvUnlink to unsubscribe from the pipe state when
        // subscription is no longer necessary.
        bool Subscribe = true;
        ui64 SubscribeCookie = 0;
    };

    struct TEvForward : public TEventLocal<TEvForward, EvForward> {
    public:
        THolder<IEventBase> Ev;
        const ui64 TabletId;
        const TEvForwardOptions Options;

        TEvForward(IEventBase *ev, ui64 tabletId, bool subscribe = true, ui64 subscribeCookie = 0)
            : Ev(ev)
            , TabletId(tabletId)
            , Options{
                .Subscribe = subscribe,
                .SubscribeCookie = subscribeCookie,
            }
        {}

        TEvForward(IEventBase *ev, ui64 tabletId, const TEvForwardOptions& options)
            : Ev(ev)
            , TabletId(tabletId)
            , Options(options)
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
        const bool Connected;
        const bool NotDelivered;
        const bool IsDeleted;

        TEvDeliveryProblem(ui64 tabletId, bool connected, bool notDelivered, bool isDeleted)
            : TabletId(tabletId)
            , Connected(connected)
            , NotDelivered(notDelivered)
            , IsDeleted(isDeleted)
        {}

        // For compatibility with existing tests
        TEvDeliveryProblem(ui64 tabletId, bool notDelivered)
            : TabletId(tabletId)
            , Connected(notDelivered ? false : true)
            , NotDelivered(notDelivered)
            , IsDeleted(false)
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

struct TPipePerNodeCacheConfig : public TAtomicRefCount<TPipePerNodeCacheConfig>{
    ui64 TabletCacheLimit = 500000;
    TDuration PipeRefreshTime = TDuration::Zero();
    NTabletPipe::TClientConfig PipeConfig = DefaultPipeConfig();
    ::NMonitoring::TDynamicCounterPtr Counters;

    TPipePerNodeCacheConfig() = default;

    static NTabletPipe::TClientConfig DefaultPipeConfig();
    static NTabletPipe::TClientConfig DefaultPersistentPipeConfig();
};

enum class EPipePerNodeCache {
    Leader,
    Follower,
    Persistent,
};

IActor* CreatePipePerNodeCache(const TIntrusivePtr<TPipePerNodeCacheConfig> &config);
TActorId MakePipePerNodeCacheID(EPipePerNodeCache kind);
TActorId MakePipePerNodeCacheID(bool allowFollower);

}
