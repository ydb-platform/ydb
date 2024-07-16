#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr {
namespace NStat {

struct StatServiceSettings {
    TDuration AggregateKeepAlivePeriod;
    TDuration AggregateKeepAliveTimeout;
    TDuration AggregateKeepAliveAckTimeout;
    size_t MaxInFlightTabletRequests;

    StatServiceSettings();

    StatServiceSettings& SetAggregateKeepAlivePeriod(const TDuration& val) {
        AggregateKeepAlivePeriod = val;
        return *this;
    }

    StatServiceSettings& SetAggregateKeepAliveTimeout(const TDuration& val) {
        AggregateKeepAliveTimeout = val;
        return *this;
    }

    StatServiceSettings& SetAggregateKeepAliveAckTimeout(const TDuration& val) {
        AggregateKeepAliveAckTimeout = val;
        return *this;
    }

    StatServiceSettings& SetMaxInFlightTabletRequests(size_t val) {
        MaxInFlightTabletRequests = val;
        return *this;
    }
};

struct TEvStatService {
    enum EEv {
        EvRequestTimeout = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDispatchKeepAlive,
        EvKeepAliveTimeout,
        EvKeepAliveAckTimeout,
        EvResetAggregatedResponse,

        EvEnd
    };

    struct TEvRequestTimeout : public NActors::TEventLocal<TEvRequestTimeout, EvRequestTimeout> {
        std::unordered_set<ui64> NeedSchemeShards;
        NActors::TActorId PipeClientId;
    };

    struct TEvDispatchKeepAlive: public NActors::TEventLocal<TEvDispatchKeepAlive, EvDispatchKeepAlive> {
        TEvDispatchKeepAlive(ui64 round): Round(round) {}

        ui64 Round;
    };

    struct TEvKeepAliveAckTimeout: public NActors::TEventLocal<TEvKeepAliveAckTimeout, EvKeepAliveAckTimeout> {
        TEvKeepAliveAckTimeout(ui64 round): Round(round) {}

        ui64 Round;
    };

    struct TEvKeepAliveTimeout: public NActors::TEventLocal<TEvKeepAliveTimeout, EvKeepAliveTimeout> {
        TEvKeepAliveTimeout(ui64 round, ui32 nodeId): Round(round), NodeId(nodeId) {}

        ui64 Round;
        ui32 NodeId;
    };
};

inline NActors::TActorId MakeStatServiceID(ui32 node) {
    const char x[12] = "StatService";
    return NActors::TActorId(node, TStringBuf(x, 12));
}

THolder<NActors::IActor> CreateStatService(const StatServiceSettings& settings = StatServiceSettings());

} // NStat
} // NKikimr
