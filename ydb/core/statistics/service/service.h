#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NStat {

struct TStatServiceSettings {
    TDuration AggregateKeepAlivePeriod;
    TDuration AggregateKeepAliveTimeout;
    TDuration AggregateKeepAliveAckTimeout;
    TDuration StatisticsRequestTimeout;
    size_t MaxInFlightTabletRequests;
    size_t FanOutFactor;

    TStatServiceSettings();

    TStatServiceSettings& SetAggregateKeepAlivePeriod(const TDuration& val) {
        AggregateKeepAlivePeriod = val;
        return *this;
    }

    TStatServiceSettings& SetAggregateKeepAliveTimeout(const TDuration& val) {
        AggregateKeepAliveTimeout = val;
        return *this;
    }

    TStatServiceSettings& SetAggregateKeepAliveAckTimeout(const TDuration& val) {
        AggregateKeepAliveAckTimeout = val;
        return *this;
    }

    TStatServiceSettings& SetStatisticsRequestTimeout(const TDuration& val) {
        StatisticsRequestTimeout = val;
        return *this;
    }

    TStatServiceSettings& SetMaxInFlightTabletRequests(size_t val) {
        MaxInFlightTabletRequests = val;
        return *this;
    }

    TStatServiceSettings& SetFanOutFactor(size_t val) {
        FanOutFactor = val;
        return *this;
    }
};

NActors::TActorId MakeStatServiceID(ui32 node);

THolder<NActors::IActor> CreateStatService(const TStatServiceSettings& settings = TStatServiceSettings());

} // NKikimr::NStat
