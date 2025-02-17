#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/limiter/usage/abstract.h>
#include <ydb/core/tx/limiter/usage/config.h>
#include <ydb/core/tx/limiter/usage/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/accessor/accessor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <queue>

namespace NKikimr::NLimiter {

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
public:
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueCount;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueVolume;
    const ::NMonitoring::TDynamicCounters::TCounterPtr InProgressLimit;

    const ::NMonitoring::TDynamicCounters::TCounterPtr InProgressCount;
    const ::NMonitoring::TDynamicCounters::TCounterPtr InProgressVolume;

    const ::NMonitoring::TDynamicCounters::TCounterPtr InProgressStart;

    const ::NMonitoring::THistogramPtr WaitingHistogram;

    TCounters(const TString& limiterName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
        : TBase("Limiter/" + limiterName, baseSignals)
        , WaitingQueueCount(TBase::GetValue("WaitingQueue/Count"))
        , WaitingQueueVolume(TBase::GetValue("WaitingQueue/Volume"))
        , InProgressLimit(TBase::GetValue("InProgress/Limit/Volume"))
        , InProgressCount(TBase::GetValue("InProgress/Count"))
        , InProgressVolume(TBase::GetValue("InProgress/Volume"))
        , InProgressStart(TBase::GetDeriviative("InProgress"))
        , WaitingHistogram(TBase::GetHistogram("Waiting", NMonitoring::ExponentialHistogram(20, 2))) {
    }
};

class TLimiterActor: public NActors::TActorBootstrapped<TLimiterActor> {
private:
    const TString LimiterName;
    const TConfig Config;
    TCounters Counters;
    class TResourceRequest {
    private:
        YDB_READONLY(TMonotonic, Instant, TMonotonic::Zero());
        YDB_READONLY_DEF(std::shared_ptr<IResourceRequest>, Request);
    public:
        TResourceRequest(const TMonotonic instant, const std::shared_ptr<IResourceRequest>& req)
            : Instant(instant)
            , Request(req) {

        }
    };

    class TResourceRequestInFlight {
    private:
        YDB_READONLY(TMonotonic, Instant, TMonotonic::Zero());
        YDB_READONLY(ui64, Volume, 0);
    public:
        TResourceRequestInFlight(const TMonotonic instant, const ui64 volume)
            : Instant(instant)
            , Volume(volume) {

        }
    };

    ui64 VolumeInFlight = 0;
    ui64 VolumeInWaiting = 0;
    std::deque<TResourceRequest> RequestsQueue;
    std::deque<TResourceRequestInFlight> RequestsInFlight;

    void HandleMain(TEvExternal::TEvAskResource::TPtr& ev);
    void HandleMain(NActors::TEvents::TEvWakeup::TPtr& ev);

public:

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternal::TEvAskResource, HandleMain);
            hFunc(NActors::TEvents::TEvWakeup, HandleMain);
        default:
            AFL_ERROR(NKikimrServices::TX_LIMITER)("limiter", LimiterName)("problem", "unexpected event")("type", ev->GetTypeRewrite());
            AFL_VERIFY_DEBUG(false)("type", ev->GetTypeRewrite());
            break;
        }
    }

    TLimiterActor(const TConfig& config, const TString& limiterName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseCounters);

    void Bootstrap() {
        Become(&TLimiterActor::StateMain);
    }
};

}
