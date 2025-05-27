#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <memory>

namespace NActors {

struct TActorSystemSetup;
struct TExecutorPoolCounters;
struct TActorSystemCounters;


// Periodically collects stats from executor threads and exposes them as mon counters
class TStatsCollectingActor : public TActorBootstrapped<TStatsCollectingActor> {
public:
    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::EActivityType::ACTORLIB_STATS;
    }

    TStatsCollectingActor(
            ui32 intervalSec,
            const TActorSystemSetup& setup,
            NMonitoring::TDynamicCounterPtr counters);
    
    ~TStatsCollectingActor();

    void Bootstrap(const TActorContext& ctx);

    STFUNC(StateWork);

protected:
    virtual void OnWakeup(const TActorContext &ctx);

    const TVector<TExecutorPoolCounters>& GetPoolCounters() const;
    const TActorSystemCounters& GetActorSystemCounters() const;

private:
    void Wakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx);
    
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

} // NActors
