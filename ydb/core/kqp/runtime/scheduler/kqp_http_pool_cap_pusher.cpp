#include "kqp_http_pool_cap_pusher.h"

#include "tree/snapshot.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NKqp::NScheduler {

namespace {

class THttpPoolCapPusher : public NActors::TActorBootstrapped<THttpPoolCapPusher> {
public:
    THttpPoolCapPusher(
        TComputeSchedulerPtr scheduler,
        NYql::IHTTPGateway::TWeakPtr gateway,
        TDuration period,
        size_t maxHandlers,
        double minDefaultFraction)
        : Scheduler(std::move(scheduler))
        , Gateway(std::move(gateway))
        , Period(period)
        , MaxHandlers(maxHandlers)
        , MinDefaultFraction(minDefaultFraction)
    {}

    void Bootstrap() {
        Schedule(Period, new NActors::TEvents::TEvWakeup());
        Become(&THttpPoolCapPusher::StateFunc);
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        if (auto gw = Gateway.lock()) {
            THashMap<TString, size_t> caps;
            const auto totalCpu = Scheduler->GetTotalCpuLimit();
            if (auto snapshot = Scheduler->GetSnapshot(); snapshot && totalCpu > 0) {
                snapshot->ForEachChild<NHdrf::NSnapshot::TDatabase>([&](auto* database, size_t) {
                    database->template ForEachChild<NHdrf::NSnapshot::TPool>([&](auto* pool, size_t) {
                        const auto& poolId = std::get<NHdrf::TPoolId>(pool->GetId());
                        const double share = double(pool->FairShare) / totalCpu;
                        caps[poolId] = static_cast<size_t>(MaxHandlers * share);
                    });
                });
            }
            const size_t s3Sum = std::accumulate(caps.begin(), caps.end(), size_t{0},
                [](size_t acc, const auto& kv) { return acc + kv.second; });
            const size_t defaultFloor = static_cast<size_t>(MaxHandlers * MinDefaultFraction);
            const size_t defaultCap = std::max(defaultFloor, MaxHandlers > s3Sum ? MaxHandlers - s3Sum : 0);
            caps[NYql::IHTTPGateway::DefaultPoolId] = defaultCap;
            gw->UpdatePoolCaps(std::move(caps));
        }
        Schedule(Period, new NActors::TEvents::TEvWakeup());
    }

    const TComputeSchedulerPtr Scheduler;
    const NYql::IHTTPGateway::TWeakPtr Gateway;
    const TDuration Period;
    const size_t MaxHandlers;
    const double MinDefaultFraction;
};

} // namespace

NActors::IActor* CreateHttpPoolCapPusher(
    TComputeSchedulerPtr scheduler,
    NYql::IHTTPGateway::TWeakPtr gateway,
    TDuration period,
    size_t maxHandlers,
    double minDefaultFraction)
{
    return new THttpPoolCapPusher(std::move(scheduler), std::move(gateway), period, maxHandlers, minDefaultFraction);
}

void RegisterHttpPoolCapPusherIfNeeded(NActors::TActorSystem* actorSystem, NYql::IHTTPGateway::TPtr gateway) {
    if (!actorSystem || !gateway) {
        return;
    }
    auto* appData = NKikimr::AppData(actorSystem);
    if (!appData || !appData->KqpComputeScheduler) {
        return;
    }
    // TODO: read Period/MaxHandlers/MinDefaultFraction from THttpGatewayConfig
    actorSystem->Register(CreateHttpPoolCapPusher(
        appData->KqpComputeScheduler,
        gateway,
        TDuration::MilliSeconds(500),
        1024,
        0.1));
}

} // namespace NKikimr::NKqp::NScheduler
