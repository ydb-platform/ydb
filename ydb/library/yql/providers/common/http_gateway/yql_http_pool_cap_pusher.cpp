#include "yql_http_pool_cap_pusher.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <yql/essentials/utils/log/log.h>

#include <util/string/builder.h>

#include <numeric>

namespace NYql {

namespace {

class THttpPoolCapPusher : public NActors::TActorBootstrapped<THttpPoolCapPusher> {
public:
    THttpPoolCapPusher(
        TPoolSharesProvider provider,
        IHTTPGateway::TWeakPtr gateway,
        TDuration period,
        size_t maxHandlers,
        double minDefaultFraction)
        : Provider(std::move(provider))
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
            sFunc(NActors::TEvents::TEvPoison, PassAway);
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        if (auto gw = Gateway.lock()) {
            THashMap<NDq::TPoolKey, size_t> caps;
            for (const auto& [poolKey, share] : Provider()) {
                const size_t raw = static_cast<size_t>(MaxHandlers * share);
                caps[poolKey] = share > 0.0 ? std::max<size_t>(raw, 1) : 0;
            }
            const size_t scheduledCapsSum = std::accumulate(caps.begin(), caps.end(), size_t{0},
                [](size_t acc, const auto& kv) { return acc + kv.second; });
            const size_t defaultFloor = static_cast<size_t>(MaxHandlers * MinDefaultFraction);
            const size_t defaultCap = std::max(defaultFloor, MaxHandlers > scheduledCapsSum ? MaxHandlers - scheduledCapsSum : 0);
            caps[NDq::TPoolKey{{}, TString{IHTTPGateway::DefaultPoolId}}] = defaultCap;

            TStringBuilder log;
            log << "HttpPoolCapPusher tick: maxHandlers=" << MaxHandlers << " caps:";
            for (const auto& [poolKey, cap] : caps) {
                log << " [" << poolKey.DatabaseId << "/" << poolKey.PoolId << "]=" << cap;
            }
            YQL_LOG(DEBUG) << log;

            gw->UpdatePoolCaps(std::move(caps));
        } else {
            YQL_LOG(DEBUG) << "HttpPoolCapPusher tick: gateway is gone";
        }
        Schedule(Period, new NActors::TEvents::TEvWakeup());
    }

    const TPoolSharesProvider Provider;
    const IHTTPGateway::TWeakPtr Gateway;
    const TDuration Period;
    const size_t MaxHandlers;
    const double MinDefaultFraction;
};

} // namespace

NActors::IActor* CreateHttpPoolCapPusher(
    TPoolSharesProvider provider,
    IHTTPGateway::TWeakPtr gateway,
    TDuration period,
    size_t maxHandlers,
    double minDefaultFraction)
{
    return new THttpPoolCapPusher(std::move(provider), std::move(gateway), period, maxHandlers, minDefaultFraction);
}

} // namespace NYql
