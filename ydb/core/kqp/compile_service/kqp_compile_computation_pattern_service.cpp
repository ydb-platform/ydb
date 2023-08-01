#include "kqp_compile_service.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/events.h>

#include <ydb/library/aclib/aclib.h>

#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

namespace NKikimr {
namespace NKqp {

namespace {

class TKqpCompileComputationPatternService : public TActorBootstrapped<TKqpCompileComputationPatternService> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_COMPUTATION_PATTERN_SERVICE;
    }

    TKqpCompileComputationPatternService(const NKikimrConfig::TTableServiceConfig_TCompileComputationPatternServiceConfig& config,
        TIntrusivePtr<TKqpCounters> counters)
        : WakeupInterval(TDuration::MilliSeconds(config.GetWakeupIntervalMs()))
        , Counters(std::move(counters))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TKqpCompileComputationPatternService::MainState);
        ScheduleWakeup(ctx);
    }
private:
    STFUNC(MainState) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        default:
            Y_FAIL("TKqpCompileComputationPatternService: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    void HandleWakeup(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_COMPUTATION_PATTERN_SERVICE, "Received wakeup");
        auto patternCache = GetKqpResourceManager()->GetPatternCache();
        if (patternCache) {
            THashMap<TString, std::shared_ptr<NMiniKQL::TPatternCacheEntry>> patternsToCompile;
            patternCache->GetPatternsToCompile(patternsToCompile);

            *Counters->CompileQueueSize = patternsToCompile.size();

            for (auto & [_, entry] : patternsToCompile) {
                entry->Pattern->Compile({}, nullptr);
                Counters->CompiledComputationPatterns->Inc();
            }
        }

        ScheduleWakeup(ctx);
    }

private:
    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(WakeupInterval, new TEvents::TEvWakeup());
    }

    TDuration WakeupInterval;
    TIntrusivePtr<TKqpCounters> Counters;
};

}

IActor* CreateKqpCompileComputationPatternService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpCompileComputationPatternService(serviceConfig.GetCompileComputationPatternServiceConfig(), std::move(counters));
}

} // namespace NKqp
} // namespace NKikimr
