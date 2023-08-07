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
        if (!patternCache) {
            ScheduleWakeup(ctx);
            return;
        }

        LoadPatternsToCompileIfNeeded(patternCache);

        TSimpleTimer timer;
        i64 compilationIntervalMs = std::max(static_cast<i64>(WakeupInterval.MilliSeconds()) / 10, MaxCompilationIntervalMs);

        size_t patternsToCompileSize = PatternsToCompile.size();
        for (; PatternToCompileIndex < patternsToCompileSize && compilationIntervalMs > 0; ++PatternToCompileIndex) {
            auto & entry = PatternsToCompile[PatternToCompileIndex];
            if (!entry->IsInCache.load()) {
                continue;
            }

            timer.Reset();

            entry->Pattern->Compile({}, nullptr);
            Counters->CompiledComputationPatterns->Inc();

            entry = nullptr;

            compilationIntervalMs -= static_cast<i64>(timer.Get().MilliSeconds());
        }

        if (PatternToCompileIndex == patternsToCompileSize) {
            PatternsToCompile.clear();
        }

        ScheduleWakeup(ctx);
    }

private:
    void LoadPatternsToCompileIfNeeded(std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> & patternCache) {
        if (PatternsToCompile.size() != 0) {
            return;
        }

        THashMap<TString, std::shared_ptr<NMiniKQL::TPatternCacheEntry>> patternsToCompile;
        patternCache->GetPatternsToCompile(patternsToCompile);

        TVector<std::pair<std::shared_ptr<NMiniKQL::TPatternCacheEntry>, size_t>> patternsToCompileWithAccessSize;
        for (auto & [_, entry] : patternsToCompile) {
            patternsToCompileWithAccessSize.emplace_back(entry, entry->AccessTimes.load());
        }

        std::sort(patternsToCompileWithAccessSize.begin(), patternsToCompileWithAccessSize.end(), [](auto & lhs, auto & rhs) {
            return lhs.second > rhs.second;
        });

        PatternsToCompile.reserve(patternsToCompileWithAccessSize.size());
        for (auto & [entry, _] : patternsToCompileWithAccessSize) {
            PatternsToCompile.push_back(entry);
        }

        *Counters->CompileQueueSize = PatternsToCompile.size();
        PatternToCompileIndex = 0;
    }

    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(WakeupInterval, new TEvents::TEvWakeup());
    }

    static constexpr i64 MaxCompilationIntervalMs = 100;

    TDuration WakeupInterval;
    TIntrusivePtr<TKqpCounters> Counters;

    using PatternsToCompileContainer = TVector<std::shared_ptr<NMiniKQL::TPatternCacheEntry>>;
    using PatternsToCompileContainerIterator = PatternsToCompileContainer::iterator;
    PatternsToCompileContainer PatternsToCompile;
    size_t PatternToCompileIndex = 0;
};

}

IActor* CreateKqpCompileComputationPatternService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpCompileComputationPatternService(serviceConfig.GetCompileComputationPatternServiceConfig(), std::move(counters));
}

} // namespace NKqp
} // namespace NKikimr
