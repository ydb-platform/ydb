#include "kqp_compile_service.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>

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
            Y_ABORT("TKqpCompileComputationPatternService: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
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
        i64 compilationIntervalMs = MaxCompilationIntervalMs;

        size_t patternsToCompileSize = PatternsToCompile.size();
        for (; PatternToCompileIndex < patternsToCompileSize && compilationIntervalMs > 0; ++PatternToCompileIndex) {
            auto& patternToCompile = PatternsToCompile[PatternToCompileIndex];
            if (!patternToCompile.Entry->IsInCache.load()) {
                continue;
            }

            timer.Reset();

            patternToCompile.Entry->Pattern->Compile({}, nullptr);
            patternCache->NotifyPatternCompiled(patternToCompile.SerializedProgram);
            patternToCompile.Entry = nullptr;

            Counters->CompiledComputationPatterns->Inc();
            compilationIntervalMs -= static_cast<i64>(timer.Get().MilliSeconds());
        }

        Counters->CompileComputationPatternsQueueSize->Set(PatternsToCompile.size() - PatternToCompileIndex);

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

        TVector<std::pair<TPatternToCompile, size_t>> patternsToCompileWithAccessTimes;
        for (auto& [serializedProgram, entry] : patternsToCompile) {
            patternsToCompileWithAccessTimes.emplace_back(TPatternToCompile{serializedProgram, entry}, entry->AccessTimes.load());
        }

        std::sort(patternsToCompileWithAccessTimes.begin(), patternsToCompileWithAccessTimes.end(), [](auto & lhs, auto & rhs) {
            return lhs.second > rhs.second;
        });

        PatternsToCompile.reserve(patternsToCompileWithAccessTimes.size());
        for (auto& [patternToCompile, _] : patternsToCompileWithAccessTimes) {
            PatternsToCompile.push_back(patternToCompile);
        }

        Counters->CompileComputationPatternsQueueSize->Set(PatternsToCompile.size());
        PatternToCompileIndex = 0;
    }

    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(WakeupInterval, new TEvents::TEvWakeup());
    }

    static constexpr i64 MaxCompilationIntervalMs = 300;

    TDuration WakeupInterval;
    TIntrusivePtr<TKqpCounters> Counters;

    struct TPatternToCompile {
        TString SerializedProgram;
        std::shared_ptr<NMiniKQL::TPatternCacheEntry> Entry;
    };

    using TPatternsToCompileContainer = TVector<TPatternToCompile>;
    TPatternsToCompileContainer PatternsToCompile;
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
