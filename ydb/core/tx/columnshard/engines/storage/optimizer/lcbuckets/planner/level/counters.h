#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

using TPortionCategoryCounterAgents = NColumnShard::TPortionCategoryCounterAgents;
using TPortionCategoryCounters = NColumnShard::TPortionCategoryCounters;

struct TLeveledOptimizer {
    static inline const TString ComponentName = "LeveledCompactionOptimizer";
    static constexpr bool HasAccumulators = false;
};

struct TTilingOptimizer {
    static inline const TString ComponentName = "TilingCompactionOptimizer";
    static constexpr bool HasAccumulators = true;
};


class TLevelAgents {
public:
    const std::shared_ptr<TPortionCategoryCounterAgents> Portions;

    TLevelAgents(const TString& name, NColumnShard::TCommonCountersOwner& baseOwner)
        : Portions(std::make_shared<TPortionCategoryCounterAgents>(baseOwner, name)) {
    }
};

template <typename TOptimizer>
class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::vector<std::shared_ptr<TLevelAgents>> Levels;
    std::vector<std::shared_ptr<TLevelAgents>> Accumulators;

public:
    TGlobalCounters()
        : TBase(TOptimizer::ComponentName) {
        for (ui32 i = 0; i < 10; ++i) {
            Levels.emplace_back(std::make_shared<TLevelAgents>("level=" + ::ToString(i), *this));
        }
        if constexpr (TOptimizer::HasAccumulators) {
            for (ui32 i = 0; i < 10; ++i) {
                Accumulators.emplace_back(std::make_shared<TLevelAgents>("acc=" + ::ToString(i), *this));
            }
        }
    }

    static std::shared_ptr<TLevelAgents> GetLevelAgent(const ui32 idx) {
        auto* self = Singleton<TGlobalCounters>();
        AFL_VERIFY(idx < self->Levels.size());
        return self->Levels[idx];
    }

    static std::shared_ptr<TLevelAgents> GetAccumulatorAgent(const ui32 idx) {
        static_assert(TOptimizer::HasAccumulators, "Optimizer does not support accumulators");
        auto* self = Singleton<TGlobalCounters>();
        AFL_VERIFY(idx < self->Accumulators.size());
        return self->Accumulators[idx];
    }
};

class TLevelCounters {
public:
    const std::shared_ptr<TPortionCategoryCounters> Portions;
    TLevelCounters(const std::shared_ptr<TLevelAgents>& agent)
        : Portions(std::make_shared<TPortionCategoryCounters>(*agent->Portions)) {
    }
};

template <typename TOptimizer>
class TCountersBase {
public:
    std::vector<TLevelCounters> Levels;
    std::vector<TLevelCounters> Accumulators;

    TCountersBase() {
        for (ui32 i = 0; i < 10; ++i) {
            auto agent = TGlobalCounters<TOptimizer>::GetLevelAgent(i);
            Levels.emplace_back(agent);
        }

        if constexpr (TOptimizer::HasAccumulators) {
            for (ui32 i = 0; i < 10; ++i) {
                auto agent = TGlobalCounters<TOptimizer>::GetAccumulatorAgent(i);
                Accumulators.emplace_back(agent);
            }
        }
    }

    const TLevelCounters& GetLevelCounters(const ui32 levelIdx) const {
        AFL_VERIFY(levelIdx < Levels.size())("idx", levelIdx)("count", Levels.size());
        return Levels[levelIdx];
    }

    const TLevelCounters& GetAccumulatorCounters(const ui32 accIdx) const {
        static_assert(TOptimizer::HasAccumulators, "Optimizer does not support accumulators");
        AFL_VERIFY(accIdx < Accumulators.size())("idx", accIdx)("count", Accumulators.size());
        return Accumulators[accIdx];
    }

};

using TCounters = TCountersBase<TLeveledOptimizer>;
using TTilingCounters = TCountersBase<TTilingOptimizer>;

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
