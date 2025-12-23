#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portions.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

static constexpr ui32 TILING_LAYERS_COUNT = 10;

class TPortionCategoryCounterAgents: public NColumnShard::TPortionCategoryCounterAgents {
private:
    using TBase = NColumnShard::TPortionCategoryCounterAgents;

public:
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> Height;

    TPortionCategoryCounterAgents(TCommonCountersOwner& base, const TString& categoryName)
        : TBase(base, categoryName)
        , Height(TBase::GetValueAutoAggregations("ByGranule/Level/Height")){
    }
};

class TPortionCategoryCounters: public NColumnShard::TPortionCategoryCounters {
private:
    using TBase = NColumnShard::TPortionCategoryCounters;
    std::shared_ptr<NColumnShard::TValueAggregationClient> Height;

public:
    TPortionCategoryCounters(TPortionCategoryCounterAgents& agents)
        : TBase(agents) {
        Height = agents.Height->GetClient();
    }

    void SetHeight(const i32 height) {
        Height->SetValue(height);
    }
};

class TLevelAgents {
public:
    const std::shared_ptr<TPortionCategoryCounterAgents> Portions;

    TLevelAgents(const TString& name, NColumnShard::TCommonCountersOwner& baseOwner)
        : Portions(std::make_shared<TPortionCategoryCounterAgents>(baseOwner, name)) {
    }
};

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::vector<std::shared_ptr<TLevelAgents>> Levels;
    std::vector<std::shared_ptr<TLevelAgents>> Accumulators;

public:
    TGlobalCounters()
        : TBase("TilingCompactionOptimizer") {
        for (ui32 i = 0; i < TILING_LAYERS_COUNT; ++i) {
            Levels.emplace_back(std::make_shared<TLevelAgents>("level=" + ::ToString(i), *this));
            Accumulators.emplace_back(std::make_shared<TLevelAgents>("acc=" + ::ToString(i), *this));
        }
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildClient(
        const std::vector<std::shared_ptr<TLevelAgents>>& agentList,
        const ui32 idx,
        const TString& debugName)
    {
        AFL_VERIFY(idx < agentList.size())("idx", idx)("limit", agentList.size())("type", debugName);
        return std::make_shared<TPortionCategoryCounters>(*agentList[idx]->Portions);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildLevelClient(const ui32 levelId) {
        return BuildClient(Singleton<TGlobalCounters>()->Levels, levelId, "level");
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildAccumulatorClient(const ui32 accId) {
        return BuildClient(Singleton<TGlobalCounters>()->Accumulators, accId, "accumulator");
    }
};

class TLevelCounters {
public:
    const std::shared_ptr<TPortionCategoryCounters> Portions;

    explicit TLevelCounters(std::shared_ptr<TPortionCategoryCounters> portions)
        : Portions(std::move(portions)) {
        AFL_VERIFY(Portions);
    }
};

class TCounters {
public:
    std::vector<TLevelCounters> Levels;
    std::vector<TLevelCounters> Accumulators;

    TCounters() {
        for (ui32 i = 0; i < TILING_LAYERS_COUNT; ++i) {
            Levels.emplace_back(TGlobalCounters::BuildLevelClient(i));
            Accumulators.emplace_back(TGlobalCounters::BuildAccumulatorClient(i));
        }
    }

    const TLevelCounters& GetLevelCounters(const ui32 levelIdx) const {
        AFL_VERIFY(levelIdx < Levels.size())("idx", levelIdx)("count", Levels.size());
        return Levels[levelIdx];
    }

    const TLevelCounters& GetAccumulatorCounters(const ui32 accIdx) const {
        AFL_VERIFY(accIdx < Accumulators.size())("idx", accIdx)("count", Accumulators.size());
        return Accumulators[accIdx];
    }
};

}  // namespace NKikimr::NOlap::NStorageOptimizer::NTiling