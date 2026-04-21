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
    const NColumnShard::TIncrementalHistogram WidthHistogram;

    TPortionCategoryCounterAgents(TCommonCountersOwner& base, const TString& categoryName)
        : TBase(base, categoryName)
        , Height(TBase::GetValueAutoAggregations("ByGranule/Level/Height"))
        , WidthHistogram(base.GetModuleId(), "ByLevel/Width", categoryName, NColumnShard::THistorgamBorders::PortionWidthBorders) {
    }
};

class TPortionCategoryCounters: public NColumnShard::TPortionCategoryCounters {
private:
    using TBase = NColumnShard::TPortionCategoryCounters;
    std::shared_ptr<NColumnShard::TValueAggregationClient> Height;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> WidthHistogram;

public:
    TPortionCategoryCounters(TPortionCategoryCounterAgents& agents)
        : TBase(agents)
        , Height(agents.Height->GetClient())
        , WidthHistogram(agents.WidthHistogram.BuildGuard()) {
    }

    void SetHeight(const i32 height) {
        Height->SetValue(height);
    }

    void AddWidth(const ui64 width) {
        WidthHistogram->Add(width, 1);
    }

    void RemoveWidth(const ui64 width) {
        WidthHistogram->Sub(width, 1);
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
    std::shared_ptr<TLevelAgents> LastLevel;
    std::shared_ptr<TLevelAgents> Tiling;

public:
    TGlobalCounters()
        : TBase("TilingCompactionOptimizer") {
        for (ui32 i = 0; i < TILING_LAYERS_COUNT; ++i) {
            Levels.emplace_back(std::make_shared<TLevelAgents>("level=" + ::ToString(i), *this));
            Accumulators.emplace_back(std::make_shared<TLevelAgents>("acc=" + ::ToString(i), *this));
        }
        LastLevel = std::make_shared<TLevelAgents>("last", *this);
        Tiling = std::make_shared<TLevelAgents>("tiling", *this);
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

    static std::shared_ptr<TPortionCategoryCounters> BuildLastClient() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->LastLevel->Portions);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildTilingClient() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->Tiling->Portions);
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
    TLevelCounters LastLevel;
    TLevelCounters Tiling;

    TCounters(): LastLevel(TGlobalCounters::BuildLastClient()), Tiling(TGlobalCounters::BuildTilingClient()) {
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

    const TLevelCounters& GetLastLevelCounters() const {
        return LastLevel;
    }

    const TLevelCounters& GetTilingCounters() const {
        return Tiling;
    }
};

}  // namespace NKikimr::NOlap::NStorageOptimizer::NTiling