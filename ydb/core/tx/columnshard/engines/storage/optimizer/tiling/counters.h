#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {


class TPortionCategoryCounterAgents: public NColumnShard::TPortionCategoryCounterAgents {
private:
    using TBase = NColumnShard::TPortionCategoryCounterAgents;

public:
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> Hight;
    TPortionCategoryCounterAgents(TCommonCountersOwner& base, const TString& categoryName)
        : TBase(base, categoryName)
        , Hight(TBase::GetValueAutoAggregations("ByGranule/Level/Hight")){
    }
};

class TPortionCategoryCounters: public NColumnShard::TPortionCategoryCounters {
private:
    using TBase = NColumnShard::TPortionCategoryCounters;
    std::shared_ptr<NColumnShard::TValueAggregationClient> Hight;

public:
    TPortionCategoryCounters(TPortionCategoryCounterAgents& agents)
        : TBase(agents) {
        Hight = agents.Hight->GetClient();
    }

    void SetHight(const i32 hight) {
        Hight->SetValue(hight);
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
        for (ui32 i = 0; i <= 10; ++i) {
            Levels.emplace_back(std::make_shared<TLevelAgents>("level=" + ::ToString(i), *this));
        }
        for (ui32 i = 0; i <= 10; ++i) {
            Accumulators.emplace_back(std::make_shared<TLevelAgents>("acc=" + ::ToString(i), *this));
        }
    }

    static std::shared_ptr<TLevelAgents> GetLevelAgents(const ui32 levelId) {
        AFL_VERIFY(levelId < Singleton<TGlobalCounters>()->Levels.size());
        return Singleton<TGlobalCounters>()->Levels[levelId];
    }

    static std::shared_ptr<TLevelAgents> GetAccumulatorAgents(const ui32 accId) {
        AFL_VERIFY(accId < Singleton<TGlobalCounters>()->Accumulators.size());
        return Singleton<TGlobalCounters>()->Accumulators[accId];
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsCounter(const ui32 levelId) {
        return std::make_shared<TPortionCategoryCounters>(*GetLevelAgents(levelId)->Portions);
    }
};

class TLevelCounters {
public:
    const std::shared_ptr<TPortionCategoryCounters> Portions;
    TLevelCounters(const ui32 levelId)
        : Portions(TGlobalCounters::BuildPortionsCounter(levelId)) {
    }
};

class TCounters {
public:
    std::vector<TLevelCounters> Levels;
    std::vector<TLevelCounters> Accumulators;

    TCounters() {
        for (ui32 i = 0; i < 10; ++i) {
            Levels.emplace_back(i);
        }
        for (ui32 i = 0; i < 10; ++i) {
            Accumulators.emplace_back(i);
        }
    }

    const TLevelCounters& GetLevelCounters(const ui32 levelIdx) const {
        AFL_VERIFY(levelIdx < Levels.size())("idx", levelIdx)("count", Levels.size());
        return Levels[levelIdx];
    }

    const TLevelCounters& GetAccumulatorCounters(const ui32 accIdx) const {
        AFL_VERIFY(accIdx < Levels.size())("idx", accIdx)("count", Accumulators.size());
        return Accumulators[accIdx];
    }
};
}