#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

using TPortionCategoryCounterAgents = NColumnShard::TPortionCategoryCounterAgents;
using TPortionCategoryCounters = NColumnShard::TPortionCategoryCounters;

class TLevelAgents {
private:
    const ui32 LevelId;

public:
    const std::shared_ptr<TPortionCategoryCounterAgents> Portions;

    TLevelAgents(const ui32 levelId, NColumnShard::TCommonCountersOwner& baseOwner)
        : LevelId(levelId)
        , Portions(std::make_shared<TPortionCategoryCounterAgents>(baseOwner, "level=" + ::ToString(LevelId))) {
    }
};

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::vector<std::shared_ptr<TLevelAgents>> Levels;

public:
    TGlobalCounters()
        : TBase("LeveledCompactionOptimizer") {
        for (ui32 i = 0; i <= 10; ++i) {
            Levels.emplace_back(std::make_shared<TLevelAgents>(i, *this));
        }
    }

    static std::shared_ptr<TLevelAgents> GetLevelAgents(const ui32 levelId) {
        AFL_VERIFY(levelId < Singleton<TGlobalCounters>()->Levels.size());
        return Singleton<TGlobalCounters>()->Levels[levelId];
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

    TCounters() {
        for (ui32 i = 0; i < 10; ++i) {
            Levels.emplace_back(i);
        }
    }

    const TLevelCounters& GetLevelCounters(const ui32 levelIdx) const {
        AFL_VERIFY(levelIdx < Levels.size())("idx", levelIdx)("count", Levels.size());
        return Levels[levelIdx];
    }

    void AddPortion(const ui32 levelId, const std::shared_ptr<TPortionInfo>& portion) {
        AFL_VERIFY(levelId < Levels.size());
        Levels[levelId].Portions->AddPortion(portion);
    }

    void RemovePortion(const ui32 levelId, const std::shared_ptr<TPortionInfo>& portion) {
        AFL_VERIFY(levelId < Levels.size());
        Levels[levelId].Portions->RemovePortion(portion);
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
