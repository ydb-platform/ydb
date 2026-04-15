#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/histogram_borders.h>
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
        , WidthHistogram(base.GetModuleId(), "ByLevel/Width", categoryName, NColumnShard::THistorgamBorders::TimeBordersMicroseconds) {
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

class TLevelCounters {
private:
    template <typename TPortion>
    static constexpr bool IsPortionInfoCompatible = std::is_convertible_v<std::shared_ptr<const TPortion>, std::shared_ptr<const NOlap::TPortionInfo>>;

public:
    const std::shared_ptr<TPortionCategoryCounters> Portions;

    explicit TLevelCounters(std::shared_ptr<TPortionCategoryCounters> portions)
        : Portions(std::move(portions)) {
        AFL_VERIFY(Portions);
    }

    template <typename TPortion>
    void AddPortion(const std::shared_ptr<const TPortion>& p) const {
        if constexpr (IsPortionInfoCompatible<TPortion>) {
            Portions->AddPortion(p);
        }
    }

    template <typename TPortion>
    void RemovePortion(const std::shared_ptr<const TPortion>& p) const {
        if constexpr (IsPortionInfoCompatible<TPortion>) {
            Portions->RemovePortion(p);
        }
    }
};

template <typename TPortion>
class TCounters {
public:
    using TLevelCounters = NTiling::TLevelCounters;

private:
    class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
    private:
        using TBase = NColumnShard::TCommonCountersOwner;
        THashMap<TString, std::shared_ptr<TPortionCategoryCounterAgents>> Agents;
        THashMap<TString, std::shared_ptr<TLevelCounters>> Buckets;

        std::shared_ptr<TPortionCategoryCounterAgents> GetBucketAgents(const TString& name) {
            auto it = Agents.find(name);
            if (it != Agents.end()) {
                return it->second;
            }
            auto bucket = std::make_shared<TPortionCategoryCounterAgents>(*this, name);
            Agents.emplace(name, bucket);
            return bucket;
        }

    public:
        TGlobalCounters()
            : TBase("TilingCompactionOptimizer") {
        }

        const TLevelCounters& GetCounter(const TString& name) {
            auto it = Buckets.find(name);
            if (it == Buckets.end()) {
                it = Buckets.emplace(name, std::make_shared<TLevelCounters>(
                    std::make_shared<TPortionCategoryCounters>(*GetBucketAgents(name)))).first;
            }
            return *it->second;
        }
    };

public:
    static const TLevelCounters& GetCounter(const TString& name) {
        return Singleton<TGlobalCounters>()->GetCounter(name);
    }

    static const TLevelCounters& GetMainCounter() {
        return GetCounter("tiling");
    }

    static const TLevelCounters& GetAccumulatorCounter(const ui32 accIdx = 0) {
        return GetCounter("acc=" + ::ToString(accIdx));
    }

    static const TLevelCounters& GetLevelCounter(const ui32 levelIdx) {
        return GetCounter("level=" + ::ToString(levelIdx));
    }

    static const TLevelCounters& GetLastCounter() {
        return GetCounter("last");
    }

    static const TLevelCounters& GetMiddleCounter(const ui32 levelIdx) {
        return GetCounter("middle=" + ::ToString(levelIdx));
    }
};

}  // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
