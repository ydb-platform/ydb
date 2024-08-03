#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

class TPortionsPKPoint {
private:
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Start;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Finish;
    THashMap<ui64, ui64> PortionIds;
    YDB_READONLY(ui64, MinMemoryRead, 0);

public:
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetStart() const {
        return Start;
    }

    void ProvidePortions(const TPortionsPKPoint& source) {
        MinMemoryRead = 0;
        for (auto&& [i, mem] : source.PortionIds) {
            if (source.Finish.contains(i)) {
                continue;
            }
            AddContained(i, mem);
        }
    }

    const THashMap<ui64, ui64>& GetPortionIds() const {
        return PortionIds;
    }

    bool IsEmpty() const {
        return Start.empty() && Finish.empty();
    }

    void AddContained(const ui32 portionId, const ui64 minMemoryRead) {
        MinMemoryRead += minMemoryRead;
        AFL_VERIFY(PortionIds.emplace(portionId, minMemoryRead).second);
    }

    void RemoveContained(const ui32 portionId, const ui64 minMemoryRead) {
        AFL_VERIFY(minMemoryRead <= MinMemoryRead);
        MinMemoryRead -= minMemoryRead;
        AFL_VERIFY(PortionIds.erase(portionId));
        if (PortionIds.empty()) {
            AFL_VERIFY(!MinMemoryRead);
        }
    }

    void RemoveStart(const std::shared_ptr<TPortionInfo>& p) {
        auto it = Start.find(p->GetPortionId());
        AFL_VERIFY(it != Start.end());
        Start.erase(it);
    }
    void RemoveFinish(const std::shared_ptr<TPortionInfo>& p) {
        auto it = Finish.find(p->GetPortionId());
        AFL_VERIFY(it != Finish.end());
        Finish.erase(it);
    }

    void AddStart(const std::shared_ptr<TPortionInfo>& p) {
        AFL_VERIFY(Start.emplace(p->GetPortionId(), p).second);
    }
    void AddFinish(const std::shared_ptr<TPortionInfo>& p) {
        AFL_VERIFY(Finish.emplace(p->GetPortionId(), p).second);
    }
};

class TPortionsIndex {
private:
    std::map<NArrow::TReplaceKey, TPortionsPKPoint> Points;
    std::map<ui64, i32> CountMemoryUsages;
    const TGranuleMeta& Owner;
    const NColumnShard::TPortionsIndexCounters& Counters;

    std::map<NArrow::TReplaceKey, TPortionsPKPoint>::iterator InsertPoint(const NArrow::TReplaceKey& key) {
        auto it = Points.find(key);
        if (it == Points.end()) {
            it = Points.emplace(key, TPortionsPKPoint()).first;
            if (it != Points.begin()) {
                auto itPred = it;
                --itPred;
                it->second.ProvidePortions(itPred->second);
            }
            ++CountMemoryUsages[it->second.GetMinMemoryRead()];
        }
        return it;
    }

    void RemoveFromMemoryUsageControl(const ui64 mem) {
        auto it = CountMemoryUsages.find(mem);
        AFL_VERIFY(it != CountMemoryUsages.end())("mem", mem);
        if (!--it->second) {
            CountMemoryUsages.erase(it);
        }
    }

public:
    TPortionsIndex(const TGranuleMeta& owner, const NColumnShard::TPortionsIndexCounters& counters)
        : Owner(owner)
        , Counters(counters)
    {

    }

    ui64 GetMinMemoryRead() const {
        if (CountMemoryUsages.empty()) {
            return 0;
        } else {
            return CountMemoryUsages.rbegin()->second;
        }
    }

    const std::map<NArrow::TReplaceKey, TPortionsPKPoint>& GetPoints() const {
        return Points;
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p);

    void RemovePortion(const std::shared_ptr<TPortionInfo>& p);

    class TPortionIntervals {
    private:
        YDB_READONLY_DEF(std::vector<NArrow::TReplaceKeyInterval>, ExcludedIntervals);
    public:
        void Add(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) {
            if (ExcludedIntervals.empty() || ExcludedIntervals.back().GetFinish() != from) {
                ExcludedIntervals.emplace_back(NArrow::TReplaceKeyInterval(from, to));
            } else {
                ExcludedIntervals.back().SetFinish(to);
            }
        }
    };

    TPortionIntervals GetIntervalFeatures(const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions) const;
};


}