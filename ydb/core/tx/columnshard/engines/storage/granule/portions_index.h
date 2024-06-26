#pragma once
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

class TPortionsPKPoint {
private:
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Start;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Finish;
    THashSet<ui64> PortionIds;
public:
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetStart() const {
        return Start;
    }

    void ProvidePortions(const TPortionsPKPoint& source) {
        for (auto&& i : source.PortionIds) {
            if (source.Finish.contains(i)) {
                continue;
            }
            AFL_VERIFY(PortionIds.emplace(i).second);
        }
    }

    const THashSet<ui64>& GetPortionIds() const {
        return PortionIds;
    }

    bool IsEmpty() const {
        return Start.empty() && Finish.empty();
    }

    void AddContained(const ui64 portionId) {
        AFL_VERIFY(PortionIds.emplace(portionId).second);
    }

    void RemoveContained(const ui64 portionId) {
        AFL_VERIFY(PortionIds.erase(portionId));
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
    const TGranuleMeta& Owner;

    std::map<NArrow::TReplaceKey, TPortionsPKPoint>::iterator InsertPoint(const NArrow::TReplaceKey& key) {
        auto it = Points.find(key);
        if (it == Points.end()) {
            it = Points.emplace(key, TPortionsPKPoint()).first;
            if (it != Points.begin()) {
                auto itPred = it;
                --itPred;
                it->second.ProvidePortions(itPred->second);
            }
        }
        return it;
    }

public:
    TPortionsIndex(const TGranuleMeta& owner)
        : Owner(owner)
    {

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