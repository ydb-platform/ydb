#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

class TPortionInfoStat {
private:
    YDB_READONLY(ui64, MinReadRawBytes, 0);
    YDB_READONLY(ui64, MinReadBlobBytes, 0);

public:
    TPortionInfoStat() = default;

    TPortionInfoStat(const ui64 rawBytes, const ui64 blobBytes)
        : MinReadRawBytes(rawBytes)
        , MinReadBlobBytes(blobBytes)
    {

    }

    void Add(const TPortionInfoStat& source) {
        MinReadRawBytes += source.MinReadRawBytes;
        MinReadBlobBytes += source.MinReadBlobBytes;
    }

    void Sub(const TPortionInfoStat& source) {
        AFL_VERIFY(MinReadRawBytes >= source.MinReadRawBytes);
        MinReadRawBytes -= source.MinReadRawBytes;
        AFL_VERIFY(MinReadBlobBytes >= source.MinReadBlobBytes);
        MinReadBlobBytes -= source.MinReadBlobBytes;
    }

    bool operator!() const {
        return !MinReadBlobBytes && !MinReadRawBytes;
    }
};

class TPortionsPKPoint {
private:
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Start;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Finish;
    THashMap<ui64, TPortionInfoStat> PortionIds;
    YDB_READONLY_DEF(TPortionInfoStat, IntervalStats);

public:
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetStart() const {
        return Start;
    }

    void ProvidePortions(const TPortionsPKPoint& source) {
        MinMemoryRead = TPortionInfoStat();
        for (auto&& [i, stat] : source.PortionIds) {
            if (source.Finish.contains(i)) {
                continue;
            }
            AddContained(i, stat);
        }
    }

    const THashMap<ui64, TPortionInfoStat>& GetPortionIds() const {
        return PortionIds;
    }

    bool IsEmpty() const {
        return Start.empty() && Finish.empty();
    }

    void AddContained(const ui32 portionId, const TPortionInfoStat& stat) {
        MinMemoryRead.Add(stat);
        AFL_VERIFY(PortionIds.emplace(portionId, minMemoryRead).second);
    }

    void RemoveContained(const ui32 portionId, const TPortionInfoStat& stat) {
        MinMemoryRead.Sub(stat);
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

class TIntervalMemoryMonitoring {
private:
    std::map<ui64, i32> CountMemoryUsages;
    const NColumnShard::TIntervalMemoryCounters& Counters;

public:
    void Add(const ui64 mem) {
        ++CountMemoryUsages[mem];
        Counters.MinReadBytes->Set(CountMemoryUsages.rbegin()->first);
    }

    void Remove(const ui64 mem) {
        auto it = CountMemoryUsages.find(mem);
        AFL_VERIFY(it != CountMemoryUsages.end())("mem", mem);
        if (!--it->second) {
            CountMemoryUsages.erase(it);
            Counters.MinReadBytes->Set(CountMemoryUsages.rbegin()->first);
        }
    }

    TIntervalMemoryMonitoring(const NColumnShard::TIntervalMemoryCounters& counters)
        : Counters(counters)
    {
    
    }

    ui64 GetMax() const {
        if (CountMemoryUsages.size()) {
            return CountMemoryUsages.rbegin()->first;
        } else {
            return 0;
        }
    }

    void FlushCounters() const {
        Counters.MinReadBytes->Set(GetMax());
    }
};

class TPortionsIndex {
private:
    std::map<NArrow::TReplaceKey, TPortionsPKPoint> Points;
    TIntervalMemoryMonitoring RawMemoryUsage;
    TIntervalMemoryMonitoring BlobMemoryUsage;
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
            RawMemoryUsage.Add(it->second.GetIntervalStats().GetMinReadRawBytes());
            BlobMemoryUsage.Add(it->second.GetIntervalStats().GetMinReadBlobBytes());
        }
        return it;
    }

    void RemoveFromMemoryUsageControl(const ui64 rawMem, const ui64 blobMem) {
        RawMemoryUsage.Remove(rawMem);
        BlobMemoryUsage.Remove(blobMem);
    }

public:
    TPortionsIndex(const TGranuleMeta& owner, const NColumnShard::TPortionsIndexCounters& counters)
        : Owner(owner)
        , RawMemoryUsage()
        , Counters(counters)
    {

    }

    ui64 GetMinRawMemoryRead() const {
        return RawMemoryUsage.GetMax();
    }

    ui64 GetMinBlobMemoryRead() const {
        return BlobMemoryUsage.GetMax();
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