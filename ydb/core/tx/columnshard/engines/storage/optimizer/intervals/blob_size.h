#pragma once
#include "counters.h"
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

namespace NKikimr::NOlap::NStorageOptimizer {

class TBlobsWithSizeLimit {
private:
    YDB_READONLY(ui64, SizeLimit, 0);
    YDB_READONLY(i64, SizeLimitToMerge, (i64)2 * 1024 * 1024);
    YDB_READONLY(i64, CountLimitToMerge, 8);
    YDB_READONLY(i64, PortionsSize, 0);
    YDB_READONLY(i64, PortionsCount, 0);
    std::map<NArrow::TReplaceKey, std::map<ui64, std::shared_ptr<TPortionInfo>>> Portions;
    std::shared_ptr<TCounters> Counters;
    std::shared_ptr<IStoragesManager> StoragesManager;
public:
    TString DebugString() const {
        return TStringBuilder()
            << "p_count=" << PortionsCount << ";"
            << "p_count_by_key=" << Portions.size() << ";"
            ;
    }

    TBlobsWithSizeLimit(const ui64 limit, const std::shared_ptr<TCounters>& counters, const std::shared_ptr<IStoragesManager>& storagesManager)
        : SizeLimit(limit)
        , Counters(counters)
        , StoragesManager(storagesManager)
    {

    }

    std::shared_ptr<TColumnEngineChanges> BuildMergeTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const;

    void AddPortion(const std::shared_ptr<TPortionInfo>& portion) {
        AFL_VERIFY(portion->BlobsBytes() < SizeLimit);
        AFL_VERIFY(Portions[portion->IndexKeyStart()].emplace(portion->GetPortion(), portion).second);
        PortionsSize += portion->BlobsBytes();
        ++PortionsCount;
        Counters->OnAddSmallPortion();
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
        auto it = Portions.find(portion->IndexKeyStart());
        AFL_VERIFY(it != Portions.end());
        AFL_VERIFY(it->second.erase(portion->GetPortion()));
        if (!it->second.size()) {
            Portions.erase(it);
        }
        PortionsSize -= portion->BlobsBytes();
        AFL_VERIFY(PortionsSize >= 0);
        --PortionsCount;
        AFL_VERIFY(PortionsCount >= 0);
        Counters->OnRemoveSmallPortion();
    }

    std::optional<TOptimizationPriority> GetWeight() const {
        Y_ABORT_UNLESS(Counters->GetSmallCounts() == PortionsCount);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("portions_opt_count", PortionsCount)("counter", (ui64)Counters->SmallPortionsByGranule.get());
        if (PortionsSize > SizeLimitToMerge || PortionsCount > CountLimitToMerge) {
            return TOptimizationPriority::Critical(PortionsCount);
        } else {
            return {};
        }
    }
};

class TBlobsBySize {
private:
    std::map<ui64, TBlobsWithSizeLimit> BlobsBySizeLimit;
public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "(";
        for (auto&& i : BlobsBySizeLimit) {
            sb << "(" << i.first << ":" << i.second.DebugString() << ");";
        }
        sb << ")";
        return sb;
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& portion) {
        auto it = BlobsBySizeLimit.upper_bound(portion->GetBlobBytes());
        if (it != BlobsBySizeLimit.end()) {
            it->second.AddPortion(portion);
        }
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
        auto it = BlobsBySizeLimit.upper_bound(portion->GetBlobBytes());
        if (it != BlobsBySizeLimit.end()) {
            it->second.RemovePortion(portion);
        }
    }

    std::optional<TOptimizationPriority> GetWeight() const {
        std::optional<TOptimizationPriority> result;
        for (auto&& i : BlobsBySizeLimit) {
            auto w = i.second.GetWeight();
            if (!w) {
                continue;
            }
            if (!result || *result < *w) {
                result = w;
            }
        }
        return result;
    }

    const TBlobsWithSizeLimit* GetMaxWeightLimiter() const {
        std::optional<TOptimizationPriority> resultWeight;
        const TBlobsWithSizeLimit* result = nullptr;
        for (auto&& i : BlobsBySizeLimit) {
            auto w = i.second.GetWeight();
            if (!w) {
                continue;
            }
            if (!resultWeight || *resultWeight < *w) {
                resultWeight = w;
                result = &i.second;
            }
        }
        return result;
    }

    std::shared_ptr<TColumnEngineChanges> BuildMergeTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const {
        auto* limiter = GetMaxWeightLimiter();
        if (!limiter) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("fail", "limiter absent");
            return nullptr;
        }
        return limiter->BuildMergeTask(limits, granule, busyPortions);
    }

    TBlobsBySize(const std::shared_ptr<TCounters>& counters, const std::shared_ptr<IStoragesManager>& storagesManager) {
//        BlobsBySizeLimit.emplace(512 * 1024, TBlobsWithSizeLimit(512 * 1024, counters, storagesManager));
        BlobsBySizeLimit.emplace(1024 * 1024, TBlobsWithSizeLimit(1024 * 1024, counters, storagesManager));
    }
};

} // namespace NKikimr::NOlap
