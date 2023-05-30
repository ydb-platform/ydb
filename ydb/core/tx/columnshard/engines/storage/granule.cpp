#include "granule.h"
#include "storage.h"

namespace NKikimr::NOlap {

bool TGranuleMeta::NeedSplit(const TCompactionLimits& limits, bool& inserted) const {
    inserted = GetSummary().GetInserted().GetPortionsCount();
    bool differentBorders = GetSummary().GetDifferentBorders();
    if (GetSummary().GetActivePortionsCount() < 2) {
        inserted = false;
        return false;
    }
    return differentBorders && (GetSummary().GetMaxColumnsSize() >= limits.GranuleBlobSplitSize || GetSummary().GetGranuleSize() >= limits.GranuleOverloadSize);
}

ui64 TGranuleMeta::Size() const {
    return GetSummary().GetGranuleSize();
}

void TGranuleMeta::UpsertPortion(const TPortionInfo& info) {
    Portions[info.Portion()] = info;
    OnAfterChangePortion(info.Portion());
}

bool TGranuleMeta::ErasePortion(const ui64 portion) {
    auto it = Portions.find(portion);
    if (it == Portions.end()) {
        return false;
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "portion_erased")("portion_info", it->second)("pathId", Record.PathId);
    Portions.erase(it);
    OnAfterChangePortion(portion);
    return true;
}

void TGranuleMeta::AddColumnRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec) {
    Portions[rec.Portion].AddRecord(indexInfo, rec);
    OnAfterChangePortion(rec.Portion);
}

void TGranuleMeta::OnAfterChangePortion(const ui64 /*portion*/) {
    ResetCaches();
    Owner->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionFinished() {
    AllowInsertionFlag = false;
    Y_VERIFY(Activity.erase(EActivity::InternalCompaction) || Activity.erase(EActivity::SplitCompaction));
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFinished")("info", DebugString());
    CompactionPriorityInfo.OnCompactionFinished();
    Owner->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionFailed(const TString& reason) {
    AllowInsertionFlag = false;
    Y_VERIFY(Activity.erase(EActivity::InternalCompaction) || Activity.erase(EActivity::SplitCompaction));
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFailed")("reason", reason)("info", DebugString());
    CompactionPriorityInfo.OnCompactionFailed();
    Owner->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionCanceled(const TString& reason) {
    AllowInsertionFlag = false;
    Y_VERIFY(Activity.erase(EActivity::InternalCompaction) || Activity.erase(EActivity::SplitCompaction));
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionCanceled")("reason", reason)("info", DebugString());
    CompactionPriorityInfo.OnCompactionCanceled();
    Owner->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionStarted(const bool inGranule) {
    AllowInsertionFlag = false;
    Y_VERIFY(Activity.empty());
    if (inGranule) {
        Activity.emplace(EActivity::InternalCompaction);
    } else {
        Activity.emplace(EActivity::SplitCompaction);
    }
}

void TGranuleMeta::RebuildMetrics() const {
    TGranuleSummary result;
    std::map<ui32, TColumnSummary> packedSizeByColumns;
    bool differentBorders = false;
    THashSet<NArrow::TReplaceKey> borders;

    for (auto&& i : Portions) {
        if (i.second.IsActive()) {
            if (!differentBorders) {
                borders.insert(i.second.IndexKeyStart());
                borders.insert(i.second.IndexKeyEnd());
                differentBorders = (borders.size() > 1);
            }
            auto sizes = i.second.BlobsSizes();
            for (auto&& c : i.second.Records) {
                auto it = packedSizeByColumns.find(c.ColumnId);
                if (it == packedSizeByColumns.end()) {
                    it = packedSizeByColumns.emplace(c.ColumnId, TColumnSummary(c.ColumnId)).first;
                }
                it->second.AddData(i.second.IsInserted(), c.BlobRange.Size, i.second.NumRows());
            }
            if (i.second.IsInserted()) {
                result.Inserted.PortionsSize += sizes.first;
                result.Inserted.MaxColumnsSize += sizes.second;
                result.Inserted.RecordsCount += i.second.NumRows();
                ++result.Inserted.PortionsCount;
            } else {
                result.Other.PortionsSize += sizes.first;
                result.Other.MaxColumnsSize += sizes.second;
                result.Other.RecordsCount += i.second.NumRows();
                ++result.Other.PortionsCount;
            }
        }
    }
    {
        std::vector<TColumnSummary> transpSorted;
        transpSorted.reserve(packedSizeByColumns.size());
        for (auto&& i : packedSizeByColumns) {
            transpSorted.emplace_back(i.second);
        }
        const auto pred = [](const TColumnSummary& l, const TColumnSummary& r) {
            return l.GetPackedBlobsSize() > r.GetPackedBlobsSize();
        };
        std::sort(transpSorted.begin(), transpSorted.end(), pred);
        std::swap(result.ColumnIdsSortedBySizeDescending, transpSorted);
    }
    result.DifferentBorders = differentBorders;
    SummaryCache = result;
}

} // namespace NKikimr::NOlap
