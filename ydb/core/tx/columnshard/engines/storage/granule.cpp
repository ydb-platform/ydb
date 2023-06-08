#include "granule.h"
#include "storage.h"

namespace NKikimr::NOlap {

bool TGranuleMeta::NeedSplit(const TCompactionLimits& limits, bool& inserted) const {
    inserted = GetAdditiveSummary().GetInserted().GetPortionsCount();
    bool differentBorders = GetHardSummary().GetDifferentBorders();
    if (GetAdditiveSummary().GetActivePortionsCount() < 2) {
        inserted = false;
        return false;
    }
    return differentBorders && (GetAdditiveSummary().GetMaxColumnsSize() >= limits.GranuleBlobSplitSize ||
        GetAdditiveSummary().GetGranuleSize() >= limits.GranuleOverloadSize);
}

ui64 TGranuleMeta::Size() const {
    return GetAdditiveSummary().GetGranuleSize();
}

void TGranuleMeta::UpsertPortion(const TPortionInfo& info) {
    auto it = Portions.find(info.Portion());
    if (it == Portions.end()) {
        OnBeforeChangePortion(nullptr, &info);
        Portions.emplace(info.Portion(), info);
    } else {
        OnBeforeChangePortion(&it->second, &info);
        it->second = info;
    }
    OnAfterChangePortion();
}

bool TGranuleMeta::ErasePortion(const ui64 portion) {
    auto it = Portions.find(portion);
    if (it == Portions.end()) {
        return false;
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "portion_erased")("portion_info", it->second)("pathId", Record.PathId);
    OnBeforeChangePortion(&it->second, nullptr);
    Portions.erase(it);
    OnAfterChangePortion();
    return true;
}

void TGranuleMeta::AddColumnRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec) {
    auto& portion = Portions[rec.Portion];
    auto portionNew = portion;
    portionNew.AddRecord(indexInfo, rec);
    OnBeforeChangePortion(&portion, &portionNew);
    portion = std::move(portionNew);
    OnAfterChangePortion();
}

void TGranuleMeta::OnAfterChangePortion() {
    Owner->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnBeforeChangePortion(const TPortionInfo* portionBefore, const TPortionInfo* portionAfter) {
    HardSummaryCache = {};
    if (!!AdditiveSummaryCache) {
        if (portionBefore && portionBefore->IsActive()) {
            AdditiveSummaryCache->RemovePortion(*portionBefore);
        }
        if (portionAfter && portionAfter->IsActive()) {
            AdditiveSummaryCache->AddPortion(*portionAfter);
        }
        OnAdditiveSummaryChange();
    }
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

void TGranuleMeta::RebuildHardMetrics() const {
    TGranuleHardSummary result;
    std::map<ui32, TColumnSummary> packedSizeByColumns;
    bool differentBorders = false;
    THashSet<NArrow::TReplaceKey> borders;

    for (auto&& i : Portions) {
        if (!i.second.IsActive()) {
            continue;
        }
        if (!differentBorders) {
            borders.insert(i.second.IndexKeyStart());
            borders.insert(i.second.IndexKeyEnd());
            differentBorders = (borders.size() > 1);
        }
        for (auto&& c : i.second.Records) {
            auto it = packedSizeByColumns.find(c.ColumnId);
            if (it == packedSizeByColumns.end()) {
                it = packedSizeByColumns.emplace(c.ColumnId, TColumnSummary(c.ColumnId)).first;
            }
            it->second.AddData(i.second.IsInserted(), c.BlobRange.Size, i.second.NumRows());
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
    HardSummaryCache = result;
}

void TGranuleMeta::RebuildAdditiveMetrics() const {
    TGranuleAdditiveSummary result;

    for (auto&& i : Portions) {
        if (!i.second.IsActive()) {
            continue;
        }
        result.AddPortion(i.second);
    }
    AdditiveSummaryCache = result;
}

const NKikimr::NOlap::TGranuleAdditiveSummary& TGranuleMeta::GetAdditiveSummary() const {
    if (!AdditiveSummaryCache) {
        RebuildAdditiveMetrics();
        OnAdditiveSummaryChange();
    }
    return *AdditiveSummaryCache;
}

void TGranuleMeta::OnAdditiveSummaryChange() const {
    if (AdditiveSummaryCache) {
        Counters.OnCompactedData(AdditiveSummaryCache->GetOther());
        Counters.OnInsertedData(AdditiveSummaryCache->GetInserted());
        Counters.OnFullData(AdditiveSummaryCache->GetGranuleSize(), AdditiveSummaryCache->GetActivePortionsCount());
    }
}

} // namespace NKikimr::NOlap
