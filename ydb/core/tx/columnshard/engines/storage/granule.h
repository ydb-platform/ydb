#pragma once
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include "optimizer/optimizer.h"

namespace NKikimr::NOlap {

class TGranulesStorage;

class TDataClassSummary: public NColumnShard::TBaseGranuleDataClassSummary {
private:
    friend class TGranuleMeta;
    THashMap<ui32, TSimpleSerializationStat> ColumnStats;
public:
    const THashMap<ui32, TSimpleSerializationStat>& GetColumnStats() const {
        return ColumnStats;
    }

    void AddPortion(const TPortionInfo& info) {
        const auto sizes = info.BlobsSizes();
        PortionsSize += sizes.first;
        RecordsCount += info.NumRows();
        ++PortionsCount;

        for (auto&& c : info.Records) {
            auto it = ColumnStats.find(c.ColumnId);
            if (it == ColumnStats.end()) {
                it = ColumnStats.emplace(c.ColumnId, c.GetSerializationStat()).first;
            } else {
                it->second.AddStat(c.GetSerializationStat());
            }
        }
    }

    void RemovePortion(const TPortionInfo& info) {
        const auto sizes = info.BlobsSizes();
        PortionsSize -= sizes.first;
        Y_VERIFY(PortionsSize >= 0);
        RecordsCount -= info.NumRows();
        Y_VERIFY(RecordsCount >= 0);
        --PortionsCount;
        Y_VERIFY(PortionsCount >= 0);

        for (auto&& c : info.Records) {
            auto it = ColumnStats.find(c.ColumnId);
            if (it == ColumnStats.end()) {
                it = ColumnStats.emplace(c.ColumnId, c.GetSerializationStat()).first;
            } else {
                it->second.RemoveStat(c.GetSerializationStat());
            }
        }
    }
};

class TGranuleAdditiveSummary {
private:
    TDataClassSummary Inserted;
    TDataClassSummary Compacted;
    friend class TGranuleMeta;
public:
    enum class ECompactionClass: ui32 {
        Split = 100,
        Internal = 50,
        WaitInternal = 30,
        NoCompaction = 0
    };

    ECompactionClass GetCompactionClass(const TCompactionLimits& limits, const TMonotonic lastModification, const TMonotonic now) const {
        if (GetActivePortionsCount() <= 1) {
            return ECompactionClass::NoCompaction;
        }
        if ((i64)GetGranuleSize() >= limits.GranuleSizeForOverloadPrevent)
        {
            return ECompactionClass::Split;
        }

        if (now - lastModification > TDuration::Seconds(limits.InGranuleCompactSeconds)) {
            if (GetInserted().GetPortionsCount()) {
                return ECompactionClass::Internal;
            }
        } else {
            if (GetInserted().GetPortionsCount() > 1 &&
                (GetInserted().GetPortionsSize() >= limits.GranuleIndexedPortionsSizeLimit ||
                    GetInserted().GetPortionsCount() >= limits.GranuleIndexedPortionsCountLimit)) {
                return ECompactionClass::Internal;
            }
            if (GetInserted().GetPortionsCount()) {
                return ECompactionClass::WaitInternal;
            }
        }

        return ECompactionClass::NoCompaction;
    }

    const TDataClassSummary& GetInserted() const {
        return Inserted;
    }
    const TDataClassSummary& GetCompacted() const {
        return Compacted;
    }
    ui64 GetGranuleSize() const {
        return Inserted.GetPortionsSize() + Compacted.GetPortionsSize();
    }
    ui64 GetActivePortionsCount() const {
        return Inserted.GetPortionsCount() + Compacted.GetPortionsCount();
    }

    class TEditGuard: TNonCopyable {
    private:
        const NColumnShard::TGranuleDataCounters& Counters;
        TGranuleAdditiveSummary& Owner;
    public:
        TEditGuard(const NColumnShard::TGranuleDataCounters& counters, TGranuleAdditiveSummary& owner)
            : Counters(counters)
            , Owner(owner)
        {

        }

        ~TEditGuard() {
            Counters.OnPortionsDataRefresh(Owner.GetInserted(), Owner.GetCompacted());
        }

        void AddPortion(const TPortionInfo& info) {
            if (info.IsInserted()) {
                Owner.Inserted.AddPortion(info);
            } else {
                Owner.Compacted.AddPortion(info);
            }
        }
        void RemovePortion(const TPortionInfo& info) {
            if (info.IsInserted()) {
                Owner.Inserted.RemovePortion(info);
            } else {
                Owner.Compacted.RemovePortion(info);
            }
        }
    };

    TEditGuard StartEdit(const NColumnShard::TGranuleDataCounters& counters) {
        return TEditGuard(counters, *this);
    }

    TString DebugString() const {
        return TStringBuilder() << "inserted:(" << Inserted.DebugString() << ");other:(" << Compacted.DebugString() << "); ";
    }
};

class TCompactionPriority {
private:
    i64 Weight = 0;
    TMonotonic ConstructionInstant = TMonotonic::Now();
public:
    i64 GetWeight() const {
        return Weight;
    }

    TCompactionPriority(std::shared_ptr<NStorageOptimizer::IOptimizerPlanner> planner)
        : Weight(planner->GetUsefulMetric())
    {

    }
    bool operator<(const TCompactionPriority& item) const {
        return std::tie(Weight) < std::tie(item.Weight);
    }

    TString DebugString() const {
        return TStringBuilder() << "summary:(" << Weight << ");";
    }

};

class TGranuleMeta: TNonCopyable {
public:
    enum class EActivity {
        GeneralCompaction
    };

private:
    TMonotonic ModificationLastTime = TMonotonic::Now();
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    mutable std::optional<TGranuleAdditiveSummary> AdditiveSummaryCache;

    void RebuildHardMetrics() const;
    void RebuildAdditiveMetrics() const;

    std::set<EActivity> Activity;
    mutable bool AllowInsertionFlag = false;
    std::shared_ptr<TGranulesStorage> Owner;
    const NColumnShard::TGranuleDataCounters Counters;
    NColumnShard::TEngineLogsCounters::TPortionsInfoGuard PortionInfoGuard;
    std::shared_ptr<NStorageOptimizer::IOptimizerPlanner> OptimizerPlanner;

    void OnBeforeChangePortion(const std::shared_ptr<TPortionInfo> portionBefore);
    void OnAfterChangePortion(const std::shared_ptr<TPortionInfo> portionAfter);
    void OnAdditiveSummaryChange() const;
public:
    std::shared_ptr<TColumnEngineChanges> GetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> self, const THashSet<TPortionAddress>& busyPortions) const {
        return OptimizerPlanner->GetOptimizationTask(limits, self, busyPortions);
    }

    std::vector<std::shared_ptr<TPortionInfo>> GroupOrderedPortionsByPK(const TSnapshot& snapshot) const {
        return OptimizerPlanner->GetPortionsOrderedByPK(snapshot);
    }

    void OnAfterPortionsLoad() {
        for (auto&& i : Portions) {
            OnAfterChangePortion(i.second);
        }
    }

    NOlap::TSerializationStats BuildSerializationStats(ISnapshotSchema::TPtr schema) const {
        NOlap::TSerializationStats result;
        for (auto&& i : GetAdditiveSummary().GetCompacted().GetColumnStats()) {
            auto field = schema->GetFieldByColumnId(i.first);
            AFL_VERIFY(field)("column_id", i.first)("schema", schema->DebugString());
            NOlap::TColumnSerializationStat columnInfo(i.first, field->name());
            columnInfo.Merge(i.second);
            result.AddStat(columnInfo);
        }
        return result;
    }

    TGranuleAdditiveSummary::ECompactionClass GetCompactionType(const TCompactionLimits& limits) const;
    const TGranuleAdditiveSummary& GetAdditiveSummary() const;
    TCompactionPriority GetCompactionPriority() const {
        return TCompactionPriority(OptimizerPlanner);
    }

    bool NeedCompaction(const TCompactionLimits& limits) const {
        if (InCompaction() || Empty()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "granule_skipped_by_state")("granule_id", GetGranuleId())("granule_size", Size());
            return false;
        }
        return GetCompactionType(limits) != TGranuleAdditiveSummary::ECompactionClass::NoCompaction;
    }

    bool InCompaction() const;

    bool IsErasable() const {
        return Activity.empty();
    }

    void OnCompactionStarted();

    void OnCompactionFailed(const TString& reason);
    void OnCompactionFinished();

    void UpsertPortion(const TPortionInfo& info);

    TString DebugString() const {
        return TStringBuilder() << "(granule:" << GetGranuleId() << ";"
            << "path_id:" << Record.PathId << ";"
            << "size:" << GetAdditiveSummary().GetGranuleSize() << ";"
            << "portions_count:" << Portions.size() << ";"
            << ")"
            ;
    }

    const TGranuleRecord Record;

    void AddColumnRecord(const TIndexInfo& indexInfo, const TPortionInfo& portion, const TColumnRecord& rec, const NKikimrTxColumnShard::TIndexPortionMeta* portionMeta);

    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }

    ui64 GetPathId() const {
        return Record.PathId;
    }

    const TPortionInfo& GetPortionVerified(const ui64 portion) const {
        auto it = Portions.find(portion);
        AFL_VERIFY(it != Portions.end())("portion_id", portion)("count", Portions.size());
        return *it->second;
    }

    std::shared_ptr<TPortionInfo> GetPortionPtr(const ui64 portion) const {
        auto it = Portions.find(portion);
        if (it == Portions.end()) {
            return nullptr;
        }
        return it->second;
    }

    bool ErasePortion(const ui64 portion);

    explicit TGranuleMeta(const TGranuleRecord& rec, std::shared_ptr<TGranulesStorage> owner, const NColumnShard::TGranuleDataCounters& counters);

    ui64 GetGranuleId() const {
        return Record.Granule;
    }
    ui64 PathId() const noexcept { return Record.PathId; }
    bool Empty() const noexcept { return Portions.empty(); }

    ui64 Size() const;
};

} // namespace NKikimr::NOlap
