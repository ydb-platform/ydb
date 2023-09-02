#pragma once
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>

namespace NKikimr::NOlap {

class TGranulesStorage;

class TCompactionPriorityInfo {
protected:
    ui32 ProblemSequenceLength = 0;
    TInstant NextAttemptInstant;
public:
    TInstant GetNextAttemptInstant() const {
        return NextAttemptInstant;
    }

    void OnCompactionFailed() {
        ++ProblemSequenceLength;
        NextAttemptInstant = TInstant::Now() + TDuration::Seconds(1);
    }

    void OnCompactionFinished() {
        ProblemSequenceLength = 0;
        NextAttemptInstant = TInstant::Zero();
    }

    void OnCompactionCanceled() {
        NextAttemptInstant = TInstant::Now() + TDuration::Seconds(1);
    }
};

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

class TCompactionPriority: public TCompactionPriorityInfo {
public:
    TGranuleAdditiveSummary::ECompactionClass GetPriorityClass() const {
        return GranuleSummary.GetCompactionClass(TCompactionLimits(), LastModification, ConstructionInstant);
    }

private:
    using TBase = TCompactionPriorityInfo;
    TGranuleAdditiveSummary GranuleSummary;
    TMonotonic LastModification;
    TMonotonic ConstructionInstant = TMonotonic::Now();

    ui64 GetWeightCorrected() const {
        switch (GetPriorityClass()) {
            case TGranuleAdditiveSummary::ECompactionClass::Split:
                return GranuleSummary.GetGranuleSize();
            case TGranuleAdditiveSummary::ECompactionClass::Internal:
            case TGranuleAdditiveSummary::ECompactionClass::WaitInternal:
                return GranuleSummary.GetInserted().GetPortionsSize();
            case TGranuleAdditiveSummary::ECompactionClass::NoCompaction:
                return GranuleSummary.GetGranuleSize() * GranuleSummary.GetActivePortionsCount() * GranuleSummary.GetActivePortionsCount();
        }
    }
public:
    TCompactionPriority(const TCompactionPriorityInfo& data, const TGranuleAdditiveSummary& granuleSummary, const TMonotonic lastModification)
        : TBase(data)
        , GranuleSummary(granuleSummary)
        , LastModification(lastModification)
    {

    }
    bool operator<(const TCompactionPriority& item) const {
        return std::tuple((ui32)GetPriorityClass(), GetWeightCorrected(), GranuleSummary.GetActivePortionsCount(), item.NextAttemptInstant)
            < std::tuple((ui32)item.GetPriorityClass(), item.GetWeightCorrected(), item.GranuleSummary.GetActivePortionsCount(), NextAttemptInstant);
    }

    TString DebugString() const {
        return TStringBuilder() << "summary:(" << GranuleSummary.DebugString() << ");";
    }

};

class TGranuleMeta: TNonCopyable {
public:
    enum class EActivity {
        SplitCompaction,
        InternalCompaction,
    };

private:
    TMonotonic ModificationLastTime = TMonotonic::Now();
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    mutable std::optional<TGranuleAdditiveSummary> AdditiveSummaryCache;

    void RebuildHardMetrics() const;
    void RebuildAdditiveMetrics() const;

    std::set<EActivity> Activity;
    TCompactionPriorityInfo CompactionPriorityInfo;
    mutable bool AllowInsertionFlag = false;
    std::shared_ptr<TGranulesStorage> Owner;
    const NColumnShard::TGranuleDataCounters Counters;
    NColumnShard::TEngineLogsCounters::TPortionsInfoGuard PortionInfoGuard;

    void OnBeforeChangePortion(const std::shared_ptr<TPortionInfo> portionBefore);
    void OnAfterChangePortion(const std::shared_ptr<TPortionInfo> portionAfter);
    void OnAdditiveSummaryChange() const;
public:
    std::vector<std::shared_ptr<TPortionInfo>> GroupOrderedPortionsByPK(const TSnapshot& snapshot) const {
        std::vector<std::shared_ptr<TPortionInfo>> portions;
        for (auto&& i : Portions) {
            if (i.second->IsVisible(snapshot)) {
                portions.emplace_back(i.second);
            }
        }
        const auto pred = [](const std::shared_ptr<TPortionInfo>& l, const std::shared_ptr<TPortionInfo>& r) {
            return l->IndexKeyStart() < r->IndexKeyStart();
        };
        std::sort(portions.begin(), portions.end(), pred);
        return portions;
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
        return TCompactionPriority(CompactionPriorityInfo, GetAdditiveSummary(), ModificationLastTime);
    }

    bool NeedCompaction(const TCompactionLimits& limits) const {
        if (InCompaction() || Empty()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "granule_skipped_by_state")("granule_id", GetGranuleId())("granule_size", Size());
            return false;
        }
        return GetCompactionType(limits) != TGranuleAdditiveSummary::ECompactionClass::NoCompaction;
    }

    bool InCompaction() const {
        return Activity.contains(EActivity::SplitCompaction) || Activity.contains(EActivity::InternalCompaction);
    }

    void AllowedInsertion() const {
        if (InCompaction()) {
            AllowInsertionFlag = true;
        }
    }

    bool IsInsertAllowed() const {
        return AllowInsertionFlag || !Activity.contains(EActivity::SplitCompaction);
    }

    bool IsErasable() const {
        return Activity.empty();
    }

    void OnCompactionStarted(const bool inGranule);

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
        Y_VERIFY(it != Portions.end());
        return *it->second;
    }

    const TPortionInfo* GetPortionPointer(const ui64 portion) const {
        auto it = Portions.find(portion);
        if (it == Portions.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    bool ErasePortion(const ui64 portion);

    explicit TGranuleMeta(const TGranuleRecord& rec, std::shared_ptr<TGranulesStorage> owner, const NColumnShard::TGranuleDataCounters& counters);

    ui64 GetGranuleId() const {
        return Record.Granule;
    }
    ui64 PathId() const noexcept { return Record.PathId; }
    bool Empty() const noexcept { return Portions.empty(); }

    ui64 Size() const;
    bool IsOverloaded(const TCompactionLimits& limits) const {
        return (i64)Size() >= limits.GranuleOverloadSize;
    }
};

} // namespace NKikimr::NOlap
