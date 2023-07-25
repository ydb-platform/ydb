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
public:
    void AddPortion(const TPortionInfo& info) {
        const auto sizes = info.BlobsSizes();
        PortionsSize += sizes.first;
        MaxColumnsSize += sizes.second;
        RecordsCount += info.NumRows();
        ++PortionsCount;
    }

    void RemovePortion(const TPortionInfo& info) {
        const auto sizes = info.BlobsSizes();
        PortionsSize -= sizes.first;
        Y_VERIFY(PortionsSize >= 0);
        MaxColumnsSize -= sizes.second;
        Y_VERIFY(MaxColumnsSize >= 0);
        RecordsCount -= info.NumRows();
        Y_VERIFY(RecordsCount >= 0);
        --PortionsCount;
        Y_VERIFY(PortionsCount >= 0);
    }

    TDataClassSummary operator+(const TDataClassSummary& item) const {
        TDataClassSummary result;
        result.PortionsSize = PortionsSize + item.PortionsSize;
        result.MaxColumnsSize = MaxColumnsSize + item.MaxColumnsSize;
        result.PortionsCount = PortionsCount + item.PortionsCount;
        result.RecordsCount = RecordsCount + item.RecordsCount;
        return result;
    }
};

class TColumnSummary {
private:
    ui32 ColumnId;
    ui64 PackedBlobsSize = 0;
    ui64 PackedRecordsCount = 0;
    ui64 InsertedBlobsSize = 0;
    ui64 InsertedRecordsCount = 0;
public:
    void AddData(const bool isInserted, const ui64 bytes, const ui64 records) {
        if (isInserted) {
            InsertedRecordsCount += records;
            InsertedBlobsSize += bytes;
        } else {
            PackedRecordsCount += records;
            PackedBlobsSize += bytes;
        }
    }

    TColumnSummary(const ui32 columnId)
        : ColumnId(columnId)
    {

    }

    ui32 GetColumnId() const {
        return ColumnId;
    }

    ui32 GetRecordsCount() const {
        return PackedRecordsCount + InsertedRecordsCount;
    }

    ui64 GetBlobsSize() const {
        return PackedBlobsSize + InsertedBlobsSize;
    }

    ui32 GetPackedRecordsCount() const {
        return PackedRecordsCount;
    }

    ui64 GetPackedBlobsSize() const {
        return PackedBlobsSize;
    }
};

class TGranuleHardSummary {
private:
    std::vector<TColumnSummary> ColumnIdsSortedBySizeDescending;
    bool DifferentBorders = false;
    friend class TGranuleMeta;
public:
    bool GetDifferentBorders() const {
        return DifferentBorders;
    }
    const std::vector<TColumnSummary>& GetColumnIdsSortedBySizeDescending() const {
        return ColumnIdsSortedBySizeDescending;
    }
};

class TGranuleAdditiveSummary {
private:
    TDataClassSummary Inserted;
    TDataClassSummary Other;
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
        if (GetMaxColumnsSize() >= limits.GranuleBlobSplitSize ||
            GetGranuleSize() >= limits.GranuleSizeForOverloadPrevent)
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
    const TDataClassSummary& GetOther() const {
        return Other;
    }
    ui64 GetGranuleSize() const {
        return (Inserted + Other).GetPortionsSize();
    }
    ui64 GetActivePortionsCount() const {
        return (Inserted + Other).GetPortionsCount();
    }
    ui64 GetMaxColumnsSize() const {
        return (Inserted + Other).GetMaxColumnsSize();
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
            Counters.OnCompactedData(Owner.GetOther());
            Counters.OnInsertedData(Owner.GetInserted());
            Counters.OnFullData(Owner.GetOther() + Owner.GetInserted());
        }

        void AddPortion(const TPortionInfo& info) {
            if (info.IsInserted()) {
                Owner.Inserted.AddPortion(info);
            } else {
                Owner.Other.AddPortion(info);
            }
        }
        void RemovePortion(const TPortionInfo& info) {
            if (info.IsInserted()) {
                Owner.Inserted.RemovePortion(info);
            } else {
                Owner.Other.RemovePortion(info);
            }
        }
    };

    TEditGuard StartEdit(const NColumnShard::TGranuleDataCounters& counters) {
        return TEditGuard(counters, *this);
    }

    TString DebugString() const {
        return TStringBuilder() << "inserted:(" << Inserted.DebugString() << ");other:(" << Other.DebugString() << "); ";
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

class TGranuleMeta: public ICompactionObjectCallback, TNonCopyable {
public:
    enum class EActivity {
        SplitCompaction,
        InternalCompaction,
    };

private:
    TMonotonic ModificationLastTime = TMonotonic::Now();
    THashMap<ui64, TPortionInfo> Portions; // portion -> portionInfo
    mutable std::optional<TGranuleAdditiveSummary> AdditiveSummaryCache;
    mutable std::optional<TGranuleHardSummary> HardSummaryCache;

    void RebuildHardMetrics() const;
    void RebuildAdditiveMetrics() const;

    std::set<EActivity> Activity;
    TCompactionPriorityInfo CompactionPriorityInfo;
    mutable bool AllowInsertionFlag = false;
    std::shared_ptr<TGranulesStorage> Owner;
    const NColumnShard::TGranuleDataCounters Counters;

    void OnBeforeChangePortion(const TPortionInfo* portionBefore, const TPortionInfo* portionAfter);
    void OnAfterChangePortion();
    void OnAdditiveSummaryChange() const;
public:
    TGranuleAdditiveSummary::ECompactionClass GetCompactionType(const TCompactionLimits& limits) const;
    const TGranuleHardSummary& GetHardSummary() const {
        if (!HardSummaryCache) {
            RebuildHardMetrics();
        }
        return *HardSummaryCache;
    }
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

    virtual void OnCompactionStarted(const bool inGranule) override;

    virtual void OnCompactionCanceled(const TString& reason) override;
    virtual void OnCompactionFailed(const TString& reason) override;
    virtual void OnCompactionFinished() override;

    void UpsertPortion(const TPortionInfo& info);

    virtual TString DebugString() const override {
        return TStringBuilder() << "granule:" << GetGranuleId() << ";"
            << "path_id:" << Record.PathId << ";"
            << "size:" << GetAdditiveSummary().GetGranuleSize() << ";"
            << "portions_count:" << Portions.size() << ";"
            ;
    }

    const TGranuleRecord Record;

    void AddColumnRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec);

    const THashMap<ui64, TPortionInfo>& GetPortions() const {
        return Portions;
    }

    ui64 GetPathId() const {
        return Record.PathId;
    }

    const TPortionInfo& GetPortionVerified(const ui64 portion) const {
        auto it = Portions.find(portion);
        Y_VERIFY(it != Portions.end());
        return it->second;
    }

    const TPortionInfo* GetPortionPointer(const ui64 portion) const {
        auto it = Portions.find(portion);
        if (it == Portions.end()) {
            return nullptr;
        }
        return &it->second;
    }

    bool ErasePortion(const ui64 portion);

    explicit TGranuleMeta(const TGranuleRecord& rec, std::shared_ptr<TGranulesStorage> owner, const NColumnShard::TGranuleDataCounters& counters)
        : Owner(owner)
        , Counters(counters)
        , Record(rec)
    {
    }

    ui64 GetGranuleId() const {
        return Record.Granule;
    }
    ui64 PathId() const noexcept { return Record.PathId; }
    bool Empty() const noexcept { return Portions.empty(); }

    ui64 Size() const;
    bool IsOverloaded(const TCompactionLimits& limits) const {
        return Size() >= limits.GranuleOverloadSize;
    }
};

} // namespace NKikimr::NOlap
