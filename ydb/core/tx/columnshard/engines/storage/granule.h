#pragma once
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

class TDataClassSummary {
private:
    ui64 PortionsSize = 0;
    ui64 MaxColumnsSize = 0;
    ui64 PortionsCount = 0;
    friend class TGranuleMeta;
public:
    ui64 GetPortionsSize() const {
        return PortionsSize;
    }
    ui64 GetMaxColumnsSize() const {
        return MaxColumnsSize;
    }
    ui64 GetPortionsCount() const {
        return PortionsCount;
    }

    TDataClassSummary operator+(const TDataClassSummary& item) const {
        TDataClassSummary result;
        result.PortionsSize = PortionsSize + item.PortionsSize;
        result.MaxColumnsSize = MaxColumnsSize + item.MaxColumnsSize;
        result.PortionsCount = PortionsCount + item.PortionsCount;
        return result;
    }
};

class TColumnSummary {
private:
    ui32 ColumnId;
    ui64 BlobsSize;
public:
    TColumnSummary(const ui32 columnId, const ui64 blobsSize)
        : ColumnId(columnId)
        , BlobsSize(blobsSize)
    {

    }

    ui32 GetColumnId() const {
        return ColumnId;
    }

    ui64 GetBlobsSize() const {
        return BlobsSize;
    }
};

class TGranuleSummary {
private:
    TDataClassSummary Inserted;
    TDataClassSummary Other;
    friend class TGranuleMeta;
    std::vector<TColumnSummary> ColumnIdsSortedBySizeDescending;
    bool DifferentBorders = false;
public:
    const std::vector<TColumnSummary>& GetColumnIdsSortedBySizeDescending() const {
        return ColumnIdsSortedBySizeDescending;
    }
    const TDataClassSummary& GetInserted() const {
        return Inserted;
    }
    const TDataClassSummary& GetOther() const {
        return Other;
    }
    bool GetDifferentBorders() const {
        return DifferentBorders;
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
};

class TCompactionPriority: public TCompactionPriorityInfo {
private:
    using TBase = TCompactionPriorityInfo;
    TGranuleSummary GranuleSummary;
    ui64 GetWeightCorrected() const {
        if (!GranuleSummary.GetDifferentBorders()) {
            return 0;
        }
        if (GranuleSummary.GetActivePortionsCount() <= 1) {
            return 0;
        }
        const ui64 weightedSize = (1.0 * GranuleSummary.GetGranuleSize() / 1024) * GranuleSummary.GetActivePortionsCount();
        return weightedSize;
    }
public:
    TCompactionPriority(const TCompactionPriorityInfo& data, const TGranuleSummary& granuleSummary)
        : TBase(data)
        , GranuleSummary(granuleSummary)
    {

    }
    bool operator<(const TCompactionPriority& item) const {
        return std::tuple(GetWeightCorrected(), GranuleSummary.GetActivePortionsCount(), item.NextAttemptInstant)
            < std::tuple(item.GetWeightCorrected(), item.GranuleSummary.GetActivePortionsCount(), NextAttemptInstant);
    }

};

class TGranuleMeta: public ICompactionObjectCallback, TNonCopyable {
private:
    THashMap<ui64, TPortionInfo> Portions; // portion -> portionInfo
    mutable std::optional<TGranuleSummary> SummaryCache;
    bool NeedSplit(const TCompactionLimits& limits, bool& inserted) const;
    void RebuildMetrics() const;

    void ResetCaches() {
        SummaryCache = {};
    }

    enum class EActivity {
        SplitCompaction,
        InternalCompaction,
    };

    std::set<EActivity> Activity;
    TCompactionPriorityInfo CompactionPriorityInfo;
    mutable bool AllowInsertionFlag = false;
    std::shared_ptr<TGranulesStorage> Owner;

    void OnAfterChangePortion(const ui64 portion);
public:
    const TGranuleSummary& GetSummary() const {
        if (!SummaryCache) {
            RebuildMetrics();
        }
        return *SummaryCache;
    }
    TCompactionPriority GetCompactionPriority() const {
        return TCompactionPriority(CompactionPriorityInfo, GetSummary());
    }

    bool NeedCompaction(const TCompactionLimits& limits) const {
        if (InCompaction() || Empty()) {
            return false;
        }
        return NeedSplitCompaction(limits) || NeedInternalCompaction(limits);
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
        return TStringBuilder() << "granule:" << GetGranuleId() << ";path_id:" << Record.PathId << ";";
    }

    const TGranuleRecord Record;

    void AddColumnRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec);

    const THashMap<ui64, TPortionInfo>& GetPortions() const {
        return Portions;
    }

    const TPortionInfo& GetPortionVerified(const ui64 portion) const {
        auto it = Portions.find(portion);
        Y_VERIFY(it != Portions.end());
        return it->second;
    }

    bool ErasePortion(const ui64 portion);

    explicit TGranuleMeta(const TGranuleRecord& rec, std::shared_ptr<TGranulesStorage> owner)
        : Owner(owner)
        , Record(rec) {
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
    bool NeedSplitCompaction(const TCompactionLimits& limits) const {
        bool inserted = false;
        return NeedSplit(limits, inserted);
    }
    bool NeedInternalCompaction(const TCompactionLimits& limits) const {
        bool inserted = false;
        return !NeedSplit(limits, inserted) && inserted;
    }
};

} // namespace NKikimr::NOlap
