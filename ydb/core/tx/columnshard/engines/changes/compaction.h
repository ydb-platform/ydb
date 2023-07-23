#pragma once
#include "indexation.h"
#include "compaction_info.h"
#include "mark_granules.h"

namespace NKikimr::NOlap {

class TGranuleMeta;

class TCompactColumnEngineChanges: public TChangesWithAppend {
private:
    using TBase = TChangesWithAppend;
    THashMap<TMark, ui32> TmpGranuleIds; // mark -> tmp granule id
protected:
    const TCompactionLimits Limits;
    std::unique_ptr<TCompactionInfo> CompactionInfo;
    TCompactionSrcGranule SrcGranule;

    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& /*context*/) override;
    virtual void DoDebugString(TStringOutput& out) const override;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) override;
    virtual TPortionMeta::EProduced GetResultProducedClass() const = 0;
public:
    virtual bool IsSplit() const = 0;

    const THashMap<TMark, ui32>& GetTmpGranuleIds() const {
        return TmpGranuleIds;
    }

    virtual THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GetGroupedBlobRanges() const override;

    std::vector<std::pair<TPortionInfo, ui64>> PortionsToMove; // {portion, new granule}
    std::vector<TPortionInfo> SwitchedPortions; // Portions that would be replaced by new ones

    TCompactColumnEngineChanges(const TCompactionLimits& limits, std::unique_ptr<TCompactionInfo>&& info, const TCompactionSrcGranule& srcGranule);

    ui64 SetTmpGranule(ui64 pathId, const TMark& mark);

    THashMap<ui64, ui64> TmpToNewGranules(TFinalizationContext& context, THashMap<ui64, std::pair<ui64, TMark>>& newGranules) const;

    virtual const TGranuleMeta* GetGranuleMeta() const override;

    virtual void OnChangesApplyFailed(const TString& errorMessage) override {
        CompactionInfo->CompactionFailed(errorMessage);
    }
    virtual void OnChangesApplyFinished() override {
        CompactionInfo->CompactionFinished();
    }

    ui32 NumSplitInto(const ui32 srcRows) const;

    bool IsMovedPortion(const TPortionInfo& info);
};

}
