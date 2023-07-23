#pragma once
#include "compaction.h"

namespace NKikimr::NOlap {

class TGranuleMeta;

class TInGranuleCompactColumnEngineChanges: public TCompactColumnEngineChanges {
private:
    using TBase = TCompactColumnEngineChanges;
    std::pair<std::shared_ptr<arrow::RecordBatch>, TSnapshot> CompactInOneGranule(ui64 granule,
        const std::vector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs,
        TConstructionContext& context) const;
    TMarksGranules MergeBorders;

    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;

protected:
    virtual TConclusion<std::vector<TString>> DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual TPortionMeta::EProduced GetResultProducedClass() const override {
        return TPortionMeta::COMPACTED;
    }
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
public:
    virtual bool IsSplit() const override {
        return false;
    }
    using TBase::TBase;

    virtual TString TypeString() const override {
        return "INTERNAL_COMPACTION";
    }
};

}
