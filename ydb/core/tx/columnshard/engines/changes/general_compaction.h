#pragma once
#include "compaction.h"

namespace NKikimr::NOlap {

class TGeneralCompactColumnEngineChanges: public TCompactColumnEngineChanges {
private:
    using TBase = TCompactColumnEngineChanges;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
protected:
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual TPortionMeta::EProduced GetResultProducedClass() const override {
        return TPortionMeta::EProduced::SPLIT_COMPACTED;
    }
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
public:
    virtual bool IsSplit() const override {
        return false;
    }
    using TBase::TBase;

    virtual TString TypeString() const override {
        return "GENERAL_COMPACTION";
    }
};

}
