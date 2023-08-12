#pragma once
#include "compaction.h"

namespace NKikimr::NOlap {

class TSplitCompactColumnEngineChanges: public TCompactColumnEngineChanges {
private:
    using TBase = TCompactColumnEngineChanges;
    std::pair<std::vector<std::shared_ptr<arrow::RecordBatch>>, TSnapshot> PortionsToBatches(const std::vector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs,
            const bool insertedOnly, TConstructionContext& context);

    std::vector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>> SliceGranuleBatches(const TIndexInfo& indexInfo,
            const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
            const TMark& ts0);

    ui64 TryMovePortions(const TMark& ts0,
        std::vector<TPortionInfo>& portions,
        std::vector<std::pair<TMark, ui64>>& tsIds,
        std::vector<std::pair<TPortionInfo, ui64>>& toMove, TConstructionContext& context);

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
        return true;
    }
    using TBase::TBase;

    virtual TString TypeString() const override {
        return "SPLIT_COMPACTION";
    }
};

}
