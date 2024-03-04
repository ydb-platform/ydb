#pragma once
#include "compaction.h"
#include <ydb/core/formats/arrow/reader/read_filter_merger.h>

namespace NKikimr::NOlap::NCompaction {

class TGeneralCompactColumnEngineChanges: public TCompactColumnEngineChanges {
private:
    using TBase = TCompactColumnEngineChanges;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    void BuildAppendedPortionsByFullBatches(TConstructionContext& context) noexcept;
    void BuildAppendedPortionsByChunks(TConstructionContext& context) noexcept;
    std::map<NIndexedReader::TSortableBatchPosition, bool> CheckPoints;
protected:
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual TPortionMeta::EProduced GetResultProducedClass() const override {
        return TPortionMeta::EProduced::SPLIT_COMPACTED;
    }
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
    virtual ui64 DoCalcMemoryForUsage() const override {
        ui64 result = 0;
        for (auto& p : SwitchedPortions) {
            result += 2 * p.GetBlobBytes();
        }
        return result;
    }
public:
    using TBase::TBase;

    void AddCheckPoint(const NIndexedReader::TSortableBatchPosition& position, const bool include = true, const bool validationDuplications = true);

    virtual TString TypeString() const override {
        return StaticTypeName();
    }
};

}
