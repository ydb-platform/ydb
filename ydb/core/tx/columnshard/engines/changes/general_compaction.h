#pragma once
#include "compaction.h"
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>

namespace NKikimr::NOlap::NCompaction {

class TGeneralCompactColumnEngineChanges: public TCompactColumnEngineChanges {
private:
    using TBase = TCompactColumnEngineChanges;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;
    NArrow::NMerger::TIntervalPositions CheckPoints;
    void BuildAppendedPortionsByChunks(TConstructionContext& context, std::vector<TReadPortionInfoWithBlobs>&& portions) noexcept;

    std::shared_ptr<NArrow::TColumnFilter> BuildPortionFilter(const std::optional<NKikimr::NOlap::TGranuleShardingInfo>& shardingActual,
        const std::shared_ptr<NArrow::TGeneralContainer>& batch, const TPortionInfo& pInfo, const THashSet<ui64>& portionsInUsage,
        const ISnapshotSchema::TPtr& resultSchema) const;
protected:
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;

    virtual bool NeedDiskWriteLimiter() const override {
        return true;
    }

    virtual TPortionMeta::EProduced GetResultProducedClass() const override {
        return TPortionMeta::EProduced::SPLIT_COMPACTED;
    }
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
    virtual ui64 DoCalcMemoryForUsage() const override {
        auto predictor = BuildMemoryPredictor();
        ui64 result = 0;
        for (auto& p : SwitchedPortions) {
            result = predictor->AddPortion(p);
        }
        return result;
    }
public:
    using TBase::TBase;

    class TMemoryPredictorSimplePolicy: public IMemoryPredictor {
    private:
        ui64 SumMemory = 0;
    public:
        virtual ui64 AddPortion(const TPortionInfo& portionInfo) override {
            for (auto&& i : portionInfo.GetRecords()) {
                SumMemory += i.BlobRange.Size;
                SumMemory += 2 * i.GetMeta().GetRawBytes();
            }
            return SumMemory;
        }
    };

    class TMemoryPredictorChunkedPolicy: public IMemoryPredictor {
    private:
        ui64 SumMemoryDelta = 0;
        ui64 SumMemoryFix = 0;
        ui32 PortionsCount = 0;
        THashMap<ui32, ui64> MaxMemoryByColumnChunk;
    public:
        virtual ui64 AddPortion(const TPortionInfo& portionInfo) override;
    };

    static std::shared_ptr<IMemoryPredictor> BuildMemoryPredictor();

    void AddCheckPoint(const NArrow::NMerger::TSortableBatchPosition& position, const bool include);

    virtual TString TypeString() const override {
        return StaticTypeName();
    }
};

}
