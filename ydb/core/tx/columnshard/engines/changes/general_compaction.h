#pragma once
#include "compaction.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/priorities/usage/events.h>

namespace NKikimr::NOlap::NCompaction {

class TPortionToMerge;

class TGeneralCompactColumnEngineChanges: public TCompactColumnEngineChanges,
                                          public NColumnShard::TMonitoringObjectsCounter<TGeneralCompactColumnEngineChanges> {
private:
    YDB_ACCESSOR(ui64, PortionExpectedSize, 1.5 * (1 << 20));
    using TBase = TCompactColumnEngineChanges;
    std::shared_ptr<NPrioritiesQueue::TAllocationGuard> PrioritiesAllocationGuard;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;
    NArrow::NMerger::TIntervalPositions CheckPoints;

    [[nodiscard]] std::vector<TWritePortionInfoWithBlobsResult> BuildAppendedPortionsByChunks(TConstructionContext& context,
        std::vector<TPortionToMerge>&& portionsToMerge,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats) noexcept;

    std::shared_ptr<NArrow::TColumnFilter> BuildPortionFilter(const std::optional<NKikimr::NOlap::TGranuleShardingInfo>& shardingActual,
        const std::shared_ptr<NArrow::TGeneralContainer>& batch, const TPortionInfo& pInfo, const THashSet<ui64>& portionsInUsage,
        const ISnapshotSchema::TPtr& resultSchema) const;

protected:
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;

    virtual bool NeedDiskWriteLimiter() const override {
        return true;
    }

    virtual NPortion::EProduced GetResultProducedClass() const override {
        return NPortion::EProduced::SPLIT_COMPACTED;
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
    void SetQueueGuard(const std::shared_ptr<NPrioritiesQueue::TAllocationGuard>& g) {
        PrioritiesAllocationGuard = g;
    }
    using TBase::TBase;

    class TMemoryPredictorChunkedPolicy: public IMemoryPredictor {
    private:
        ui64 SumMemoryFix = 0;
        ui64 SumMemoryRaw = 0;
    public:
        virtual ui64 AddPortion(const TPortionInfo::TConstPtr& portionInfo) override;
    };

    static std::shared_ptr<IMemoryPredictor> BuildMemoryPredictor();

    void AddCheckPoint(const NArrow::NMerger::TSortableBatchPosition& position, const bool include);
    void SetCheckPoints(NArrow::NMerger::TIntervalPositions&& positions) {
        CheckPoints = std::move(positions);
    }

    virtual TString TypeString() const override {
        return StaticTypeName();
    }
};

}   // namespace NKikimr::NOlap::NCompaction
