#pragma once
#include "compaction.h"
#include <ydb/core/formats/arrow/reader/read_filter_merger.h>

namespace NKikimr::NOlap::NCompaction {

class TGeneralCompactColumnEngineChanges: public TCompactColumnEngineChanges {
private:
    using TBase = TCompactColumnEngineChanges;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    std::map<NIndexedReader::TSortableBatchPosition, bool> CheckPoints;
    void BuildAppendedPortionsByFullBatches(TConstructionContext& context) noexcept;
    void BuildAppendedPortionsByChunks(TConstructionContext& context) noexcept;
protected:
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
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

    class IMemoryPredictor {
    public:
        virtual ui64 AddPortion(const TPortionInfo& portionInfo) = 0;
        virtual ~IMemoryPredictor() = default;
    };

    class TMemoryPredictorSimplePolicy: public IMemoryPredictor {
    private:
        ui64 SumMemory = 0;
    public:
        virtual ui64 AddPortion(const TPortionInfo& portionInfo) override {
            for (auto&& i : portionInfo.GetRecords()) {
                SumMemory += i.BlobRange.Size;
                SumMemory += 2 * i.GetMeta().GetRawBytesVerified();
            }
            return SumMemory;
        }
    };

    class TMemoryPredictorChunkedPolicy: public IMemoryPredictor {
    private:
        ui64 SumMemory = 0;
        ui32 PortionsCount = 0;
        THashMap<ui32, ui64> MaxMemoryByColumnChunk;
    public:
        virtual ui64 AddPortion(const TPortionInfo& portionInfo) override {
            SumMemory += portionInfo.GetRecordsCount() * (2 * sizeof(ui64) + sizeof(ui32) + sizeof(ui16));
            for (auto&& i : portionInfo.GetRecords()) {
                SumMemory += i.BlobRange.Size;
                auto it = MaxMemoryByColumnChunk.find(i.GetColumnId());
                ++PortionsCount;
                if (it == MaxMemoryByColumnChunk.end()) {
                    it = MaxMemoryByColumnChunk.emplace(i.GetColumnId(), i.GetMeta().GetRawBytesVerified()).first;
                    SumMemory += it->second * PortionsCount;
                } else if (it->second < i.GetMeta().GetRawBytesVerified()) {
                    SumMemory -= it->second * (PortionsCount - 1);
                    it->second = i.GetMeta().GetRawBytesVerified();
                    SumMemory += it->second * PortionsCount;
                }
            }
            return SumMemory;
        }
    };

    static std::shared_ptr<IMemoryPredictor> BuildMemoryPredictor();

    void AddCheckPoint(const NIndexedReader::TSortableBatchPosition& position, const bool include = true, const bool validationDuplications = true);

    virtual TString TypeString() const override {
        return StaticTypeName();
    }
};

}
