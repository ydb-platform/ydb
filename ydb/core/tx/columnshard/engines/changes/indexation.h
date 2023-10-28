#pragma once
#include "abstract/abstract.h"
#include "with_appended.h"
#include <ydb/core/tx/columnshard/engines/insert_table/data.h>
#include <ydb/core/formats/arrow/reader/read_filter_merger.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TInsertColumnEngineChanges: public TChangesWithAppend {
private:
    using TBase = TChangesWithAppend;
    std::shared_ptr<arrow::RecordBatch> AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
        const TIndexInfo& indexInfo, const TInsertedData& inserted) const;
    std::vector<NOlap::TInsertedData> DataToIndex;
protected:
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
    virtual ui64 DoCalcMemoryForUsage() const override {
        ui64 result = 0;
        for (auto& ptr : DataToIndex) {
            result += ptr.GetMeta().GetRawBytes();
        }
        return result;
    }
public:
    const TMark DefaultMark;
    THashMap<ui64, std::vector<NIndexedReader::TSortableBatchPosition>> PathToGranule; // pathId -> positions (sorted by pk)
public:
    TInsertColumnEngineChanges(const TMark& defaultMark, std::vector<NOlap::TInsertedData>&& dataToIndex, const TSplitSettings& splitSettings, const TSaverContext& saverContext)
        : TBase(splitSettings, saverContext, StaticTypeName())
        , DataToIndex(std::move(dataToIndex))
        , DefaultMark(defaultMark)
    {
    }

    const std::vector<NOlap::TInsertedData>& GetDataToIndex() const {
        return DataToIndex;
    }

    virtual THashSet<TPortionAddress> GetTouchedPortions() const override {
        return TBase::GetTouchedPortions();
    }

    static TString StaticTypeName() {
        return "CS::INDEXATION";
    }

    virtual TString TypeString() const override {
        return StaticTypeName();
    }
    std::optional<ui64> AddPathIfNotExists(ui64 pathId);

};

}
