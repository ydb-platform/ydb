#pragma once
#include "abstract/abstract.h"
#include "with_appended.h"
#include <ydb/core/tx/columnshard/engines/insert_table/data.h>
#include <util/generic/hash.h>
#include <ydb/core/formats/arrow/reader/position.h>

namespace NKikimr::NOlap {

class TInsertColumnEngineChanges: public TChangesWithAppend {
private:
    using TBase = TChangesWithAppend;
    std::vector<NOlap::TInsertedData> DataToIndex;
protected:
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) override;

    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual TConclusionStatus DoConstructBlobs(TConstructionContext& context) noexcept override;
    virtual NColumnShard::ECumulativeCounters GetCounterIndex(const bool isSuccess) const override;
    virtual ui64 DoCalcMemoryForUsage() const override {
        ui64 result = 0;
        for (auto& ptr : DataToIndex) {
            result += ptr.GetMeta().GetRawBytes();
        }
        return result;
    }

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const override {
        return nullptr;
    }

public:
    THashMap<ui64, NArrow::NMerger::TIntervalPositions> PathToGranule;   // pathId -> positions (sorted by pk)
public:
    TInsertColumnEngineChanges(std::vector<NOlap::TInsertedData>&& dataToIndex, const TSaverContext& saverContext)
        : TBase(saverContext, NBlobOperations::EConsumer::INDEXATION)
        , DataToIndex(std::move(dataToIndex))
    {
    }

    const std::vector<NOlap::TInsertedData>& GetDataToIndex() const {
        return DataToIndex;
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
