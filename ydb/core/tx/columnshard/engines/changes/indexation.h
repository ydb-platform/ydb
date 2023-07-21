#pragma once
#include "abstract.h"
#include "with_appended.h"
#include <ydb/core/tx/columnshard/engines/insert_table/data.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TInsertColumnEngineChanges: public TChangesWithAppend {
private:
    using TBase = TChangesWithAppend;
protected:
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) override;
    virtual void DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
public:
    const TMark DefaultMark;
    THashMap<ui64, std::vector<std::pair<TMark, ui64>>> PathToGranule; // pathId -> {mark, granule}
    std::vector<NOlap::TInsertedData> DataToIndex;
    ui32 ReservedGranuleIds{ 0 };
    THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> CachedBlobs;
public:
    TInsertColumnEngineChanges(const TMark& defaultMark)
        : DefaultMark(defaultMark)
    {

    }
    virtual THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GetGroupedBlobRanges() const override;
    virtual TString TypeString() const override {
        return "insert";
    }
    bool AddPathIfNotExists(ui64 pathId);

};

}
