#pragma once
#include "abstract/abstract.h"
#include <util/generic/hash.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

namespace NKikimr::NOlap {

class TChangesWithAppend: public TColumnEngineChanges {
private:
    using TBase = TColumnEngineChanges;

protected:
    TSplitSettings SplitSettings;
    TSaverContext SaverContext;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual bool DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) override;
    virtual void DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual void DoWriteIndexComplete(NColumnShard::TColumnShard& /*self*/, TWriteIndexCompleteContext& /*context*/) override {

    }
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    std::vector<TPortionInfoWithBlobs> MakeAppendedPortions(const std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule,
        const TSnapshot& snapshot, const TGranuleMeta* granuleMeta, TConstructionContext& context) const;

    virtual void DoDebugString(TStringOutput& out) const override {
        out << "remove=" << PortionsToRemove.size() << ";append=" << AppendedPortions.size() << ";";
    }

public:
    const TSplitSettings& GetSplitSettings() const {
        return SplitSettings;
    }

    TChangesWithAppend(const TSplitSettings& splitSettings, const TSaverContext& saverContext, const TString& consumerId)
        : TBase(saverContext.GetStoragesManager(), consumerId)
        , SplitSettings(splitSettings)
        , SaverContext(saverContext)
    {

    }

    virtual THashSet<TPortionAddress> GetTouchedPortions() const override {
        THashSet<TPortionAddress> result;
        for (auto&& i : PortionsToRemove) {
            result.emplace(i.first);
        }
        return result;
    }

    THashMap<TPortionAddress, TPortionInfo> PortionsToRemove;
    std::vector<TPortionInfoWithBlobs> AppendedPortions;
    virtual ui32 GetWritePortionsCount() const override {
        return AppendedPortions.size();
    }
    virtual TPortionInfoWithBlobs* GetWritePortionInfo(const ui32 index) override {
        Y_ABORT_UNLESS(index < AppendedPortions.size());
        return &AppendedPortions[index];
    }
    virtual bool NeedWritePortion(const ui32 /*index*/) const override {
        return true;
    }
};

}
