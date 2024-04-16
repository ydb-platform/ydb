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
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard& self, TWriteIndexContext& context) override;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;
    std::vector<TPortionInfoWithBlobs> MakeAppendedPortions(const std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule,
        const TSnapshot& snapshot, const TGranuleMeta* granuleMeta, TConstructionContext& context) const;

    virtual void DoDebugString(TStringOutput& out) const override {
        out << "remove=" << PortionsToRemove.size() << ";append=" << AppendedPortions.size() << ";";
    }

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const = 0;

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const override final {
        auto actLock = DoBuildDataLockImpl();
        auto selfLock = std::make_shared<NDataLocks::TListPortionsLock>(PortionsToRemove);
        return std::make_shared<NDataLocks::TCompositeLock>(std::vector<std::shared_ptr<NDataLocks::ILock>>({actLock, selfLock}));
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
