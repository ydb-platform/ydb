#pragma once
#include "abstract/abstract.h"
#include <util/generic/hash.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

namespace NKikimr::NOlap {

class TChangesWithAppend: public TColumnEngineChanges {
private:
    using TBase = TColumnEngineChanges;
    THashMap<TPortionAddress, TPortionInfo> PortionsToRemove;
protected:
    TSaverContext SaverContext;
    virtual void DoCompile(TFinalizationContext& context) override;
    virtual void DoOnAfterCompile() override;
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) override;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;

    virtual void DoDebugString(TStringOutput& out) const override {
        out << "remove=" << PortionsToRemove.size() << ";append=" << AppendedPortions.size() << ";";
    }

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const = 0;

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const override final {
        auto actLock = DoBuildDataLockImpl();
        if (actLock) {
            auto selfLock = std::make_shared<NDataLocks::TListPortionsLock>(TypeString() + "::" + GetTaskIdentifier() + "::REMOVE", PortionsToRemove);
            return std::make_shared<NDataLocks::TCompositeLock>(TypeString() + "::" + GetTaskIdentifier(), std::vector<std::shared_ptr<NDataLocks::ILock>>({actLock, selfLock}));
        } else {
            auto selfLock = std::make_shared<NDataLocks::TListPortionsLock>(TypeString() + "::" + GetTaskIdentifier(), PortionsToRemove);
            return selfLock;
        }
    }
public:
    TChangesWithAppend(const TSaverContext& saverContext, const NBlobOperations::EConsumer consumerId)
        : TBase(saverContext.GetStoragesManager(), consumerId)
        , SaverContext(saverContext)
    {

    }

    const THashMap<TPortionAddress, TPortionInfo>& GetPortionsToRemove() const {
        return PortionsToRemove;
    }

    ui32 GetPortionsToRemoveSize() const {
        return PortionsToRemove.size();
    }

    bool HasPortionsToRemove() const {
        return PortionsToRemove.size();
    }

    void AddPortionToRemove(const TPortionInfo& info) {
        AFL_VERIFY(!info.HasRemoveSnapshot());
        AFL_VERIFY(PortionsToRemove.emplace(info.GetAddress(), info).second);
    }

    std::vector<TWritePortionInfoWithBlobsResult> AppendedPortions;
    virtual ui32 GetWritePortionsCount() const override {
        return AppendedPortions.size();
    }
    virtual TWritePortionInfoWithBlobsResult* GetWritePortionInfo(const ui32 index) override {
        Y_ABORT_UNLESS(index < AppendedPortions.size());
        return &AppendedPortions[index];
    }
    virtual bool NeedWritePortion(const ui32 /*index*/) const override {
        return true;
    }
};

}
