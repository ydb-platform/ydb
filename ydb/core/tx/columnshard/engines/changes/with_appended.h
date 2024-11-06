#pragma once
#include "abstract/abstract.h"
#include <util/generic/hash.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

namespace NKikimr::NOlap {

class TChangesWithAppend: public TColumnEngineChanges {
private:
    using TBase = TColumnEngineChanges;
    THashMap<TPortionAddress, std::shared_ptr<const TPortionInfo>> PortionsToRemove;
    THashMap<TPortionAddress, std::shared_ptr<const TPortionInfo>> PortionsToMove;

protected:
    std::optional<ui64> TargetCompactionLevel;
    TSaverContext SaverContext;
    virtual void OnDataAccessorsInitialized(const TDataAccessorsInitializationContext& /*context*/) override {

    }

    virtual void DoCompile(TFinalizationContext& context) override;
    virtual void DoOnAfterCompile() override;
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) override;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;

    virtual void DoDebugString(TStringOutput& out) const override {
        out << "remove=" << PortionsToRemove.size() << ";append=" << AppendedPortions.size() << ";move=" << PortionsToMove.size();
    }

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const = 0;

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const override final {
        auto actLock = DoBuildDataLockImpl();
        THashSet<TPortionAddress> portions;
        for (auto&& i : PortionsToRemove) {
            AFL_VERIFY(portions.emplace(i.first).second);
        }
        for (auto&& i : PortionsToMove) {
            AFL_VERIFY(portions.emplace(i.first).second);
        }
        if (actLock) {
            auto selfLock = std::make_shared<NDataLocks::TListPortionsLock>(TypeString() + "::" + GetTaskIdentifier() + "::REMOVE/MOVE", portions);
            return std::make_shared<NDataLocks::TCompositeLock>(TypeString() + "::" + GetTaskIdentifier(), std::vector<std::shared_ptr<NDataLocks::ILock>>({actLock, selfLock}));
        } else {
            auto selfLock = std::make_shared<NDataLocks::TListPortionsLock>(TypeString() + "::" + GetTaskIdentifier(), portions);
            return selfLock;
        }
    }
public:
    TChangesWithAppend(const TSaverContext& saverContext, const NBlobOperations::EConsumer consumerId)
        : TBase(saverContext.GetStoragesManager(), consumerId)
        , SaverContext(saverContext)
    {

    }

    void AddMovePortions(const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
        for (auto&& i : portions) {
            AFL_VERIFY(i);
            AFL_VERIFY(PortionsToMove.emplace(i->GetAddress(), i).second)("portion_id", i->GetPortionId());
            PortionsToAccess->AddPortion(i);
        }
    }

    const THashMap<TPortionAddress, TPortionInfo::TConstPtr>& GetPortionsToRemove() const {
        return PortionsToRemove;
    }

    ui32 GetPortionsToRemoveSize() const {
        return PortionsToRemove.size();
    }

    bool HasPortionsToRemove() const {
        return PortionsToRemove.size();
    }

    void SetTargetCompactionLevel(const ui64 level) {
        TargetCompactionLevel = level;
    }

    void AddPortionToRemove(const TPortionInfo::TConstPtr& info, const bool addIntoDataAccessRequest = true) {
        AFL_VERIFY(!info->HasRemoveSnapshot());
        AFL_VERIFY(PortionsToRemove.emplace(info->GetAddress(), info).second);
        if (addIntoDataAccessRequest) {
            PortionsToAccess->AddPortion(info);
        }
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

}   // namespace NKikimr::NOlap
