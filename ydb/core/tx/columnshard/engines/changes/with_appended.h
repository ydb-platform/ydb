#pragma once
#include "abstract/abstract.h"
#include "abstract/move_portions.h"
#include "abstract/remove_portions.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TChangesWithAppend: public TColumnEngineChanges {
private:
    using TBase = TColumnEngineChanges;
    TRemovePortionsChange PortionsToRemove;
    TMovePortionsChange PortionsToMove;

protected:
    std::vector<TWritePortionInfoWithBlobsResult> AppendedPortions;
    TSaverContext SaverContext;
    bool NoAppendIsCorrect = false;

    virtual void OnDataAccessorsInitialized(const TDataAccessorsInitializationContext& /*context*/) override {
    }

    virtual void DoCompile(TFinalizationContext& context) override;
    virtual void DoOnAfterCompile() override;
    virtual void DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) override;
    virtual void DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) override;
    virtual void DoStart(NColumnShard::TColumnShard& self) override;

    virtual void DoDebugString(TStringOutput& out) const override {
        out << "remove=" << PortionsToRemove.GetSize() << ";append=" << AppendedPortions.size() << ";move=" << PortionsToMove.GetSize();
    }

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLockImpl() const = 0;

    virtual std::shared_ptr<NDataLocks::ILock> DoBuildDataLock() const override final {
        auto actLock = DoBuildDataLockImpl();
        auto removePortionsLock = PortionsToRemove.BuildDataLock(TypeString() + "::" + GetTaskIdentifier() + "::REMOVE", GetLockCategory());
        auto movePortionsLock = PortionsToMove.BuildDataLock(TypeString() + "::" + GetTaskIdentifier() + "::MOVE", GetLockCategory());
        return NDataLocks::TCompositeLock::Build(TypeString() + "::" + GetTaskIdentifier(), {actLock, removePortionsLock, movePortionsLock});
    }

public:
    TChangesWithAppend(const TSaverContext& saverContext, const NBlobOperations::EConsumer consumerId)
        : TBase(saverContext.GetStoragesManager(), consumerId)
        , SaverContext(saverContext) {
    }

    const TRemovePortionsChange& GetPortionsToRemove() const {
        return PortionsToRemove;
    }

    const TMovePortionsChange& GetPortionsToMove() const {
        return PortionsToMove;
    }

    void SetTargetCompactionLevel(const ui32 level) {
        PortionsToMove.SetTargetCompactionLevel(level);
    }

    const std::vector<TWritePortionInfoWithBlobsResult>& GetAppendedPortions() const {
        return AppendedPortions;
    }

    std::vector<TWritePortionInfoWithBlobsResult>& MutableAppendedPortions() {
        return AppendedPortions;
    }

    void AddMovePortions(const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
        PortionsToMove.AddPortions(portions);
        for (auto&& i : portions) {
            PortionsToAccess.emplace_back(i);
        }
    }

    void AddPortionToRemove(const TPortionInfo::TConstPtr& info, const bool addIntoDataAccessRequest = true) {
        AFL_VERIFY(PortionsToRemove.AddPortion(info));
        if (addIntoDataAccessRequest) {
            PortionsToAccess.emplace_back(info);
        }
    }

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
