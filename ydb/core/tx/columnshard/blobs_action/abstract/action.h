#pragma once
#include "storage.h"
#include "remove.h"
#include "write.h"
#include "read.h"
#include "storages_manager.h"

namespace NKikimr::NOlap {

struct TPortionInfo;

class TStorageAction {
private:
    std::shared_ptr<IBlobsStorageOperator> Storage;
    std::shared_ptr<IBlobsDeclareRemovingAction> Removing;
    std::shared_ptr<IBlobsWritingAction> Writing;
    std::shared_ptr<IBlobsReadingAction> Reading;

public:
    TStorageAction(const std::shared_ptr<IBlobsStorageOperator>& storage)
        : Storage(storage) {

    }

    const std::shared_ptr<IBlobsDeclareRemovingAction>& GetRemoving() {
        if (!Removing) {
            Removing = Storage->StartDeclareRemovingAction();
        }
        return Removing;
    }
    const std::shared_ptr<IBlobsWritingAction>& GetWriting() {
        if (!Writing) {
            Writing = Storage->StartWritingAction();
        }
        return Writing;
    }
    const std::shared_ptr<IBlobsWritingAction>& GetWritingOptional() const {
        return Writing;
    }
    const std::shared_ptr<IBlobsReadingAction>& GetReading() {
        if (!Reading) {
            Reading = Storage->StartReadingAction();
        }
        return Reading;
    }

    std::shared_ptr<IBlobsReadingAction> GetReadingOptional() const {
        return Reading;
    }

    bool HasReading() const {
        return !!Reading;
    }
    bool HasWriting() const {
        return !!Writing;
    }

    void OnExecuteTxAfterAction(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs, const bool success) {
        if (Removing) {
            Removing->OnExecuteTxAfterRemoving(self, dbBlobs, success);
        }
        if (Writing) {
            Writing->OnExecuteTxAfterWrite(self, dbBlobs, success);
        }
    }

    void OnCompleteTxAfterAction(NColumnShard::TColumnShard& self) {
        if (Removing) {
            Removing->OnCompleteTxAfterRemoving(self);
        }
        if (Writing) {
            Writing->OnCompleteTxAfterWrite(self);
        }
    }
};

class TBlobsAction {
private:
    std::shared_ptr<IStoragesManager> Storages;
    THashMap<TString, TStorageAction> StorageActions;

    TStorageAction& GetStorageAction(const TString& storageId) {
        auto it = StorageActions.find(storageId);
        if (it == StorageActions.end()) {
            it = StorageActions.emplace(storageId, Storages->GetOperator(storageId)).first;
        }
        return it->second;
    }
public:
    TBlobsAction(std::shared_ptr<IStoragesManager> storages)
        : Storages(storages)
    {

    }

    ui32 GetWritingBlobsCount() const {
        ui32 result = 0;
        for (auto&& [_, action] : StorageActions) {
            if (!!action.GetWritingOptional()) {
                result += action.GetWritingOptional()->GetBlobsCount();
            }
        }
        return result;
    }

    ui64 GetWritingTotalSize() const {
        ui64 result = 0;
        for (auto&& [_, action] : StorageActions) {
            if (!!action.GetWritingOptional()) {
                result += action.GetWritingOptional()->GetTotalSize();
            }
        }
        return result;
    }

    std::vector<std::shared_ptr<IBlobsReadingAction>> GetReadingActions() const {
        std::vector<std::shared_ptr<IBlobsReadingAction>> result;
        for (auto&& i : StorageActions) {
            if (i.second.HasReading()) {
                result.emplace_back(i.second.GetReadingOptional());
            }
        }
        return result;
    }

    std::vector<std::shared_ptr<IBlobsWritingAction>> GetWritingActions() const {
        std::vector<std::shared_ptr<IBlobsWritingAction>> result;
        for (auto&& i : StorageActions) {
            if (i.second.HasWriting()) {
                result.emplace_back(i.second.GetWritingOptional());
            }
        }
        return result;
    }

    bool NeedDraftWritingTransaction() const {
        for (auto&& i : GetWritingActions()) {
            if (i->NeedDraftTransaction()) {
                return true;
            }
        }
        return false;
    }

    void OnExecuteTxAfterAction(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs, const bool success) {
        for (auto&& i : StorageActions) {
            i.second.OnExecuteTxAfterAction(self, dbBlobs, success);
        }
    }

    void OnCompleteTxAfterAction(NColumnShard::TColumnShard& self) {
        for (auto&& i : StorageActions) {
            i.second.OnCompleteTxAfterAction(self);
        }
    }

    std::shared_ptr<IBlobsDeclareRemovingAction> GetRemoving(const TString& storageId) {
        return GetStorageAction(storageId).GetRemoving();
    }

    std::shared_ptr<IBlobsDeclareRemovingAction> GetRemoving(const TPortionInfo& portionInfo);

    std::shared_ptr<IBlobsWritingAction> GetWriting(const TString& storageId) {
        return GetStorageAction(storageId).GetWriting();
    }

    std::shared_ptr<IBlobsWritingAction> GetWriting(const TPortionInfo& portionInfo);

    std::shared_ptr<IBlobsReadingAction> GetReading(const TString& storageId) {
        return GetStorageAction(storageId).GetReading();
    }

    std::shared_ptr<IBlobsReadingAction> GetReading(const TPortionInfo& portionInfo);

};

}
