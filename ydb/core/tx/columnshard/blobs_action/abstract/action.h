#pragma once
#include "storage.h"
#include "remove.h"
#include "write.h"
#include "read.h"
#include "storages_manager.h"

namespace NKikimr::NOlap {

class TPortionInfo;

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

    const std::shared_ptr<IBlobsDeclareRemovingAction>& GetRemoving(const NBlobOperations::EConsumer consumerId) {
        if (!Removing) {
            Removing = Storage->StartDeclareRemovingAction(consumerId);
        }
        return Removing;
    }
    const std::shared_ptr<IBlobsWritingAction>& GetWriting(const NBlobOperations::EConsumer consumerId) {
        if (!Writing) {
            Writing = Storage->StartWritingAction(consumerId);
        }
        return Writing;
    }
    const std::shared_ptr<IBlobsWritingAction>& GetWritingOptional() const {
        return Writing;
    }
    const std::shared_ptr<IBlobsReadingAction>& GetReading(const NBlobOperations::EConsumer consumerId) {
        if (!Reading) {
            Reading = Storage->StartReadingAction(consumerId);
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

    void OnExecuteTxAfterAction(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
        if (Removing) {
            Removing->OnExecuteTxAfterRemoving(dbBlobs, blobsWroteSuccessfully);
        }
        if (Writing) {
            Writing->OnExecuteTxAfterWrite(self, dbBlobs, blobsWroteSuccessfully);
        }
    }

    void OnCompleteTxAfterAction(NColumnShard::TColumnShard& self, const bool blobsWroteSuccessfully) {
        if (Removing) {
            Removing->OnCompleteTxAfterRemoving(blobsWroteSuccessfully);
        }
        if (Writing) {
            Writing->OnCompleteTxAfterWrite(self, blobsWroteSuccessfully);
        }
    }
};

class TBlobsAction {
private:
    std::shared_ptr<IStoragesManager> Storages;
    THashMap<TString, TStorageAction> StorageActions;
    const NBlobOperations::EConsumer ConsumerId;

    TStorageAction& GetStorageAction(const TString& storageId) {
        auto it = StorageActions.find(storageId);
        if (it == StorageActions.end()) {
            it = StorageActions.emplace(storageId, Storages->GetOperator(storageId)).first;
        }
        return it->second;
    }
public:
    explicit TBlobsAction(std::shared_ptr<IStoragesManager> storages, const NBlobOperations::EConsumer consumerId)
        : Storages(storages)
        , ConsumerId(consumerId)
    {

    }

    TString GetStorageIds() const {
        TStringBuilder sb;
        for (auto&& i : StorageActions) {
            sb << i.first << ",";
        }
        return sb;
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

    [[nodiscard]] TConclusion<bool> NeedDraftWritingTransaction() const {
        bool hasWriting = false;
        for (auto&& i : GetWritingActions()) {
            if (i->NeedDraftTransaction()) {
                return true;
            }
            hasWriting = true;
        }
        if (hasWriting) {
            return false;
        } else {
            return TConclusionStatus::Fail("has not writings");
        }
    }

    void OnExecuteTxAfterAction(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
        for (auto&& i : StorageActions) {
            i.second.OnExecuteTxAfterAction(self, dbBlobs, blobsWroteSuccessfully);
        }
    }

    void OnCompleteTxAfterAction(NColumnShard::TColumnShard& self, const bool blobsWroteSuccessfully) {
        for (auto&& i : StorageActions) {
            i.second.OnCompleteTxAfterAction(self, blobsWroteSuccessfully);
        }
    }

    std::shared_ptr<IBlobsDeclareRemovingAction> GetRemoving(const TString& storageId) {
        return GetStorageAction(storageId).GetRemoving(ConsumerId);
    }

    std::shared_ptr<IBlobsWritingAction> GetWriting(const TString& storageId) {
        return GetStorageAction(storageId).GetWriting(ConsumerId);
    }

    std::shared_ptr<IBlobsReadingAction> GetReading(const TString& storageId) {
        return GetStorageAction(storageId).GetReading(ConsumerId);
    }

};

}
