#pragma once

#include "blob_manager.h"

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/remove_gc.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TGCTask: public IBlobsGCAction {
private:
    using TBase = IBlobsGCAction;
public:
    struct TGCLists {
        THashSet<TLogoBlobID> KeepList;
        THashSet<TLogoBlobID> DontKeepList;
        mutable ui32 RequestsCount = 0;
    };
    using TGCListsByGroup = THashMap<ui32, TGCLists>;
private:
    TGCListsByGroup ListsByGroupId;
    std::optional<TGenStep> CollectGenStepInFlight;
    const ui64 TabletId;
    const ui64 CurrentGen;
    std::deque<TUnifiedBlobId> KeepsToErase;
    std::shared_ptr<TBlobManager> Manager;
protected:
    virtual void RemoveBlobIdFromDB(const TTabletId tabletId, const TUnifiedBlobId& blobId, TBlobManagerDb& dbBlobs) override;
    virtual void DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) override;
    virtual bool DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) override;
    virtual bool DoIsEmpty() const override {
        return !CollectGenStepInFlight && KeepsToErase.empty();
    }

public:
    TGCTask(const TString& storageId, TGCListsByGroup&& listsByGroupId, const std::optional<TGenStep>& collectGenStepInFlight, std::deque<TUnifiedBlobId>&& keepsToErase,
        const std::shared_ptr<TBlobManager>& manager, TBlobsCategories&& blobsToRemove, const std::shared_ptr<TRemoveGCCounters>& counters, const ui64 tabletId, const ui64 currentGen);

    const TGCListsByGroup& GetListsByGroupId() const {
        return ListsByGroupId;
    }

    bool IsFinished() const {
        return ListsByGroupId.empty();
    }

    void OnGCResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);

    std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> BuildRequest(const ui64 groupId) const;
};

}
