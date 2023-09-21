#pragma once

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/blob_manager.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TGCTask: public IBlobsGCAction {
private:
    using TBase = IBlobsGCAction;
public:
    struct TGCLists {
        THashSet<TLogoBlobID> KeepList;
        THashSet<TLogoBlobID> DontKeepList;
    };
    using TGCListsByGroup = THashMap<ui32, TGCLists>;
private:
    TGCListsByGroup ListsByGroupId;
    NColumnShard::TGenStep CollectGenStepInFlight;
    // Maps PerGenerationCounter value to the group in PerGroupGCListsInFlight
    THashMap<ui64, ui32> CounterToGroupInFlight;
    std::deque<TUnifiedBlobId> KeepsToErase;
    std::deque<TUnifiedBlobId> DeletesToErase;
    std::shared_ptr<NColumnShard::TBlobManager> Manager;
protected:
    virtual void DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) override;
    virtual void DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) override;
public:
    bool IsEmpty() const {
        return ListsByGroupId.empty();
    }

    TGCTask(const TString& storageId, TGCListsByGroup&& listsByGroupId, const NColumnShard::TGenStep& collectGenStepInFlight, std::deque<TUnifiedBlobId>&& keepsToErase, std::deque<TUnifiedBlobId>&& deletesToErase,
        const std::shared_ptr<NColumnShard::TBlobManager>& manager);

    bool IsFinished() const {
        return ListsByGroupId.empty();
    }

    void OnGCResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);

    THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> BuildRequests(ui64& perGenerationCounter, const ui64 tabletId, const ui64 currentGen);
};

}
