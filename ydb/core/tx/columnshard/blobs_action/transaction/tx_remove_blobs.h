#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>

namespace NKikimr::NColumnShard {

class TTxRemoveSharedBlobs: public TTransactionBase<TColumnShard> {
private:
    std::shared_ptr<NOlap::IBlobsDeclareRemovingAction> RemoveAction;
    const ui32 TabletTxNo;
    const NActors::TActorId InitiatorActorId;
    NOlap::TTabletsByBlob SharingBlobIds;
    const std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> Manager;
    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxWrite[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
public:
    TTxRemoveSharedBlobs(TColumnShard* self,
        const NOlap::TTabletsByBlob& sharingBlobIds, const NActors::TActorId initiatorActorId,
        const TString& storageId)
        : TBase(self)
        , TabletTxNo(++Self->TabletTxCounter)
        , InitiatorActorId(initiatorActorId)
        , SharingBlobIds(sharingBlobIds)
        , Manager(Self->GetStoragesManager()->GetSharedBlobsManager()->GetStorageManagerVerified(storageId))
    {
        Self->GetStoragesManager()->GetSharedBlobsManager()->StartExternalModification();
        RemoveAction = Self->GetStoragesManager()->GetOperatorVerified(storageId)->StartDeclareRemovingAction(NOlap::NBlobOperations::EConsumer::CLEANUP_SHARED_BLOBS);
        auto categories = Manager->BuildRemoveCategories(SharingBlobIds);
        for (auto it = categories.GetDirect().GetIterator(); it.IsValid(); ++it) {
            RemoveAction->DeclareRemove(it.GetTabletId(), it.GetBlobId());
        }
        for (auto it = categories.GetBorrowed().GetIterator(); it.IsValid(); ++it) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_BLOBS)("problem", "borrowed_to_remove")("blob_id", it.GetBlobId())("tablet_id", it.GetTabletId());
        }
        AFL_VERIFY(categories.GetBorrowed().IsEmpty());
        AFL_VERIFY(categories.GetSharing().GetSize() == SharingBlobIds.GetSize())("sharing_category", categories.GetSharing().GetSize())(
                                                            "sharing", SharingBlobIds.GetSize());
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_DELETE_SHARED_BLOBS; }
};


}
