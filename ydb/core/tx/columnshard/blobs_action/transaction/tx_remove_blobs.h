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
    TTxRemoveSharedBlobs(TColumnShard* self, const std::shared_ptr<NOlap::IBlobsDeclareRemovingAction>& removeAction,
        const NOlap::TTabletsByBlob& sharingBlobIds, const NActors::TActorId initiatorActorId,
        const std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> manager)
        : TBase(self)
        , RemoveAction(removeAction)
        , TabletTxNo(++Self->TabletTxCounter)
        , InitiatorActorId(initiatorActorId)
        , SharingBlobIds(sharingBlobIds)
        , Manager(manager)
    {
        const auto categories = Manager->BuildRemoveCategories(SharingBlobIds);
        AFL_VERIFY(categories.HasSharingOnly() || categories.IsEmpty());
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_DELETE_SHARED_BLOBS; }
};


}
