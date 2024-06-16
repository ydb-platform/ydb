#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NColumnShard {

class TTxRemoveSharedBlobs: public TTransactionBase<TColumnShard> {
private:
    std::shared_ptr<NOlap::IBlobsDeclareRemovingAction> RemoveAction;
    const ui32 TabletTxNo;
    const NActors::TActorId InitiatorActorId;

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxWrite[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
public:
    TTxRemoveSharedBlobs(TColumnShard* self, const std::shared_ptr<NOlap::IBlobsDeclareRemovingAction>& removeAction, const NActors::TActorId initiatorActorId)
        : TBase(self)
        , RemoveAction(removeAction)
        , TabletTxNo(++Self->TabletTxCounter)
        , InitiatorActorId(initiatorActorId)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_DELETE_SHARED_BLOBS; }
};


}
