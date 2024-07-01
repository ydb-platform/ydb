#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NColumnShard {
class TTxInsertTableCleanup: public TTransactionBase<TColumnShard> {
private:
    std::shared_ptr<NOlap::IBlobsDeclareRemovingAction> BlobsAction;
public:
    TTxInsertTableCleanup(TColumnShard* self)
        : TBase(self) {
        Y_ABORT_UNLESS(self->InsertTable->GetAborted().size());
    }

    ~TTxInsertTableCleanup() {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_CLEANUP_INSERT_TABLE; }
};

}
