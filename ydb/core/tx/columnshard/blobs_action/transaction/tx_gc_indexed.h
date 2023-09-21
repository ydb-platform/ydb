#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NColumnShard {
class TTxGarbageCollectionFinished: public TTransactionBase<TColumnShard> {
private:
    std::shared_ptr<NOlap::IBlobsGCAction> Action;
public:
    TTxGarbageCollectionFinished(TColumnShard* self, const std::shared_ptr<NOlap::IBlobsGCAction>& action)
        : TBase(self)
        , Action(action) {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_GC_FINISHED; }
};


}
