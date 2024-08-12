#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

class TTxFinishAsyncTransaction: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;
    const ui64 TxId;
public:
    TTxFinishAsyncTransaction(TColumnShard& owner, const ui64 txId)
        : TBase(&owner)
        , TxId(txId)
    {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void Complete(const TActorContext& ctx) override;
};

}
