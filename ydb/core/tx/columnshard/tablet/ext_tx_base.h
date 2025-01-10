#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NColumnShard {

class TColumnShard;

//Base class for LocalDB transactions with ColumnShard specific
class TExtendedTransactionBase: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    const TString TxInfo;
    const ui32 TabletTxNo;
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& ctx) = 0;
    virtual void DoComplete(const NActors::TActorContext & ctx) = 0;

public:
    virtual bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& ctx) override final;
    virtual void Complete(const NActors::TActorContext& ctx) override final;

    TExtendedTransactionBase(TColumnShard* self, const TString& txInfo = Default<TString>());
};

} //namespace NKikimr::NColumnShard
