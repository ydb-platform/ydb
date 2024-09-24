#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NOlap::NDataSharing {

template <class TShard>
class TExtendedTransactionBase: public NTabletFlatExecutor::TTransactionBase<TShard> {
private:
    const TString TxInfo;
    const ui32 TabletTxNo;
    using TBase = NTabletFlatExecutor::TTransactionBase<TShard>;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& ctx) = 0;
    virtual void DoComplete(const NActors::TActorContext & ctx) = 0;

public:
    virtual bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& ctx) override final {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build()("tablet_id", TBase::Self->TabletID())("local_tx_no", TabletTxNo)("tx_info", TxInfo);
        return DoExecute(txc, ctx);
    }
    virtual void Complete(const NActors::TActorContext& ctx) override final {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build()("tablet_id", TBase::Self->TabletID())("local_tx_no", TabletTxNo)("tx_info", TxInfo);
        return DoComplete(ctx);
    }

    TExtendedTransactionBase(TShard* self, const TString& txInfo = Default<TString>())
        : TBase(self)
        , TxInfo(txInfo)
        , TabletTxNo(++TBase::Self->TabletTxCounter)
    {

    }
};

}
