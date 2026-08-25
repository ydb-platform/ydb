#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

class TDataShard::TTxSchemaChanged : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxSchemaChanged(TDataShard* ds, TEvDataShard::TEvSchemaChangedResult::TPtr ev)
        : TBase(ds)
        , Ev(ev)
        , TxId(0)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEMA_CHANGED; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        TxId = Ev->Get()->Record.GetTxId();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Got TEvSchemaChangedResult from SS at "
                    << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        Self->Pipeline.CompleteSchemaTx(db, TxId);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        NTabletPipe::CloseAndForgetClient(Self->SelfId(), Self->SchemeShardPipe);
        Self->CheckStateChange(ctx);
    }

private:
    TEvDataShard::TEvSchemaChangedResult::TPtr Ev;
    ui64 TxId;
};

ITransaction* TDataShard::CreateTxSchemaChanged(TEvDataShard::TEvSchemaChangedResult::TPtr& ev) {
    return new TTxSchemaChanged(this, ev);
}

}}
