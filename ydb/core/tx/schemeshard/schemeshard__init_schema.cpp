#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxInitSchema : public TTransactionBase<TSchemeShard> {
    TTxInitSchema(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TxInitSchema.Execute");
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TxInitSchema.Complete");
        Self->Execute(Self->CreateTxUpgradeSchema(), ctx);
    }
};

ITransaction* TSchemeShard::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

} // NSchemeShard
} // NKikimr
