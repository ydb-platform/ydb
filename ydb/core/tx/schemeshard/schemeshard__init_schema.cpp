#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxInitSchema : public TTransactionBase<TSchemeShard> {
    TTxInitSchema(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "TxInitSchema.Execute");
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "TxInitSchema.Complete");
        Self->Execute(Self->CreateTxUpgradeSchema(), ctx);
    }
};

ITransaction* TSchemeShard::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

} // NSchemeShard
} // NKikimr
