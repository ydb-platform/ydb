#include "tenant_slot_broker_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TENANT_SLOT_BROKER

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxInitScheme : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxInitScheme(TTenantSlotBroker *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TTxInitScheme Execute");

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TTxInitScheme Complete");

        Self->ProcessTx(Self->CreateTxLoadState(), ctx);
        Self->TxCompleted(this, ctx);
    }
};

ITransaction *TTenantSlotBroker::CreateTxInitScheme()
{
    return new TTxInitScheme(this);
}

} // NTenantSlotBroker
} // NKikimr
