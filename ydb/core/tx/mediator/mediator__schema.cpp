#include "mediator_impl.h"

namespace NKikimr {
namespace NTxMediator {

using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct TTxMediator::TTxSchema : public TTransactionBase<TTxMediator> {
    TTxSchema(TSelf *mediator)
        : TBase(mediator)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << Self->TabletID()
            << " TTxSchema Complete");
        Self->Execute(Self->CreateTxUpgrade(), ctx);
    }
};

ITransaction* TTxMediator::CreateTxSchema() {
    return new TTxSchema(this);
}

}
}
