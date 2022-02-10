#include "coordinator_impl.h"

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxSchema : public TTransactionBase<TTxCoordinator> {
    TTxSchema(TSelf *coordinator)
        : TBase(coordinator)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        Self->Execute(Self->CreateTxUpgrade(), ctx);
    }
};

ITransaction* TTxCoordinator::CreateTxSchema() {
    return new TTxSchema(this);
}

}
}
