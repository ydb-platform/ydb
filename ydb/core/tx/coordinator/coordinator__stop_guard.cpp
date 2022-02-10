#include "coordinator_impl.h"

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxStopGuard : public TTransactionBase<TTxCoordinator> {
    TTxStopGuard(TSelf *coordinator)
        : TBase(coordinator)
    {}

    bool Execute(TTransactionContext &, const TActorContext &) override {
        // nothing
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        Self->OnStopGuardComplete(ctx);
    }
};

ITransaction* TTxCoordinator::CreateTxStopGuard() {
    return new TTxStopGuard(this);
}

}
}
