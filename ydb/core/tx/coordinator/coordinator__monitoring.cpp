#include "coordinator_impl.h"
#include "coordinator__check.h"

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxMonitoring : public TTxCoordinator::TTxConsistencyCheck {
    TActorId ActorToRespond;
    TString CheckResult;

    TTxMonitoring(TSelf* self, const TActorId& actorToRespond)
        : TTxCoordinator::TTxConsistencyCheck(self)
        , ActorToRespond(actorToRespond)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        try {
            bool ok = TTxCoordinator::TTxConsistencyCheck::Execute(txc, ctx);
            if (!ok)
                return false;
            CheckResult = "ConsistencyCheck OK";
        }
        catch(const yexception& e) {
            CheckResult = "ConsistencyCheck FAILED<br>";
            CheckResult += e.what();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TTxCoordinator::TTxConsistencyCheck::Complete(ctx);
        ctx.Send(ActorToRespond, new NMon::TEvRemoteHttpInfoRes(CheckResult));
    }
};

ITransaction* TTxCoordinator::CreateTxMonitoring(NMon::TEvRemoteHttpInfo::TPtr& ev) {
    return new TTxMonitoring(this, ev->Sender);
}

}
}
