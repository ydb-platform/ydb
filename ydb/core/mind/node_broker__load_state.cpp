#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr {
namespace NNodeBroker {

class TNodeBroker::TTxLoadState : public TTransactionBase<TNodeBroker> {
public:
    TTxLoadState(TNodeBroker *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_LOAD_STATE; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxLoadState Execute");

        if (!Self->Dirty.DbLoadState(txc, ctx))
            return false;

        // Move epoch if required.
        auto now = ctx.Now();
        while (now > Self->Dirty.Epoch.End) {
            TStateDiff diff;
            Self->Dirty.ComputeNextEpochDiff(diff);
            Self->Dirty.DbApplyStateDiff(diff, txc);
            Self->Dirty.ApplyStateDiff(diff);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxLoadState Complete");

        Self->Committed = Self->Dirty;
        Self->Become(&TNodeBroker::StateWork);
        Self->SubscribeForConfigUpdates(ctx);
        Self->ScheduleEpochUpdate(ctx);
        Self->PrepareEpochCache();
        Self->SignalTabletActive(ctx);
    }

private:
};

ITransaction *TNodeBroker::CreateTxLoadState()
{
    return new TTxLoadState(this);
}

} // NNodeBroker
} // NKikimr
