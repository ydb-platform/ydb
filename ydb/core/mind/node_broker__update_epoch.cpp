#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr {
namespace NNodeBroker {

class TNodeBroker::TTxUpdateEpoch : public TTransactionBase<TNodeBroker> {
public:
    TTxUpdateEpoch(TNodeBroker *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_UPDATE_EPOCH; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER, "TTxUpdateEpoch Execute");

        Self->Dirty.ComputeNextEpochDiff(Diff);
        Self->Dirty.DbApplyStateDiff(Diff, txc);
        Self->Dirty.ApplyStateDiff(Diff);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxUpdateEpoch Complete");

        Self->Committed.ApplyStateDiff(Diff);
        Self->ScheduleEpochUpdate(ctx);
        Self->PrepareEpochCache();
        Self->ProcessDelayedListNodesRequests();
    }

private:
    TStateDiff Diff;
};

ITransaction *TNodeBroker::CreateTxUpdateEpoch()
{
    return new TTxUpdateEpoch(this);
}

} // NNodeBroker
} // NKikimr
