#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/protos/counters_node_broker.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NODE_BROKER

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
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateEpoch Execute");

        Self->Dirty.ComputeNextEpochDiff(Diff);
        Self->Dirty.ApplyStateDiff(Diff);
        Self->Dirty.DbApplyStateDiff(Diff, txc);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxUpdateEpoch Complete");

        Self->Committed.ApplyStateDiff(Diff);
        Self->ScheduleEpochUpdate(ctx);
        Self->PrepareEpochCache();
        Self->PrepareUpdateNodesLog();
        Self->ProcessDelayedListNodesRequests();
        Self->ScheduleProcessSubscribersQueue(ctx);

        Self->UpdateCommittedStateCounters();
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
