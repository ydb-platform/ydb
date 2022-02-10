#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard { 

using namespace NTabletFlatExecutor;

class TDataShard::TTxDisconnected : public NTabletFlatExecutor::TTransactionBase<TDataShard> { 
public:
    TTxDisconnected(TDataShard *self, TEvInterconnect::TEvNodeDisconnected::TPtr ev) 
        : TBase(self)
        , Event(std::move(ev))
    {}

    TTxType GetTxType() const override
    {
        return TXTYPE_DISCONNECTED;
    }

    bool Execute(TTransactionContext &/*txc*/, const TActorContext &ctx)
    {
        ui32 nodeId = Event->Get()->NodeId;
        Self->Pipeline.ProcessDisconnected(nodeId);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxDisconnected, shard: " << Self->TabletID() << ", node: " << nodeId);

        Self->PlanQueue.Progress(ctx);
        return true;
    }

    void Complete(const TActorContext &/*ctx*/) override {
    }

private:
    TEvInterconnect::TEvNodeDisconnected::TPtr Event;
};

void TDataShard::Handle(TEvents::TEvUndelivered::TPtr &ev, 
                               const TActorContext &ctx)
{
    ui64 txId = ev->Cookie;
    auto op = Pipeline.FindOp(ev->Cookie);
    if (op) {
        op->AddInputEvent(ev.Release());
        Pipeline.AddCandidateOp(op);
        PlanQueue.Progress(ctx);
    }
}

void TDataShard::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, 
                               const TActorContext &ctx)
{
    ui32 nodeId = ev->Get()->NodeId;

    LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
                 "Shard " << TabletID() << " disconnected from node " << nodeId);

    Pipeline.ProcessDisconnected(nodeId);
    PlanQueue.Progress(ctx);
}


} // namesapce NDataShard 
} // namespace NKikimr
