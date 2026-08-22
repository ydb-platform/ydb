#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "datashard_failpoints.h"

namespace NKikimr {
namespace NDataShard {

class TBlockFailPointUnit : public TExecutionUnit {
public:
    TBlockFailPointUnit(TDataShard &dataShard,
                         TPipeline &pipeline);
    ~TBlockFailPointUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TBlockFailPointUnit::TBlockFailPointUnit(TDataShard &dataShard,
                                           TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::BlockFailPoint, false, dataShard, pipeline)
{
}

TBlockFailPointUnit::~TBlockFailPointUnit()
{
}

bool TBlockFailPointUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    if (!op->HasFlag(TTxFlags::BlockFailPointWaiting)) {
        return true;
    }
    if (op->HasFlag(TTxFlags::BlockFailPointUnblocked)) {
        return true;
    }
    return false;
}

EExecutionStatus TBlockFailPointUnit::Execute(TOperation::TPtr op,
                                               TTransactionContext &,
                                               const TActorContext &)
{
    if (!op->HasFlag(TTxFlags::BlockFailPointWaiting)) {
        auto future = gBlockOperationsFailPoint.Block(DataShard.TabletID(), op->GetTxId());
        if (future.Initialized() && !future.IsReady()) {
            op->SetFlag(TTxFlags::BlockFailPointWaiting);
            TActorId selfId = DataShard.SelfId();
            future.Subscribe([actorSystem = TActivationContext::ActorSystem(), selfId = selfId, txId = op->GetTxId()](auto&) {
                actorSystem->Send(new IEventHandle(selfId, selfId, new TDataShard::TEvPrivate::TEvBlockFailPointUnblock(txId)));
            });
            return EExecutionStatus::Continue;
        }
    }

    op->ResetFlag(TTxFlags::BlockFailPointWaiting);
    op->ResetFlag(TTxFlags::BlockFailPointUnblocked);
    return EExecutionStatus::Executed;
}

void TBlockFailPointUnit::Complete(TOperation::TPtr,
                                    const TActorContext &)
{
}

THolder<TExecutionUnit> CreateBlockFailPointUnit(TDataShard &dataShard,
                                                  TPipeline &pipeline)
{
    return THolder(new TBlockFailPointUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
