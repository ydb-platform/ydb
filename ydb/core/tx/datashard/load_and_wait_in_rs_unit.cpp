#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TLoadAndWaitInRSUnit : public TExecutionUnit {
public:
    TLoadAndWaitInRSUnit(TDataShard &dataShard,
                         TPipeline &pipeline);
    ~TLoadAndWaitInRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TLoadAndWaitInRSUnit::TLoadAndWaitInRSUnit(TDataShard &dataShard,
                                           TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::LoadAndWaitInRS, true, dataShard, pipeline)
{
}

TLoadAndWaitInRSUnit::~TLoadAndWaitInRSUnit()
{
}

bool TLoadAndWaitInRSUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    // Check if operation expects input read sets.
    if (op->InReadSets().empty())
        return true;

    // Allow to proceed if operation has to load
    // input read sets from local database.
    if (!op->HasLoadedInRSFlag())
        return true;

    // Check if all read sets are received.
    if (!op->GetRemainReadSets())
        return true;

    return false;
}

EExecutionStatus TLoadAndWaitInRSUnit::Execute(TOperation::TPtr op,
                                               TTransactionContext &txc,
                                               const TActorContext &ctx)
{
    if (op->InReadSets().empty())
        return EExecutionStatus::Executed;

    // Load read sets from local database.
    if (!op->HasLoadedInRSFlag()) {
        if (!Pipeline.LoadInReadSets(op, txc, ctx))
            return EExecutionStatus::Restart;

        Y_ENSURE(op->HasLoadedInRSFlag());

        if (!IsReadyToExecute(op))
            return EExecutionStatus::Continue;
    }

    // We only count transactions that had to wait for incoming readsets
    DataShard.IncCounter(COUNTER_WAIT_READSETS_LATENCY_MS, op->GetCurrentElapsedAndReset().MilliSeconds());
    return EExecutionStatus::Executed;
}

void TLoadAndWaitInRSUnit::Complete(TOperation::TPtr,
                                    const TActorContext &)
{
}

THolder<TExecutionUnit> CreateLoadAndWaitInRSUnit(TDataShard &dataShard,
                                                  TPipeline &pipeline)
{
    return THolder(new TLoadAndWaitInRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
